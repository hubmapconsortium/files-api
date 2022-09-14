import logging
import threading
import json
import requests

from flask import Flask, Response, request, current_app
from contextlib import closing

# Local modules
from S3_worker import S3Worker
from app_db import DBConn

from hubmap_commons.hm_auth import AuthHelper

# Set up scalars with SQL strings matching the paramstyle of the database module, as
# specified at https://peps.python.org/pep-0249
#
# Using the "format" paramstyle with mysql-connector-python module for MySQL 8.0+
#
# Ignore threat of unsanitized user input for SQL injection, XSS, etc. due to current
# nature of site at AWS, UUID format checking in this microservice, etc.
SQL_SELECT_FILES_DESCENDED_FROM_ANCESTOR_UUID = \
    ("SELECT UUID AS file_uuid"
     "       ,PATH AS path"
     "       ,CHECKSUM AS checksum"
     "       ,SIZE AS size"
     "       ,BASE_DIR AS base_dir"
     " FROM files"
     "  INNER JOIN ancestors ON ancestors.DESCENDANT_UUID = files.UUID"
     " WHERE ancestors.ANCESTOR_UUID = %s"
     )

class FileWorker:

    def __init__(self, globusGroups=None, app_config=None):
        self.logger = logging.getLogger()
        self.auth_helper = AuthHelper.configured_instance(app_config['APP_CLIENT_ID'], app_config['APP_CLIENT_SECRET'] )

        if app_config is None:
            raise Exception("Configuration data loaded by the app must be passed to the worker.")
        try:
            clientId = app_config['APP_CLIENT_ID']
            clientSecret = app_config['APP_CLIENT_SECRET']
            dbHost = app_config['DB_HOST']
            dbName = app_config['DB_NAME']
            dbUsername = app_config['DB_USERNAME']
            dbPassword = app_config['DB_PASSWORD']

            self.aws_access_key_id = app_config['AWS_ACCESS_KEY_ID']
            self.aws_secret_access_key = app_config['AWS_SECRET_ACCESS_KEY']
            self.aws_s3_bucket_name = app_config['AWS_S3_BUCKET_NAME']
            self.aws_object_url_expiration_in_secs = app_config['AWS_OBJECT_URL_EXPIRATION_IN_SECS']

            if 'LARGE_RESPONSE_THRESHOLD' not in app_config or int(app_config['LARGE_RESPONSE_THRESHOLD'] > 9999999):
                self.logger.error("LARGE_RESPONSE_THRESHOLD missing from app.cfg or too big for AWS Gateway. Defaulting to smaller value.")
                self.large_response_threshold = 5000000
            else:
                self.large_response_threshold = int(app_config['LARGE_RESPONSE_THRESHOLD'])
                self.logger.info(f"large_response_threshold set to {self.large_response_threshold}.")

            if not clientId:
                raise Exception("Configuration parameter APP_CLIENT_ID not valid.")
            if not clientSecret:
                raise Exception("Configuration parameter APP_CLIENT_SECRET not valid.")
        except KeyError as ke:
            self.logger.error("Expected configuration failed to load %s from app_config=%s.", ke, app_config)
            raise Exception("Expected configuration failed to load. See the logs.")

        if not clientId or not clientSecret:
            raise Exception("Globus client id and secret are required in AuthHelper")

        self.dbHost = dbHost
        self.dbName = dbName
        self.dbUsername = dbUsername
        self.dbPassword = dbPassword
        self.lock = threading.RLock()
        self.hmdb = DBConn(self.dbHost, self.dbUsername, self.dbPassword, self.dbName)

    # Test the connection to the supporting database the self.hmdb variable is
    # connected to during construction.
    # input: none
    # output: Boolean, valued True if an SQL SELECT statement executes, False otherwise.
    def testConnection(self):
        try:
            res = None
            with closing(self.hmdb.getDBConnection()) as dbConn:
                with closing(dbConn.cursor(prepared=True)) as curs:
                    curs.execute("select 'ANYTHING'")
                    res = curs.fetchone()

            if not res:
                return False
            else:
                return res[0] == 'ANYTHING'
        except Exception as e:
            self.logger.error(e, exc_info=True)
            return False

    # get the information for all files attached to a specific entity
    # input: id (hubmap_id or uuid) of the parent entity
    # output: an array of dicts with each dict containing the attributes of a file
    #         file attributes:
    #             path: the local file system path, including name of the file
    #         checksum: the checksum of the file
    #             size: the size of the file
    #        file_uuid: the uuid of the file
    #         base_dir: the base directory type, one of
    #                   INGEST_PORTAL_UPLOAD - the file was uploaded into the space for file uploads from the Ingest UI
    #                   DATA_UPLOAD - the file was upload into the upload space for datasets usually via Globus
    #
    def get_file_info(self, entity_uuid):
        uuid_tuple = (entity_uuid,)  # N.B. comma to force creation of tuple with one value, rather than scalar

        # run the query and morph results to an array of dict
        with closing(self.hmdb.getDBConnection()) as dbConn:
            with closing(dbConn.cursor(prepared=True)) as curs:
                # query that finds all files associated with entity by joining the ancestors table (entity is
                # the ancestor, files are the descendants) with the files table
                curs.execute(SQL_SELECT_FILES_DESCENDED_FROM_ANCESTOR_UUID
                             , uuid_tuple)
                results = [dict((curs.description[i][0].lower(), value) for i, value in enumerate(row)) for row in
                           curs.fetchall()]

        results_json = json.dumps(results)
        if len(results_json) < self.large_response_threshold:
            return Response(response=results_json, mimetype="application/json")
        else:
            anS3Worker = None
            try:
                anS3Worker = S3Worker(self.aws_access_key_id, self.aws_secret_access_key, self.aws_s3_bucket_name, self.aws_object_url_expiration_in_secs)
                self.logger.info("anS3Worker initialized")
            except Exception as e:
                self.logger.error(f"Error getting anS3Worker to handle len(results)={len(results)}.")
                self.logger.error(e, exc_info=True)
                raise Exception("Large result storage setup error.  See log.")

            try:
                # return anS3Worker.do_whatever_with_S3()
                obj_key = anS3Worker.stash_text_as_object(results_json, uuid_tuple[0])
                aws_presigned_url = anS3Worker.create_URL_for_object(obj_key)
                return Response(aws_presigned_url, 303)
            except Exception as e:
                self.logger.error(f"Error getting presigned URL for obj_key={obj_key}.")
                self.logger.error(e, exc_info=True)
                raise Exception("Large result storage creation error.  See log.")

    # Use the uuid-api service to find out the uuid of a given identifier, for
    # use with endpoints requiring a uuid as the identifier.
    # input: id (hubmap_id or uuid) of an entity
    # output: the uuid of the entity
    def get_identifier_info(self, entity_id):
        # Get the single Globus groups token for authorization
        auth_helper_instance = AuthHelper.instance()
        auth_token = auth_helper_instance.getAuthorizationTokens(request.headers)
        # Try to keep all this isinstance() checking lower in the code, closer to things which are not
        # ready for change, like commons.AuthHelper. Flip to Python's exception mechanism, and let
        # upper-level endpoints form a response.
        if isinstance(auth_token, Response):
            raise requests.exceptions.HTTPError(response=auth_token)
        elif isinstance(auth_token, str):
            token = auth_token
        else:
            raise requests.exceptions.HTTPError(response=Response("Valid Globus groups token required",401))

        get_url = current_app.config['UUID_API_URL'] + '/uuid/' + entity_id
        response = requests.get(get_url, headers = {'Authorization': 'Bearer ' + token}, verify = False)
        if response.status_code != 200:
            raise requests.exceptions.HTTPError(response=response)
        return response.json()['uuid']
