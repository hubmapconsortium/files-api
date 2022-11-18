import csv
import logging
import re
import threading
import json
from http.client import HTTPException

import requests
from types import MappingProxyType

import yaml
from flask import Flask, Response, request, current_app
from contextlib import closing

# Local modules
from S3_worker import S3Worker
from app_db import DBConn

from hubmap_commons.hm_auth import AuthHelper
from hubmap_commons.hubmap_const import HubmapConst

# UMLS Concept Unique Identifiers used to encode entity-api information
UMLS_AGE_GROUP_CUI = 'C0001779'
UMLS_RACE_GROUP_CUI = 'C0034510'

# Yaml file to be parsed for organ description lookup
ORGAN_TYPES_YAML = 'https://raw.githubusercontent.com/hubmapconsortium/search-api/master/src/search-schema/data/definitions/enums/organ_types.yaml'
# Yaml file to be parsed for tissue description lookup
TISSUE_TYPES_YAML = 'https://raw.githubusercontent.com/hubmapconsortium/search-api/main/src/search-schema/data/definitions/enums/tissue_sample_types.yaml'
ASSAY_TYPES_YAML = 'https://raw.githubusercontent.com/hubmapconsortium/search-api/main/src/search-schema/data/definitions/enums/assay_types.yaml'

# Keep a file of descriptions associated with a Dataset data_type and a file regex pattern until
# the ontology service is available.  Load the file into a dictionary keyed by data_type, and
# containing a dictionary keyed by file regex pattern with the description as a value.
DATASET_DESCRIPTION_CSV_FILE = 'DatasetDescriptionLookup.tsv'

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

    def __init__(self, globusGroups=None, appConfig=None, requestHeaders=None):
        self.logger = logging.getLogger()
        self.auth_helper = AuthHelper.configured_instance(appConfig['APP_CLIENT_ID'], appConfig['APP_CLIENT_SECRET'])

        if requestHeaders:
            # Get user token from Authorization header
            # getAuthorizationTokens() also handles MAuthorization header, but we are not using that here
            try:
                self.user_token = self.auth_helper.getAuthorizationTokens(requestHeaders)
            except Exception as e:
                msg = "Failed to parse the Authorization token by calling commons.auth_helper.getAuthorizationTokens(). See logs."
                # Log the full stack trace, prepend a line with our message
                self.logger.exception(msg)
                self.logger.exception(e)
        else:
            self.user_token = None

        self.user_groups_by_id_dict = self.auth_helper.get_globus_groups_info()['by_id']

        if appConfig is None:
            raise Exception("Configuration data loaded by the app must be passed to the worker.")
        try:
            clientId = appConfig['APP_CLIENT_ID']
            clientSecret = appConfig['APP_CLIENT_SECRET']
            self.dbHost = appConfig['DB_HOST']
            self.dbName = appConfig['DB_NAME']
            self.dbUsername = appConfig['DB_USERNAME']
            self.dbPassword = appConfig['DB_PASSWORD']
            self.uuid_api_url = appConfig['UUID_API_URL']
            self.entity_api_url = appConfig['ENTITY_API_URL']
            self.search_api_url = appConfig['SEARCH_API_URL']
            self.files_api_index_name = appConfig['FILES_API_INDEX_NAME']

            self.aws_access_key_id = appConfig['AWS_ACCESS_KEY_ID']
            self.aws_secret_access_key = appConfig['AWS_SECRET_ACCESS_KEY']
            self.aws_s3_bucket_name = appConfig['AWS_S3_BUCKET_NAME']
            self.aws_object_url_expiration_in_secs = appConfig['AWS_OBJECT_URL_EXPIRATION_IN_SECS']

            if 'LARGE_RESPONSE_THRESHOLD' not in appConfig or int(appConfig['LARGE_RESPONSE_THRESHOLD'] > 9999999):
                self.logger.error("LARGE_RESPONSE_THRESHOLD missing from app.cfg or too big for AWS Gateway. Defaulting to smaller value.")
                self.large_response_threshold = 5000000
            else:
                self.large_response_threshold = int(appConfig['LARGE_RESPONSE_THRESHOLD'])
                self.logger.info(f"large_response_threshold set to {self.large_response_threshold}.")

            if not clientId:
                raise Exception("Configuration parameter APP_CLIENT_ID not valid.")
            if not clientSecret:
                raise Exception("Configuration parameter APP_CLIENT_SECRET not valid.")
        except KeyError as ke:
            self.logger.error("Expected configuration failed to load %s from appConfig=%s.", ke, appConfig)
            raise Exception("Expected configuration failed to load. See the logs.")

        if not clientId or not clientSecret:
            raise Exception("Globus client id and secret are required in AuthHelper")

        self.lock = threading.RLock()
        self.hmdb = DBConn(self.dbHost, self.dbUsername, self.dbPassword, self.dbName)

        # Keep a semi-immutable dictionary of known organs, from values used by all the microservices.
        response = requests.get(url=ORGAN_TYPES_YAML, verify=False)
        if response.status_code == 200:
            yaml_file = response.text
            try:
                self.organ_type_dict = MappingProxyType(yaml.safe_load(yaml_file))
            except yaml.YAMLError as e:
                raise yaml.YAMLError(e)
        else:
            self.logger.error(f"Unable to retrieve {ORGAN_TYPES_YAML}")
            raise HTTPException(response.status_code, f"Unable to retrieve {ORGAN_TYPES_YAML}")

        # Keep a semi-immutable dictionary of known tissues, from values used by all the microservices.
        response = requests.get(url=TISSUE_TYPES_YAML, verify=False)
        if response.status_code == 200:
            yaml_file = response.text
            try:
                self.tissue_type_dict = MappingProxyType(yaml.safe_load(yaml_file))
            except yaml.YAMLError as e:
                raise yaml.YAMLError(e)
        else:
            self.logger.error(f"Unable to retrieve {TISSUE_TYPES_YAML}")
            raise HTTPException(response.status_code, f"Unable to retrieve {TISSUE_TYPES_YAML}")


        # Keep a semi-immutable dictionary of known assay, from values used by all the microservices. From that
        # dictionary, create a reverse lookup dictionary keyed by alt-names for the
        # purpose of resolving the dataset_data_types list to a recognized column of the spreadsheet.
        response = requests.get(url=ASSAY_TYPES_YAML, verify=False)
        if response.status_code == 200:
            yaml_file = response.text
            try:
                self.assay_type_dict = MappingProxyType(yaml.safe_load(yaml_file))
            except yaml.YAMLError as e:
                raise yaml.YAMLError(e)
        else:
            self.logger.error(f"Unable to retrieve {ASSAY_TYPES_YAML}")
            raise HTTPException(response.status_code, f"Unable to retrieve {ASSAY_TYPES_YAML}")
        self.assay_type_altname_ref = {}
        # The specified alt-names entry may be either a list or a string. Convert strings to a one-element
        # list, then convert the list into a tuple to be the immutable object used as the dictionary key.
        # Set the entry value to the current, preferred value.
        for assay_type_key in self.assay_type_dict.keys():
            self.assay_type_altname_ref[(assay_type_key,)] = assay_type_key
            # Also, create an entry using the "description" as an alternate key, for use with values
            # returned using entity-api's /datasets/<dataset id>/prov-info endpoint. This can be eliminated once
            # a new entity-api endpoint is used which returns a Dataset type directly rather than the description.
            self.assay_type_altname_ref[(self.assay_type_dict[assay_type_key]['description'],)] = assay_type_key
        # Forget about Big O, loop through the dictionary again to process alt-names.
        for assay_type_key in self.assay_type_dict.keys():
            for alt_name in self.assay_type_dict[assay_type_key]['alt-names']:
                if isinstance(alt_name, str):
                    self.assay_type_altname_ref[tuple([alt_name])] = assay_type_key
                    continue
                if isinstance(alt_name, list):
                    # When an alt-names entry is a list, create entries forward & backward for
                    # it, to better match what may be encountered in the wild.
                    self.assay_type_altname_ref[tuple(alt_name)] = assay_type_key
                    self.assay_type_altname_ref[tuple(reversed(alt_name))] = assay_type_key
                    continue
                self.logger.warning(f"alt_name={str(alt_name)} not loaded for assay_type_key={assay_type_key} due to unexpected type.")

        # Set up a dictionary of dictionaries from information in the TSV file created from the analysis spreadsheet.
        # The dictionary key is a tuple to match the keys of the self.assay_type_altname_ref dictionary.
        # The dictionary value is a dictionary keyed by the specified file regex pattern, with values for the
        # description and a compiled, expanded file regular expression supporting soft-matching & grouping.
        self.dataset_desc_dict = {}
        with open(DATASET_DESCRIPTION_CSV_FILE) as tsvfile:
            reader = csv.DictReader(tsvfile, delimiter='\t')
            for row in reader:
                dict_key = tuple([row['Dataset code']])
                if not dict_key in self.dataset_desc_dict:
                    self.dataset_desc_dict[dict_key] = {}
                expanded_fpattern = '(?P<exp_pat_prefix>.*?)(?P<fpattern>'+row['file pattern']+')(?P<exp_pat_suffix>.*?)'
                pattern_dict = {'description': row['file description'], 'file_pattern_re_obj': re.compile(expanded_fpattern)}
                if row['file pattern'] in self.dataset_desc_dict[dict_key]:
                    self.logger.warning(f"Loading {DATASET_DESCRIPTION_CSV_FILE}, found existing dataset_desc_dict[{dict_key}] entry for '{row['file pattern']}', keeping '{self.dataset_desc_dict[dict_key][row['file pattern']]['description']}', skipping '{row['file description']}'.")
                else:
                    self.dataset_desc_dict[dict_key][row['file pattern']] = pattern_dict

    def _stash_results_in_S3(self, object_content, key_uuid):
        anS3Worker = None
        try:
            anS3Worker = S3Worker(self.aws_access_key_id, self.aws_secret_access_key, self.aws_s3_bucket_name,
                                  self.aws_object_url_expiration_in_secs)
            self.logger.info("anS3Worker initialized")
        except Exception as e:
            self.logger.error(f"Error getting anS3Worker to handle len(object_content)={len(object_content)}.")
            self.logger.error(e, exc_info=True)
            raise Exception("Large result storage setup error.  See log.")

        try:
            # return anS3Worker.do_whatever_with_S3()
            obj_key = anS3Worker.stash_text_as_object(object_content, key_uuid)
            aws_presigned_url = anS3Worker.create_URL_for_object(obj_key)
            return aws_presigned_url
        except Exception as e:
            self.logger.error(f"Error getting presigned URL for obj_key={obj_key}.")
            self.logger.error(e, exc_info=True)
            raise Exception("Large result storage creation error.  See log.")

    def _get_entity_generation_info(self, entity_uuid, theQuery):
        uuid_tuple = (entity_uuid,)  # N.B. comma to force creation of tuple with one value, rather than scalar

        # run the query and morph results to an array of dict
        with closing(self.hmdb.getDBConnection()) as dbConn:
            with closing(dbConn.cursor(prepared=True)) as curs:
                # query that finds all files associated with entity by joining the ancestors table (entity is
                # the ancestor, files are the descendants) with the files table
                curs.execute(theQuery
                             ,uuid_tuple)
                results = [dict((curs.description[i][0].lower(), value) for i, value in enumerate(row)) for row in
                           curs.fetchall()]

        return json.dumps(results)


    # Rely on the search-api to delete all documents matching the provided Dataset UUID
    def _clear_dataset_file_info_docs(self, dataset_uuid):
        # If the self.user_token is not set for this instance, do not process request.
        if not self.user_token:
            raise requests.exceptions.HTTPError(response=Response("Valid Globus groups token required", 401))

        post_url = self.search_api_url + '/clear-docs/' + self.files_api_index_name + '/' + dataset_uuid
        headers = {'Authorization': 'Bearer ' + self.user_token}
        params = {'async': True}
        rspn = requests.post(f"{post_url}", headers=headers, params=params)
        return rspn

    def _get_dataset_files_info(self, dataset_uuid):
        # If the self.user_token is not set for this instance, do not process request.
        if not self.user_token:
            raise requests.exceptions.HTTPError(response=Response("Valid Globus groups token required",401))

        get_url = self.uuid_api_url + '/' + dataset_uuid + '/files'
        response = requests.get(get_url, headers = {'Authorization': 'Bearer ' + self.user_token}, verify = False)
        if response.status_code == 200:
            return json.dumps(response.json())
        elif response.status_code == 303:
            if response.text.startswith('https://hm-api-responses.s3.amazonaws.com/'):
                # Get the JSON response from the S3 bucket and return it.
                bucket_response = requests.get(response.text)
                return bucket_response.text
            else:
                raise Exception(f"Unexpected {response.status_code} response for dataset_uuid={dataset_uuid}.")
        else:
            raise requests.exceptions.HTTPError(response=response)

    # Use the entity-api service to provenance info of a given Dataset identifier.
    # input: id (hubmap_id or uuid) of a Dataset entity
    # output: YAML with info from Neo4j
    def _get_dataset_prov_info(self, dataset_id):
        # If the self.user_token is not set for this instance, do not process request.
        if not self.user_token:
            raise requests.exceptions.HTTPError(response=Response("Valid Globus groups token required",401))

        get_url = self.entity_api_url + '/datasets/' + dataset_id + '/prov-info?include_samples=all&format=json'
        response = requests.get(get_url, headers={'Authorization': 'Bearer ' + self.user_token}, verify=False)
        if response.status_code != 200:
            self.logger.error(f"For dataset_id={dataset_id}, get_url={get_url} returned status_code={response.status_code}: {response.text}.")
            raise requests.exceptions.HTTPError(response=response)
        return response.json()

    # Use the entity-api service to get provenance info of a given Dataset identifier.
    # input: id (hubmap_id or uuid) of a Dataset entity
    # output: YAML with info from Neo4j
    def _get_entity(self, entity_id, entity_type_check=None):
        # If the self.user_token is not set for this instance, do not process request.
        if not self.user_token:
            raise requests.exceptions.HTTPError(response=Response("Valid Globus groups token required",401))

        get_url = self.entity_api_url + '/entities/' + entity_id
        response = requests.get(get_url, headers={'Authorization': 'Bearer ' + self.user_token}, verify=False)
        if response.status_code != 200:
            raise requests.exceptions.HTTPError(response=response)
        if entity_type_check:
            id_attributes = json.loads(response.text)
            if id_attributes['entity_type'].lower() != entity_type_check.lower():
                raise Exception(f"Identifier {entity_id} type is {id_attributes['entity_type']}, not {entity_type_check}.")
        return response.json()

    # Use the entity-api service to get all the entities of a given type.
    # input: An entity type recognized by the entity-api
    # output: @TODO YAML with info from Neo4j
    def _get_all_entities(self, entity_type):
        # Rely on the type checking the entity-api does with entity-type.
        get_url = self.entity_api_url + '/' + entity_type + '/entities'
        response = requests.get(get_url)
        if response.status_code != 200:
            raise requests.exceptions.HTTPError(response=response)
        return response.json()

    # Use the search-api service "add" the document to the index for files
    # input: An entity type recognized by the entity-api
    # output: @TODO YAML with info from Neo4j
    def _write_or_update_doc(self, es_doc, id, index_name):
        # Rely on the search-api to determine if writing or updating.

        # If the self.user_token is not set for this instance, do not process request.
        if not self.user_token:
            raise requests.exceptions.HTTPError(response=Response("Valid Globus groups token required",401))

        post_url = self.search_api_url + '/add/' + id + '/' + index_name
        headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + self.user_token}
        params = {'async': True} #@TODO-KBKBKB undo hard coding, pass all the way down from files-api endpoint call?
        rspn = requests.post(f"{post_url}", headers=headers, data=json.dumps(es_doc), params=params)
        return rspn

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
            return Response(self._stash_results_in_S3(object_content=results_json, key_uuid=uuid_tuple[0]), 303)

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
    def get_dataset_file_infos(self, dataset_uuid):

        # @TODO-Once non-primary Dataset prov-info can be retrieved, test with Datasets descended from
        # multiple Datasets, like 65b92f0191dc73e9470f46ceb217054d or 77cdce75f41a8b9260727070b16b2ae5

        # Get what uuid-api knows about the Dataset's files
        files_info = self._get_dataset_files_info(dataset_uuid)
        files_list = json.loads(files_info)

        # Get what entity-api knows about the Dataset's files and ancestors
        entity_prov_info = self._get_dataset_prov_info(dataset_uuid)

        tissue_samples_dict_list = []
        organs_dict_list = []
        donors_dict_list = []
        for sample_uuid in entity_prov_info['dataset_samples'].keys():
            sample_dict = {}
            specimen_type = entity_prov_info['dataset_samples'][sample_uuid]['specimen_type']
            if specimen_type != 'organ':
                sample_dict['uuid'] = sample_uuid
                sample_dict['code'] = specimen_type
                sample_dict['type'] = self.tissue_type_dict[specimen_type]['description']
                tissue_samples_dict_list.append(sample_dict)

        # Determine the current Dataset type with an acceptable description, given
        # the value on the returned entity. Recognized Dataset types are the keys and
        # the alt-names entries of the assay_types.yaml file of the search-api.
        #
        # Convert the entity Dataset type info to a tuple that can be used for dictionary lookup.
        if isinstance(entity_prov_info['dataset_data_types'],str):
            entity_ds_type = (entity_prov_info['dataset_data_types'],)
        elif isinstance(entity_prov_info['dataset_data_types'],list):
            entity_ds_type = tuple(entity_prov_info['dataset_data_types'],)
        else:
            self.logger.warning(f"For Dataset entity of type '{entity_prov_info['dataset_data_types']}', unable to build key tuple to look up description in dictionary.")

        # Set up an empty dictionary, and try to replace it with one appropriate for the
        # Dataset data type.
        dataset_type_desc_dict = {}
        if entity_ds_type:
            if entity_ds_type in self.dataset_desc_dict:
                dataset_type_desc_dict = self.dataset_desc_dict[entity_ds_type]
            else:
                if entity_ds_type in self.assay_type_altname_ref:
                    alt_entity_ds_type = tuple([self.assay_type_altname_ref[entity_ds_type]])
                    if alt_entity_ds_type in self.dataset_desc_dict:
                        dataset_type_desc_dict = self.dataset_desc_dict[alt_entity_ds_type]
                        self.logger.warning(f"For Dataset entity of type '{entity_prov_info['dataset_data_types']}', using assay_types.yaml alt-names entry {str(alt_entity_ds_type)}.")
                    else:
                        self.logger.warning(f"For Dataset entity of type '{str(alt_entity_ds_type)}', no description in analysis file {DATASET_DESCRIPTION_CSV_FILE}.")
                else:
                    self.logger.warning(f"For Dataset entity of type '{str(entity_ds_type)}', unable find description in dictionary.")
        else:
            # If the tuple for the entity dataset type is not set, the previous block logs a warning, and
            # the following code below will result in the description attribute being left off the JSON of
            # each File in the Response by using the empty dictionary dataset_type_desc_dict.
            pass

        for organ_uuid in entity_prov_info['organ_uuid']:
            donor_dict = {}
            organ_dict = {}
            organ_dict['uuid'] = organ_uuid

            organ_info = self._get_entity(entity_id=organ_uuid, entity_type_check='Sample')
            if not organ_info['specimen_type'] or organ_info['specimen_type'] != 'organ':
                continue
            donor_dict['uuid'] = organ_info['direct_ancestor']['uuid'] if organ_info['direct_ancestor']['uuid'] else None
            if not organ_info['organ']:
                raise Exception(f"Unable to identify organ type for {organ_uuid} for dataset {dataset_uuid}.")
            if not organ_info['direct_ancestor']:
                raise Exception(f"Unable to identify donor for {organ_uuid} for dataset {dataset_uuid}.")
            organ_dict['type_code'] = organ_info['organ'] if organ_info['organ'] else None
            try:
                organ_dict['type'] = self.organ_type_dict[organ_dict['type_code']] if organ_dict['type_code'] and self.organ_type_dict[organ_dict['type_code']] else None
            except KeyError as ke:
                organ_dict['type'] = 'Unrecognized organ code: ' + organ_dict['type_code']
            if 'organ_donor_data' in organ_info['direct_ancestor']['metadata']:
                for concept in organ_info['direct_ancestor']['metadata']['organ_donor_data']:
                    if concept['grouping_concept'] == UMLS_AGE_GROUP_CUI:
                        donor_dict['age'] = float(concept['data_value']) if concept['data_value'] else None
                        donor_dict['units'] = concept['units'] if concept['units'] else None
                    if concept['grouping_concept'] == UMLS_RACE_GROUP_CUI:
                        donor_dict['race'] = concept['preferred_term'] if concept['preferred_term'] else None
            if donor_dict:
                donors_dict_list.append(donor_dict)
            organs_dict_list.append(organ_dict)

        dataset_file_info_list = []
        for file_info in files_list:
            #file_info['description'] = None # No default value. Do not include in Response if not filled
            #file_info['type_code'] = f"TBD-OPTIONAL the code of the associated ontology term for the file as resolved through the HuBMAP application ontology...not {str(entity_ds_type)}"
            file_info['rel_path'] = file_info['path']
            # Check the relative path of the file against file regex patterns associated with this
            # Dataset type, to see if a description is available
            for fpattern in dataset_type_desc_dict.keys():
                matchobj = dataset_type_desc_dict[fpattern]['file_pattern_re_obj'].search(file_info['rel_path'])
                # First tuple is expanded_pattern prefix .*, named exp_pat_prefix
                # Last tuple is expanded_prefix suffix .*, named exp_pat_suffix
                # Tuples in between first and last match the entire end-user provided file regular expression, as well
                # as any elements in the file regex which are in parentheses.
                # The exact match to the end-user provided file regex is the tuple named fpattern

                if matchobj:
                    if matchobj.group('fpattern'):
                        if not matchobj.group('exp_pat_prefix') and not matchobj.group('exp_pat_suffix'):
                            # Exact match
                            file_info['description'] = dataset_type_desc_dict[fpattern]['description']
                            break
                        else:
                            # Partial match, embedded between some prefix and suffix on the rel_path
                            pass
                            '''
                            # Enable this block of code to shove tab-separated entries into the log which
                            # can be filtered out by the FLAG* pattern, then moved to a spreadsheet for analysis.
                            # This statement shows partial matches, so an analysis of them can be requested.
                            self.logger.debug("FLAG_GAP_DATASET_UUIDS_PARENTING_FILES" + "\t" +
                                              f"{dataset_uuid}" + "\t" +
                                              f"{str(entity_ds_type)}" + "\t" +
                                              f"{fpattern}" + "\t" +
                                              f"{file_info['rel_path']}" + "\t" +
                                              f"{matchobj.group('exp_pat_prefix')}" + "\t" +
                                              f"{matchobj.group('fpattern')}" + "\t" +
                                              f"{matchobj.group('exp_pat_suffix')}")
                            '''

            # The file extension is everything after the last period, if there is any period. Blank otherwise.
            #@TODO-KBKBKB-peel off the leaf of rel_path, which should be just the file name, and find file_extension of its last period. Align with file-api-spec.yaml
            file_info['file_extension'] = file_info['rel_path'][file_info['rel_path'].rindex('.')+1:] if file_info['rel_path'].find('.') > -1 else ''
            file_info['samples'] = tissue_samples_dict_list
            file_info['organs'] = organs_dict_list
            file_info['donors'] = donors_dict_list
            file_info['dataset_uuid'] = dataset_uuid
            file_info['data_types'] = entity_prov_info['dataset_data_types']
            file_info.pop('path')
            file_info.pop('base_dir')
            dataset_file_info_list.append(file_info)

        results_json = json.dumps(dataset_file_info_list)
        #results_json = files_info + anc_info + desc_info

        return Response(response=results_json
                        , mimetype="application/json")
        # if len(results_json) < self.large_response_threshold:
        #     return Response(response=results_json
        #                     , mimetype="application/json")
        # else:
        #     return Response(self._stash_results_in_S3(object_content=results_json
        #                                               ,key_uuid=dataset_uuid)
        #                     , 303)

    # Use the uuid-api service to find out the uuid of a given identifier, for
    # use with endpoints requiring a uuid as the identifier.
    # input: id (hubmap_id or uuid) of an entity
    # output: the uuid of the entity
    def get_identifier_info(self, entity_id, entity_type_check=None):
        # If the self.user_token is not set for this instance, do not process request.
        if not self.user_token:
            raise requests.exceptions.HTTPError(response=Response("Valid Globus groups token required",401))

        get_url = self.uuid_api_url + '/uuid/' + entity_id
        response = requests.get(get_url, headers = {'Authorization': 'Bearer ' + self.user_token}, verify = False)
        if response.status_code != 200:
            raise requests.exceptions.HTTPError(response=response)
        if entity_type_check:
            id_attributes = json.loads(response.text)
            if id_attributes['type'].lower() != entity_type_check.lower():
                raise Exception(f"Identifier {entity_id} type is {id_attributes['type']}, not {entity_type_check}.")
        return response.json()['uuid']

    # Use the entity-api service to get all the entities of a given type.
    # input: An entity type found in the ID_ENTITY_TYPES list commonly repeated in each service's app.cfg.
    # output: @TODO YAML with info from Neo4j
    def get_all_public_datasets(self):
        # Rely on the type checking the entity-api does with entity-type.
        theDatasets = self._get_all_entities('DATASET')

        datasetIdentifierList = []
        for aDataset in theDatasets:
            if aDataset['status'] == 'Published' and not aDataset['contains_human_genetic_sequences']:
                datasetIdentifierList.append(aDataset['uuid'])
        return datasetIdentifierList

    # Use the entity-api service to get the entity for the given identifier, if
    # the entity-type @TODO
    # input: id (hubmap_id or uuid) of an entity
    # output: the @TODO of the entity
    def get_entity(self, entity_id, entity_type_check=None):
        theEntity = self._get_entity(entity_id=entity_id, entity_type_check=entity_type_check)
        return theEntity

    # Use the entity-api service to get all the entities of a given type.
    # input: An entity type found in the ID_ENTITY_TYPES list commonly repeated in each service's app.cfg.
    # output: @TODO YAML with info from Neo4j
    def write_or_update_doc(self, file_info):
        self.logger.debug(f"Putting file_info of size {len(file_info)} in index {self.files_api_index_name}.")
        resp = self._write_or_update_doc(es_doc=file_info,
                                         id=file_info['file_uuid'],
                                         index_name=self.files_api_index_name)
        if resp.status_code not in [200, 202]:
            raise requests.exceptions.HTTPError(response=resp)
        else:
            return resp #@TODO-should we transform queuing msg to something nice?

    # Verify any requirements or permissions need to proceed with indexing operations for
    # the specified Dataset.  If no Dataset is specified, verify admin privileges to index all Datasets.
    def verify_indexable(self, aDataset):
        if aDataset is None:
            if self.auth_helper.has_data_admin_privs(self.user_token):
                return True
            else:
                raise Exception(f"Permission denied for indexing all Datasets.")

        # Confirm the retrieved Dataset is a public Dataset
        # Align constants with search-api indexer_base.py Indexer.DATASET_STATUS_PUBLISHED
        if aDataset['status'] != 'Published' or aDataset['contains_human_genetic_sequences']:
            raise Exception(f"Only public datasets may be indexed. 'status'={aDataset['status']} or"
                            + f" 'contains_human_genetic_sequences'={aDataset['contains_human_genetic_sequences']}"
                            + " not allowed")

        # Verify the user has write permission for the entity whose documents are to be cleared from the ES index
        entity_group_uuid = aDataset['group_uuid']
        if not entity_group_uuid in self.user_groups_by_id_dict.keys() and \
                not self.auth_helper.has_data_admin_privs(self.user_token):
            raise Exception(f"Permission denied for modifying ES index entries for '{aDataset['uuid']}'.")

        return True

    # Get all the files for a Dataset, build a file info document for each, and add each file info document to
    # the Elasticsearch index.
    def index_dataset(self, aDatasetUUID):

        isBulk = False #@TODO-KBKBKB ditch this variable if Elasticsearch Bulk API can easily replace N search-api threads for a Dataset with N files.
        if isBulk:
            pass
            '''
            @TODO-KBKBKB-is we is or is we ain't doin' Bulk API if we expect requests overwhelming Elasticsearch?
            # Create a fresh file infos document for the specified Dataset from the dataset-file-info endpoint. @TODO-if the "reindex" option is specified but the file-info cannot be created, should this return normal, empty response, and effectively go on to delete from index?  Or should it halt before removing from the index, and tell them to use a DELETE endpoint if they want to delete? Can index contain an empty doc for a Dataset with no files yet?
            # Connect to the database and retrieve the information for files
            # descended from the entity.
            dataset_files_info_response = self.get_dataset_file_infos(aDatasetUUID)
            files_info_list = dataset_files_info_response.get_json()

            if not files_info_list:
                self.logger.error(
                    f"Unable to retrieve the file set JSON to do indexing for dataset_uuid={aDatasetUUID}")
                # @TODO-verify with [] response. Decide if Exception should actually be raised.
                raise Exception(f"Unexpected JSON content for dataset_uuid={aDatasetUUID}")

            # Try clearing the documents for the Dataset before inserting current documents, in case
            # Files were removed from the Dataset since initially put in the index.  But don't skip
            # inserting files if deletion is not successful.
            try:
                self._clear_dataset_file_info_docs(aDatasetUUID)
            except Exception as e:
                self.logger.error(f"While clearing existing file info documents from {self.files_api_index_name} for"
                                  f" {aDatasetUUID}, encountered {e.text}. Continuing with insertion.")
            self.logger.critical("@TODO-KBKBKB implement this with the Elasticsearch Bulk API.")
            # Re-work the full dictionary of responses from each search-api /add operation into
            # something more compact from the files-api.
            index_response_dict = {}

            ELASTICSEARCH_REQUEST_ACTION_DELIMITER = '\n'
            https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
            POST / <target>/_bulk
            POST / _bulk
            {"index": {"_index": "my_index", "_id": "1", "dynamic_templates": {"work_location": "geo_point"}}}
            {"field": "value1", "work_location": "41.12,-71.34", "raw_location": "41.12,-71.34"}
            {"create": {"_index": "my_index", "_id": "2", "dynamic_templates": {"home_location": "geo_point"}}}
            {"field": "value2", "home_location": "41.12,-71.34"}
            {"index": {"_index": "test", "_id": "1"}}
            {"field1": "value1"}
            for file_info_dict in files_info_list:
                bulk_ES_request = f"POST /{self.files_api_index_name}/_bulk{ELASTICSEARCH_REQUEST_ACTION_DELIMITER}"


            bulk_load_resp = self.bulk_load_docs(bulk_ES_request)
            index_response_dict[file_info_dict['file_uuid']] = bulk_load_resp.text
            self.logger.info(f"File {file_info_dict['file_uuid']} for dataset_uuid={aDatasetUUID} returned '{bulk_load_resp.text}'")

            return index_response_dict
            '''
        else:
            # Create a fresh file infos document for the specified Dataset from the dataset-file-info endpoint. @TODO-if the "reindex" option is specified but the file-info cannot be created, should this return normal, empty response, and effectively go on to delete from index?  Or should it halt before removing from the index, and tell them to use a DELETE endpoint if they want to delete? Can index contain an empty doc for a Dataset with no files yet?
            # Connect to the database and retrieve the information for files
            # descended from the entity.
            dataset_files_info_response = self.get_dataset_file_infos(aDatasetUUID)
            files_info_list = dataset_files_info_response.get_json()

            if not files_info_list:
                self.logger.error(f"Unable to retrieve the file set JSON to do indexing for dataset_uuid={aDatasetUUID}")
                # @TODO-verify with [] response. Decide if Exception should actually be raised.
                raise Exception(f"Unexpected JSON content getting file info documents for dataset_uuid={aDatasetUUID}")

            # Try clearing the documents for the Dataset before inserting current documents, in case
            # Files were removed from the Dataset since initially put in the index.  But don't skip
            # inserting files if deletion is not successful.
            try:
                self._clear_dataset_file_info_docs(aDatasetUUID)
            except Exception as e:
                self.logger.error(f"While clearing existing file info documents from {self.files_api_index_name} for"
                                  f" {aDatasetUUID}, encountered {e.text}. Continuing with insertion.")

            # Re-work the full dictionary of responses from each search-api /add operation into
            # something more compact from the files-api.
            self.logger.info(f"For Dataset '{aDatasetUUID}'."
                             f" inserting {len(files_info_list)} file info documents"
                             f" into {self.files_api_index_name}."
                             )
            index_response_dict = {}
            for file_info_dict in files_info_list:
                file_resp = self.write_or_update_doc(file_info_dict)
                index_response_dict[file_info_dict['file_uuid']] = file_resp.text
                #@TODO-KBKBKB undo, after not I/O bound...self.logger.info(f"File {file_info_dict['file_uuid']} for dataset_uuid={aDatasetUUID} returned '{file_resp.text}'")

            return index_response_dict

    # Get all the Datasets, and loop through each one.  Add a file info document to
    # the Elasticsearch index for each File in an indexable Dataset.
    def index_all_datasets(self):
        inserted_datasets_list = []
        failed_datasets_list = []
        skipped_datasets_list = []

        try:
            datasetList = self.get_all_public_datasets()
            # @TODO-KBKBKB undo
            #datasetList = datasetList[11:30]
            #datasetList = ['654418415bed5ecb9596b17a0320a2c6', '077f7862f6306055899374c7807a30c3', 'd3130f4a89946cc6b300b115a3120b7a', 'cd880c54e0095bad5200397588eccf81', 'a296c763352828159f3adfa495becf3e', '298caad597d4a9eaaa3edbc89d79db82','073cad035ce246a0134e2214569adde9', 'b6eba6afe660a8a85c2648e368b0cf9f']
            # @TODO-KBKBKB end undo
            self.logger.info(f"Processing {len(datasetList)} Datasets for {self.files_api_index_name} index inclusion.")
            index_response_dict = {}

            for dataset_uuid in datasetList:
                try:
                    theDataset = self.get_entity(entity_id=dataset_uuid, entity_type_check='DATASET')

                    # Even though verified admin privileges to get this far, verify Dataset attributes are
                    # compatible with index inclusion.
                    # N.B. does not test for "primary" Datasets, which may cause an exception below, but which
                    #      should also be allowed to be indexed in the near future.
                    self.verify_indexable(aDataset=theDataset)

                    index_response_dict[dataset_uuid] = self.index_dataset(aDatasetUUID=dataset_uuid)

                    inserted_datasets_list.append(dataset_uuid)
                    self.logger.info(f"Finished updating {self.files_api_index_name} for all file info documents"
                                     f" for Dataset '{dataset_uuid}'.")
                except Exception as eDataset:
                    if eDataset.response.status_code == 400 and \
                       re.match(".*Make sure this is a Primary Dataset.*", str(eDataset.response.text)):
                        skipped_datasets_list.append(dataset_uuid)
                        self.logger.warning(f"While updating {self.files_api_index_name} for all file info documents"
                                            f" for Dataset '{dataset_uuid}'"
                                            f", skipped the Dataset. Check if a Primary Dataset.")
                    else:
                        failed_datasets_list.append(dataset_uuid)
                        self.logger.error(f"While updating {self.files_api_index_name} for all file info documents"
                                          f" for Dataset '{dataset_uuid}'"
                                          f", got '{eDataset.response.text}'.")
                    # Continue this loop for other Datasets in datasetList
        except Exception as eWholeList:
            self.logger.error(f"While updating {self.files_api_index_name} for all file info documents"
                              f", got e='{str(eWholeList)}'.")
            raise Exception(f"An error was encountered while updating"
                            f"{self.files_api_index_name} for all file info documents."
                            f" See logs.")
        self.logger.info(f"Inserted entries for {len(inserted_datasets_list)} Datasets in ES {self.files_api_index_name} index.")
        self.logger.info(f"Skipped {len(skipped_datasets_list)} Datasets (due to unsupported characteristics).")
        self.logger.info(f"Failed to index {len(failed_datasets_list)} Datasets (due to logged errors).")
        allDatasetResultDict = {}
        allDatasetResultDict['failed'] = failed_datasets_list
        allDatasetResultDict['skipped'] = skipped_datasets_list
        allDatasetResultDict['succeeded'] = inserted_datasets_list
        return Response(json.dumps(allDatasetResultDict))

