import csv
import logging
import re
import threading
import json
from datetime import datetime, timezone
import dateutil.parser
from http.client import HTTPException
from string import Template
from sys import getsizeof

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

# Default values for scrolling an OpenSearch index using the search-api endpoint
SCROLL_HITS_PER_READ = 10000
SCROLL_OPEN_MINUTES_READ = 10
SCROLL_OPEN_MINUTES_DUMP = 0

# UMLS Concept Unique Identifiers used to encode entity-api information
UMLS_AGE_GROUP_CUI = 'C0001779'
UMLS_RACE_GROUP_CUI = 'C0034510'

# Yaml file to be parsed for organ description lookup
ORGAN_TYPES_YAML = 'https://raw.githubusercontent.com/hubmapconsortium/search-api/main/src/search-schema/data/definitions/enums/organ_types.yaml'
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

SQL_SELECT_MOD_TIME_OF_DATASET_FILES = \
    ("SELECT uuidFile.UUID AS file_uuid"
     "       ,uuidFile.TIME_GENERATED AS file_uuid_gen_time"
     "       ,f.LAST_MODIFIED AS file_last_modified"
     "       ,uuidDataset.UUID AS dataset_uuid"
     "       ,uuidDataset.TIME_GENERATED AS dataset_uuid_gen_time"
     " FROM files AS f"
     "  INNER JOIN uuids AS uuidFile ON f.UUID = uuidFile.UUID"
     "  INNER JOIN ancestors AS ancDataset ON f.UUID = ancDataset.DESCENDANT_UUID"
     "   INNER JOIN uuids AS uuidDataset ON ancDataset.ANCESTOR_UUID=uuidDataset.UUID"
     " WHERE uuidDataset.ENTITY_TYPE='dataset'"
     )

from enum import Enum
class DatasetIndexScopeType(Enum):
    GENETIC = 'GENETIC'
    NONPUBLIC = 'NONPUBLIC'
    PUBLIC = 'PUBLIC'

class FileWorker:

    def __init__(self, appConfig=None, requestHeaders=None):
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)
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
            self.uuid_api_url = appConfig['UUID_API_URL'].strip('/')
            self.entity_api_url = appConfig['ENTITY_API_URL'].strip('/')
            self.search_api_url = appConfig['SEARCH_API_URL'].strip('/')
            self.files_api_public_index = appConfig['FILES_API_PUBLIC_INDEX']
            self.files_api_nonpublic_index = appConfig['FILES_API_NONPUBLIC_INDEX']

            self.max_docs_per_scroll_page = appConfig['MAX_DOCS_PER_SCROLL_PAGE']
            self.max_time_open_scroll_context = appConfig['MAX_TIME_OPEN_SCROLL_CONTEXT']

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
    def _clear_dataset_file_info_docs(self, es_index_name, dataset_uuid, bearer_token):

        post_url = self.search_api_url + '/clear-docs/' + es_index_name + '/' + dataset_uuid
        headers = {'Authorization': 'Bearer ' + bearer_token}
        params = {'async': True}
        rspn = requests.post(f"{post_url}", headers=headers, params=params)
        return rspn

    def _get_dataset_files_info(self, dataset_uuid, bearer_token):

        get_url = self.uuid_api_url + '/' + dataset_uuid + '/files'
        response = requests.get(get_url, headers = {'Authorization': 'Bearer ' + bearer_token}, verify = False)
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
    def _get_dataset_prov_info(self, dataset_id, bearer_token):

        get_url = self.entity_api_url + '/datasets/' + dataset_id + '/prov-info?include_samples=all&format=json'
        response = requests.get(get_url, headers={'Authorization': 'Bearer ' + bearer_token}, verify=False)
        if response.status_code != 200:
            self.logger.error(f"For dataset_id={dataset_id}, get_url={get_url} returned status_code={response.status_code}: {response.text}.")
            ## A status_code of 400 is returned for non-primary Datasets.  This may change in the future, but
            ## do not raise an exception about it or halt processing.
            # raise requests.exceptions.HTTPError(response=response)
        return response.json()

    # Use the entity-api service to get provenance info of a given Dataset identifier.
    # input: id (hubmap_id or uuid) of a Dataset entity
    # output: JSON with info from Neo4j. Exception raised if response is not 200, or
    #         if the returned JSON does not contain an entity match the type specified in
    #         the optional argument entity_type_check.
    def _get_entity(self, entity_id, bearer_token, entity_type_check=None):

        get_url = self.entity_api_url + '/entities/' + entity_id
        response = requests.get(get_url, headers={'Authorization': 'Bearer ' + bearer_token})
        if response.status_code != 200:
            raise requests.exceptions.HTTPError(response=response)
        if entity_type_check:
            id_attributes = json.loads(response.text)
            if id_attributes['entity_type'].lower() != entity_type_check.lower():
                raise Exception(f"Identifier {entity_id} type is {id_attributes['entity_type']}, not {entity_type_check}.")
        return response.json()

    # Use the entity-api service to get all the entities of a given type.
    # N.B. This uses an entity-api endpoint not accessible through the AWS Gateway, and
    #      therefore relies upon Docker configuration on the same server.
    # input: An entity type recognized by the entity-api
    # output: @TODO YAML with info from Neo4j
    def _get_all_entities_identifiers(self, entity_type, bearer_token):
        # Rely on the type checking the entity-api does with entity-type.
        get_url = self.entity_api_url + '/' + entity_type + '/entities'
        response = requests.get(get_url, headers={'Authorization': 'Bearer ' + bearer_token})
        if response.status_code != 200:
            raise requests.exceptions.HTTPError(response=response)
        return response.json()

    # Use the search-api service "add" the document to the index for files.
    # Rely on the search-api to determine if writing or updating.
    # input: An entity type recognized by the entity-api
    # output: @TODO YAML with info from Neo4j
    def _write_or_update_doc(self, es_index_name, es_doc_dict, bearer_token):
        file_id = es_doc_dict['file_uuid']
        self.logger.debug(  f"For file_id={file_id}"
                            f" putting file_info with {len(es_doc_dict)} entries of size"
                            f" {getsizeof(json.dumps(es_doc_dict))} bytes in index {es_index_name}.")

        post_url = self.search_api_url + '/add/' + file_id + '/' + es_index_name
        headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + bearer_token}
        params = {'async': True}
        rspn = requests.post(f"{post_url}", headers=headers, data=json.dumps(es_doc_dict), params=params)
        if rspn.status_code not in [200, 202]:
            raise requests.exceptions.HTTPError(response=rspn)
        return rspn

    # Use the entity-api service to get all the entities of type Dataset, then
    # discard all marked as containing genetic information
    # input: A token for querying the entity-api
    # output: A list of UUIDs for Datasets which do not have genetic information.
    def _get_all_nongenetic_datasets(self, bearer_token):
        # Rely on the type checking the entity-api does with entity-type.
        theDatasets = self._get_all_entities_identifiers('DATASET', bearer_token=bearer_token)

        datasetIdentifierList = []
        for aDataset in theDatasets:
            if not aDataset['contains_human_genetic_sequences']:
                datasetIdentifierList.append(aDataset['uuid'])
        return datasetIdentifierList

    # Wrapper for calls to methods which return datasets which belong in the indices
    # input: A token for querying the entity-api
    # output: A list of UUIDs for Datasets whose Files info should be indexed.
    def _get_indexable_datasets(self, bearer_token):
        return self._get_all_nongenetic_datasets(bearer_token=bearer_token)

    # Return a value from DatasetIndexScopeType which can be used to determine
    # which Elasticsearch index the Dataset's documents should be put into.
    def _get_dataset_scope(self, aDataset):
        # Confirm the retrieved Dataset is a public Dataset
        # Align constants with search-api indexer_base.py Indexer.DATASET_STATUS_PUBLISHED
        if aDataset['contains_human_genetic_sequences']:
            return DatasetIndexScopeType.GENETIC
        elif aDataset['status'] == 'Published': # and preceding indicates not genetic
            return DatasetIndexScopeType.PUBLIC
        else:
            return DatasetIndexScopeType.NONPUBLIC

    # Add search results for File index times to an accumulating dictionary keyed by Dataset UUID
    def _accumulate_hits(self, new_search_hits, hit_accum_dict):
        # Count on the _id of each hit to be the File UUID also found as hit.fields.file_uuid
        for hit in new_search_hits:
            for dataset_uuid in hit['fields']['dataset_uuid']:
                try:
                    if dataset_uuid not in hit_accum_dict:
                        hit_accum_dict[dataset_uuid] = {}
                    # Expect to not get the same File UUID twice in new_search_hits, so not
                    # testing if hit['_id'] entry would be an overwrite.
                    hit_accum_dict[dataset_uuid][hit['_id']] = dateutil.parser.parse(hit['fields']['file_info_refresh_timestamp'][0])
                except KeyError as ke:
                    raise ke
        return len(new_search_hits)

    # Form a Dataset UUID keyed dictionary containing TIME_GENERATED info from MySQL query results
    def _get_mod_time_dict(self, new_query_results):
        # Form a dictionary keyed by Dataset UUID, containing the "time generated" for the
        # Dataset UUID, and a dictionary of file information for the Dataset. The contained
        # dictionary is keyed by File UUID, and contains the "last modified time" of that UUID.
        gen_time_dict = {}
        for row in new_query_results:
            try:
                if not row['dataset_uuid'] in gen_time_dict:
                    awareUTCDatasetGenTime = row['dataset_uuid_gen_time'].replace(tzinfo=timezone.utc)
                    gen_time_dict[row['dataset_uuid']] = {"uuid_gen_time": awareUTCDatasetGenTime
                                                          ,"dataset_files": {}}
                awareUTCFileModTime = row['file_last_modified'].replace(tzinfo=timezone.utc)
                gen_time_dict[row['dataset_uuid']]['dataset_files'][row['file_uuid']] = awareUTCFileModTime
            except KeyError as ke:
                raise ke
        return gen_time_dict
    def _get_modification_time_of_dataset_files(self):

        # run the query and morph results to an array of dict
        with closing(self.hmdb.getDBConnection()) as dbConn:
            with closing(dbConn.cursor(prepared=True)) as curs:
                # query that finds all files associated with entity by joining the ancestors table (entity is
                # the ancestor, files are the descendants) with the files table
                curs.execute(SQL_SELECT_MOD_TIME_OF_DATASET_FILES)
                results = [dict((curs.description[i][0].lower(), value) for i, value in enumerate(row)) for row in
                           curs.fetchall()]

        return results

        # if len(results_json) < self.large_response_threshold:
        #     return Response(response=results_json, mimetype="application/json")
        # else:
        #     return Response(self._stash_results_in_S3(object_content=results_json, key_uuid=uuid_tuple[0]), 303)

    #
    def _read_all_index_results(self, bearer_token):
        scroll_id = None
        current_read_hits = []
        # Set up a dictionary which can be converted to JSON for opening a scroll search
        # N.B. "scroll_open_minutes" is not a part of the OpenSearch JSON, but is how we signal
        #      search-api's single scroll-search endpoint what to do.  In the future, if making
        #      the search-api scroll-search public means more endpoints compatible with OpenSearch,
        #      this method will be rewritten and can become more generic by passing in "query".
        scroll_open_json_dict = {"scroll_open_minutes": SCROLL_OPEN_MINUTES_READ
                                 ,"size": SCROLL_HITS_PER_READ
                                 ,"query": {"match_all": {}}
                                 ,"fields": [ "file_uuid", "dataset_uuid", "file_info_refresh_timestamp"]
                                 ,"_source": False
                                 }
        # Set up a dictionary which can be converted to JSON for continued reading of a scroll
        scroll_read_json_dict = {"scroll_open_minutes": SCROLL_OPEN_MINUTES_READ
                                    ,"scroll_id": "set after each response"}
        # Set up a dictionary which can be converted to JSON for closing a scroll
        scroll_dump_json_dict = {"scroll_open_minutes": SCROLL_OPEN_MINUTES_DUMP
                                    ,"scroll_id": "set after final response"}
        # Open a scroll using the search-api composite index name 'files', expecting to
        # get a scroll on the "consortium" (aka "private") OpenSearch index.  Assume the
        # corresponding OpenSearch "public" index contains a subset of the "consortium" entries, each
        # in the same state in both indices, as maintained by this method.
        post_url = f"{self.search_api_url}/files/scroll-search"

        headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + bearer_token}

        try:
            # Open a scroll using an open dictionary configured above
            rspn_open = requests.post(post_url, headers=headers, data=json.dumps(scroll_open_json_dict))
        except ConnectionError as ce:
            self.logger.error("Failure to open scroll. See JSON")
        except Exception as e:
            raise Exception(f"Scroll search failed due to {str(e)}")
        if rspn_open.status_code not in [200]:
            raise requests.exceptions.HTTPError(response=rspn_open)
        else:
            json_open_rspn = rspn_open.json()
            if '_scroll_id' in json_open_rspn:
                scroll_id = json_open_rspn['_scroll_id']
                current_read_hits = json_open_rspn['hits']
            else:
                self.logger.error(f"Missing '_scroll_id' during scroll open in JSON {json_open_rspn}")
                raise requests.exceptions.HTTPError(response=rspn_open)
        # Hang onto the scroll_id for further OpenSearch operations
        scroll_dump_json_dict['scroll_id'] = scroll_id
        scroll_read_json_dict['scroll_id'] = scroll_id

        hits_size = 0
        all_hits = {}

        try:
           hits_size = self._accumulate_hits(current_read_hits['hits'], all_hits)
        except KeyError:
            self.logger.error("Failed to load search hit into hit_stash with coded keys.")
            return "Failed to process search results due to a key coding error.  See logs.", 500

        self.logger.debug(f"Response to open scroll had {len(current_read_hits['hits'])}."
                          f" After processing, len(all_hits)={len(all_hits)}.")

        while hits_size >= SCROLL_HITS_PER_READ:
            try:
                # Continue reading a scroll using a read dictionary configured above
                rspn_read = requests.post(post_url, headers=headers, data=json.dumps(scroll_read_json_dict))
            except Exception as e:
                raise Exception(f"Scroll read failed due to {str(e)}")
            if rspn_read.status_code not in [200]:
                self.logger.error(f"Unexpected {rspn_read.status_code} response during scroll read. {rspn_read.raw}")
                raise requests.exceptions.HTTPError(response=rspn_read)
            else:
                json_read_rspn = rspn_read.json()
                if '_scroll_id' in json_read_rspn:
                    scroll_id = json_read_rspn['_scroll_id']
                    current_read_hits = json_read_rspn['hits']
                else:
                    self.logger.error(f"Missing '_scroll_id' during scroll read in JSON {json_read_rspn}")
                    raise requests.exceptions.HTTPError(response=rspn_read)
            # Update the scroll_id for further OpenSearch operations, as it may change with each operation.
            scroll_dump_json_dict['scroll_id'] = scroll_id
            scroll_read_json_dict['scroll_id'] = scroll_id

            try:
                #hits_size = self._accumulate_hits(current_read_hits['hits'], all_hits)
                hits_size = self._accumulate_hits(current_read_hits['hits'], all_hits)
            except KeyError:
                self.logger.error("Failed to load search hit into hit_stash with coded keys.")
                return "Failed to process search results due to a key coding error.  See logs.", 500

            self.logger.log(logging.DEBUG-1
                            ,f"Response to read scroll had {len(current_read_hits['hits'])}."
                             f" After processing, len(all_hits)={len(all_hits)}.")

        self.logger.debug(f"With len(all_hits)={len(all_hits)}, reading complete.")

        # close a scroll using a dump dictionary configured above
        rspn_dump = requests.post(post_url, headers=headers, data=json.dumps(scroll_dump_json_dict))
        if rspn_dump.status_code not in [200]:
            self.logger.error(f"Unexpected {rspn_dump.status_code} response during scroll close. {rspn_dump.raw}")
            raise requests.exceptions.HTTPError(response=rspn_dump)
        return all_hits

    def _get_index_refresh_operations(self, bearer_token=None):

        # Anticipating this method is a long-running process, use the internal, non-expiring token to
        # complete the following loop, despite the status of the token AWS Gateway checked to limit this
        # functionality to Data Admins.
        durable_token = self.auth_helper.getProcessSecret()

        all_file_mod_times = self._get_modification_time_of_dataset_files()
        mod_time_dict = self._get_mod_time_dict(all_file_mod_times)

        all_file_index_dates = self._read_all_index_results(bearer_token=durable_token)

        # Get a list of all the datasets which should be in the indices, so we can check for gaps.
        datasetList = self._get_indexable_datasets(bearer_token=durable_token)
        self.logger.info(f"Processing {len(datasetList)} Datasets for inclusion in Elasticsearch indices.")

        index_ops_dict = {'add':[], 'delete':[], 'reindex':[]}
        # Determine what is in the (consortium) index, but is not known to entity-api and Neo4j, so should be removed
        for dataset_uuid in all_file_index_dates:
            if dataset_uuid not in datasetList:
                self.logger.log(logging.DEBUG-1
                                ,f"{dataset_uuid} in OpenSearch but not Neo4j, remove")
                index_ops_dict['delete'].append(dataset_uuid)

        # Determine what is known to entity-api & Neo4j which should be in the (consortium) index which
        # should be added to the index or which needs to be re-indexed.
        for dataset_uuid in datasetList:
            if dataset_uuid not in all_file_index_dates:
                self.logger.log(logging.DEBUG-1
                                ,f"{dataset_uuid} in Neo4j but not OpenSearch, add")
                index_ops_dict['add'].append(dataset_uuid)
            else:
                # Determine what is known to entity-api & Neo4j, and to the (consortium) index, but has a
                # LAST_MODIFIED date in UUID which is after the date on the index entry. When one File of
                # a Dataset is stale or in one datastore but not the other, reindex the whole dataset.
                for file_uuid in all_file_index_dates[dataset_uuid].keys():
                    if file_uuid in all_file_index_dates[dataset_uuid] \
                        and file_uuid not in mod_time_dict[dataset_uuid]['dataset_files']:
                        self.logger.log(logging.DEBUG - 1
                                        , f"For dataset_uuid={dataset_uuid}, file_uuid={file_uuid},"
                                          f" found in OpenSearch but not Neo4j,"
                                          f" so reindex.")
                        index_ops_dict['reindex'].append(dataset_uuid)
                        break
                    if file_uuid in mod_time_dict[dataset_uuid]['dataset_files'] \
                        and file_uuid not in all_file_index_dates[dataset_uuid]:
                        self.logger.log(logging.DEBUG - 1
                                        , f"For dataset_uuid={dataset_uuid}, file_uuid={file_uuid},"
                                          f" found in Neo4j but not OpenSearch,"
                                          f" so reindex.")
                        index_ops_dict['reindex'].append(dataset_uuid)
                        break
                    if all_file_index_dates[dataset_uuid][file_uuid] < mod_time_dict[dataset_uuid]['dataset_files'][file_uuid]:
                        self.logger.log(logging.DEBUG-1
                                        ,f"For dataset_uuid={dataset_uuid}, file_uuid={file_uuid}, "
                                         f"modification time of {mod_time_dict[dataset_uuid]['dataset_files'][file_uuid]} "
                                         f"is before index time of {all_file_index_dates[dataset_uuid][file_uuid]}, "
                                         f"so reindex.")
                        index_ops_dict['reindex'].append(dataset_uuid)
                        # One file being updated indicates the whole dataset should be reindexed, so no
                        # need to process further.
                        break

        return index_ops_dict

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
    def get_dataset_file_infos(self, dataset_uuid, bearer_token=None):
        if not bearer_token:
            bearer_token = self.user_token

        # Get what uuid-api knows about the Dataset's files
        files_info = self._get_dataset_files_info(dataset_uuid, bearer_token=bearer_token)
        files_list = json.loads(files_info)

        # Get what entity-api knows about the Dataset's files and ancestors
        entity_prov_info = self._get_dataset_prov_info(dataset_uuid, bearer_token=bearer_token)
        # If the returned JSON contains 'error', it was logged by the _get_dataset_prov_info(), so
        # just return an empty dictionary.
        if 'error' in entity_prov_info:
            return Response(response=json.dumps({})
                            , mimetype="application/json")

        tissue_samples_dict_list = []
        organs_dict_list = []
        donors_dict_list = []
        for sample_uuid in entity_prov_info['dataset_samples'].keys():
            sample_dict = {}
            sample_category = entity_prov_info['dataset_samples'][sample_uuid]['sample_category']
            if sample_category != 'organ':
                sample_dict['uuid'] = sample_uuid
                sample_dict['code'] = sample_category
                sample_dict['type'] = sample_category
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

            organ_info = self._get_entity(entity_id=organ_uuid, bearer_token=bearer_token, entity_type_check='Sample')
            if not organ_info['sample_category'] or organ_info['sample_category'] != 'organ':
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
            if 'metadata' in organ_info['direct_ancestor'] and \
                'organ_donor_data' in organ_info['direct_ancestor']['metadata']:
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
            file_info['file_extension'] = file_info['rel_path'][file_info['rel_path'].rindex('.')+1:] if file_info['rel_path'].find('.') > -1 else ''
            file_info['samples'] = tissue_samples_dict_list
            file_info['organs'] = organs_dict_list
            file_info['donors'] = donors_dict_list
            file_info['dataset_uuid'] = dataset_uuid
            file_info['data_types'] = entity_prov_info['dataset_data_types']
            file_info.pop('path')
            file_info.pop('base_dir')
            # See admonition at https://docs.python.org/3/library/datetime.html#datetime.datetime.utcnow to
            # retrieve a non-naive UTC time.
            awareUTCTimeNow = datetime.now(timezone.utc)
            file_info['file_info_refresh_timestamp'] = awareUTCTimeNow.isoformat()
            dataset_file_info_list.append(file_info)

        results_json = json.dumps(dataset_file_info_list)

        return Response(response=results_json
                        , mimetype="application/json")
        # if len(results_json) < self.large_response_threshold:
        #     return Response(response=results_json
        #                     , mimetype="application/json")
        # else:
        #     return Response(self._stash_results_in_S3(object_content=results_json
        #                                               ,key_uuid=dataset_uuid)
        #                     , 303)

    # Use the entity-api service to get provenance info of a given Dataset identifier.
    # input: entity_id-ID (hubmap_id or uuid) of a Dataset entity.
    #        bearer_token-An optional token for querying the entity-api. The users token will be used for
    #                     querying when this argument is not specified.
    #        entity_type_check-An optional entity type.  If specified, and the ID in entity_id does not
    #                          match this type, an exception is raised.
    # output: JSON with info from Neo4j.
    # exceptions-Exception raised if response is not 200, or if the returned JSON does not contain an
    #            entity match the type specified in the optional argument entity_type_check.
    def get_entity(self, entity_id, bearer_token=None, entity_type_check=None):
        if not bearer_token:
            bearer_token = self.user_token
        theEntity = self._get_entity(entity_id=entity_id, bearer_token=bearer_token, entity_type_check=entity_type_check)
        return theEntity

    # Return indication whether the user token has admin privileges.
    def verify_user_is_data_admin(self):
        return self.auth_helper.has_data_admin_privs(self.user_token)

    def verify_user_in_write_group(self, aDataset):
        # Verify the user has write permission for the entity whose documents are to be cleared from the ES index
        entity_group_uuid = aDataset['group_uuid']
        return entity_group_uuid in self.user_groups_by_id_dict.keys()

    # Use the uuid-api service to find out the uuid of a given identifier, for
    # use with endpoints requiring a uuid as the identifier.
    # input: entity_id-ID (hubmap_id or uuid) of a Dataset entity.
    #        bearer_token-An optional token for querying the entity-api. The users token will be used for
    #                     querying when this argument is not specified.
    #        entity_type_check-An optional entity type.  If specified, and the ID in entity_id does not
    #                          match this type, an exception is raised.
    # output: the uuid of the entity
    # exceptions-Exception raised if response is not 200, or if the returned JSON does not contain an
    #            entity match the type specified in the optional argument entity_type_check.

    def get_identifier_info(self, entity_id, entity_type_check=None, bearer_token=None):
        if not bearer_token:
            bearer_token = self.user_token

        get_url = self.uuid_api_url + '/uuid/' + entity_id
        response = requests.get(get_url, headers = {'Authorization': 'Bearer ' + bearer_token}, verify = False)
        if response.status_code != 200:
            raise requests.exceptions.HTTPError(response=response)
        if entity_type_check:
            id_attributes = json.loads(response.text)
            if id_attributes['type'].lower() != entity_type_check.lower():
                raise Exception(f"Identifier {entity_id} type is {id_attributes['type']}, not {entity_type_check}.")
        return response.json()['uuid']

    # Get all the files for a Dataset, build a file info document for each, and add each file info document to
    # the Elasticsearch index.
    def index_dataset(self, aDataset, bearer_token=None):
        if not bearer_token:
            bearer_token = self.user_token

        # Any needed data admin privileges should have been checked before reaching this method.
        # Verify Dataset attributes are compatible with index inclusion, and which index should contain them.
        # N.B. does not test for "primary" Datasets, which may cause an exception, but which
        #      should also be allowed to be indexed in the near future.
        dataset_scope = self._get_dataset_scope(aDataset=aDataset)
        if dataset_scope == DatasetIndexScopeType.GENETIC:
            raise Exception(f"Dataset {aDataset['uuid']} with "
                            f"'contains_human_genetic_sequences'={aDataset['contains_human_genetic_sequences']}"
                            f" is not allowed in Elasticsearch indices.")
        elif dataset_scope == DatasetIndexScopeType.PUBLIC:
            target_indices = [self.files_api_public_index, self.files_api_nonpublic_index]
        elif dataset_scope == DatasetIndexScopeType.NONPUBLIC:
            target_indices = [self.files_api_nonpublic_index]
        else:
            self.logger.error(f"Unrecognized state for dataset_scope={dataset_scope} for aDataset['uuid']={aDataset['uuid']}.")
            raise Exception(f"Unable to determine appropriate index for aDataset['uuid']={aDataset['uuid']}. See logs.")
        self.logger.info(f"Target ES indexes for dataset {aDataset['uuid']} file info documents: {str(target_indices)}.")

        # Create a fresh file infos document for the specified Dataset from the dataset-file-info endpoint.
        # Connect to the database and retrieve the information for files
        # descended from the entity.
        dataset_files_info_response = self.get_dataset_file_infos(aDataset['uuid'], bearer_token=bearer_token)

        if not dataset_files_info_response or not dataset_files_info_response.get_json():
            # if this is a Dataset with no files, but the response is fine, do not log an error
            if dataset_files_info_response.status_code == 200:
                pass
            else:
                self.logger.error(f"Unable to retrieve the file set JSON to do indexing for aDataset['uuid']={aDataset['uuid']}")
                raise Exception(f"Unexpected JSON content getting file info documents for aDataset['uuid']={aDataset['uuid']}")
        files_info_list = dataset_files_info_response.get_json()

        # Try clearing the documents for the Dataset before inserting current documents, in case
        # Files were removed from the Dataset since initially put in the index.  But don't skip
        # inserting files if deletion is not successful.
        try:
            for target_index in target_indices:
                self._clear_dataset_file_info_docs(es_index_name=target_index, dataset_uuid=aDataset['uuid'], bearer_token=bearer_token)
        except Exception as e:
            self.logger.error(f"While clearing existing file info documents from {target_index} for"
                              f" {aDataset['uuid']}, encountered {e.text}. Continuing with insertion.")

        # Re-work the full dictionary of responses from each search-api /add operation into
        # something more compact from the files-api.
        self.logger.info(f"For Dataset '{aDataset['uuid']}'."
                         f" inserting {len(files_info_list)} file info documents"
                         f" into {str(target_indices)}."
                         )
        es_response_dict = {}
        for file_info_dict in files_info_list:
            file_response_dict = {}
            for target_index in target_indices:
                file_resp = self._write_or_update_doc(es_index_name=target_index,
                                                      es_doc_dict=file_info_dict,
                                                      bearer_token=bearer_token)
                file_response_dict[target_index] = file_resp.text
            es_response_dict[file_info_dict['file_uuid']] = file_response_dict

        return es_response_dict

    # Get all the Datasets, and loop through each one.  Add a file info document to
    # the Elasticsearch index for each File in an indexable Dataset.
    def index_all_datasets(self):
        inserted_datasets_list = []
        failed_datasets_list = []
        skipped_datasets_list = []

        try:
            # Anticipating this method is a long-running process, use the internal, non-expiring token to
            # complete the following loop, despite the status of the token AWS Gateway checked to limit this
            # functionality to Data Admins.
            durable_token = self.auth_helper.getProcessSecret()

            datasetList = self._get_all_nongenetic_datasets(bearer_token=durable_token)

            self.logger.info(f"Processing {len(datasetList)} Datasets for inclusion in Elasticsearch indices.")
            index_response_dict = {}

            for dataset_uuid in datasetList:
                try:
                    theDataset = self.get_entity(entity_id=dataset_uuid, bearer_token=durable_token, entity_type_check='DATASET')

                    index_response_dict[dataset_uuid] = self.index_dataset(aDataset=theDataset, bearer_token=durable_token)

                    inserted_datasets_list.append(dataset_uuid)
                    self.logger.info(f"Finished updating Elasticsearch indices for {len(index_response_dict[dataset_uuid])} file info documents"
                                     f" for Dataset '{dataset_uuid}'.")
                except Exception as eDataset:
                    if hasattr(eDataset,'response') and \
                       hasattr(eDataset.response, 'status_code') and \
                       eDataset.response.status_code == 400 and \
                       hasattr(eDataset.response, 'text') and \
                       re.search("Make sure this is a Primary Dataset", str(eDataset.response.text)):
                        skipped_datasets_list.append(dataset_uuid)
                        self.logger.warning(f"While updating Elasticsearch indices for all file info documents"
                                            f" for Dataset '{dataset_uuid}'"
                                            f", skipped the Dataset. Check if a Primary Dataset.")
                    else:
                        failed_datasets_list.append(dataset_uuid)
                        self.logger.error(f"While updating Elasticsearch indices for all file info documents"
                                          f" for Dataset '{dataset_uuid}'"
                                          f", got eDataset='{eDataset}'.")
                    # Continue this loop for other Datasets in datasetList
        except Exception as eWholeList:
            self.logger.error(f"While updating Elasticsearch indices with file info documents"
                              f" during 'reindex all'"
                              f", got e='{eWholeList}'.")
            raise Exception(f"An error was encountered while updating"
                            f" Elasticsearch indices with file info documents"
                            f" during 'reindex all'"
                            f" See logs.")
        self.logger.info(f"Inserted entries for {len(inserted_datasets_list)} Datasets into Elasticsearch indices.")
        self.logger.info(f"Skipped {len(skipped_datasets_list)} Datasets (due to unsupported characteristics).")
        self.logger.info(f"Failed to index {len(failed_datasets_list)} Datasets (due to logged errors).")
        allDatasetResultDict = {}
        allDatasetResultDict['failed'] = failed_datasets_list
        allDatasetResultDict['skipped'] = skipped_datasets_list
        allDatasetResultDict['succeeded'] = inserted_datasets_list
        return Response(json.dumps(allDatasetResultDict))

    def refresh_indices(self):
        # Anticipating this method is a long-running process, use the internal, non-expiring token to
        # complete the following loop, despite the status of the token AWS Gateway checked to limit this
        # functionality to Data Admins.
        durable_token = self.auth_helper.getProcessSecret()

        ops_dict = self._get_index_refresh_operations(bearer_token=durable_token)
        if ops_dict:
            self.logger.info(f"Identified {len(ops_dict['delete'])} Datasets to delete"
                             f" because they are in OpenSearch but not Neo4j.")
            self.logger.info(f"Identified {len(ops_dict['add'])} Datasets to add"
                             f" because they are in Neo4j but not OpenSearch.")
            self.logger.info(f"Identified {len(ops_dict['reindex'])} Datasets to reindex"
                             f" because the Files have been modified since the Dataset was indexed.")

        index_response_dict = {}
        for dataset_uuid in ops_dict['delete']:
            self.logger.error(f"Dataset {dataset_uuid} unexpectedly in Neo4j but not Opensearch. No endpoint"
                              f" supporting removal.")
            index_response_dict[dataset_uuid] = {'error': 'Unable to delete, see logs.'}
        for dataset_uuid in ops_dict['add']:
            try:
                theDataset = self.get_entity(entity_id=dataset_uuid, bearer_token=durable_token, entity_type_check='DATASET')
            except requests.HTTPError as he:
                self.logger.error(f"For Dataset {dataset_uuid}, unable to 'add' due to {he.response.text}")
                index_response_dict[dataset_uuid] = {'error' : 'Unable to add, see logs.'}
                continue
            index_response_dict[dataset_uuid] = self.index_dataset(aDataset=theDataset, bearer_token=durable_token)
        for dataset_uuid in ops_dict['reindex']:
            try:
                theDataset = self.get_entity(entity_id=dataset_uuid, bearer_token=durable_token, entity_type_check='DATASET')
            except requests.HTTPError as he:
                self.logger.error(f"For Dataset {dataset_uuid}, unable to 'reindex' due to {he.response.text}")
                index_response_dict[dataset_uuid] = {'error': 'Unable to reindex, see logs.'}
                continue
            index_response_dict[dataset_uuid] = self.index_dataset(aDataset=theDataset, bearer_token=durable_token)

        return Response(json.dumps(index_response_dict)), 200
