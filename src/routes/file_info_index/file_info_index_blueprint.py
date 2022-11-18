from flask import Blueprint, Response, request
import json

import logging
import requests
import re
import threading

from file_worker import FileWorker

logger = logging.getLogger(__name__)

def construct_blueprint(globusGroups, appConfig):
    file_info_index_blueprint = Blueprint('file_info_index', __name__)
    globus_groups = globusGroups
    app_config = appConfig

    """
    Index file info documents for the files in a dataset
    Returns
    -------
    json
        A json containing details of the dataset indexing operation
    """
    @file_info_index_blueprint.route('/<dataset_id>/dataset-files-index', methods=['PUT'])
    @file_info_index_blueprint.route('/datasets/<dataset_id>/reindex', methods=['PUT'])
    def create_update_dataset_file_infos(dataset_id):
        try:
            fworker = FileWorker(globusGroups=globus_groups, appConfig=app_config, requestHeaders=request.headers)

            # Specify argument to confirm dataset_id is for a Dataset entity.
            theDataset = fworker.get_entity(entity_id=dataset_id, entity_type_check='DATASET')

            fworker.verify_indexable(aDataset=theDataset)

            dataset_uuid = theDataset['uuid']

            # Confirm the Elastic Search index exists? Or leave that to search-api?
            # https://www.elastic.co/guide/en/elasticsearch/reference/master/indices-exists.html
            # Need to add "HEAD id" to search-api, if not calling ES directly.

            asynchronous = request.args.get('async')

            index_response_dict = {}

            if asynchronous:
                index_response_dict[dataset_uuid] = fworker.index_dataset(aDatasetUUID=dataset_uuid)

                # Consolidate the successful responses together into a subdictionary in the JSON response.
                dataset_files_info_response_dict = {'dataset_uuid': dataset_uuid, 'file_count': len(index_response_dict),
                                                    'index_responses': {}}
                accepted_file_uuid_list = []
                for file_uuid in index_response_dict[dataset_uuid].keys():
                    if re.match(index_response_dict[dataset_uuid][file_uuid], f"Request of adding {file_uuid} accepted"):
                        accepted_file_uuid_list.append(file_uuid)
                    else:
                        dataset_files_info_response_dict['index_responses'][file_uuid] = index_response_dict[dataset_uuid][file_uuid]
                dataset_files_info_response_dict['index_responses']["Request of adding <file_uuid> accepted"] = accepted_file_uuid_list

                # # Create a fresh file manifest document for the specified Dataset from the dataset-file-info endpoint. @TODO-if the "reindex" option is specified but the file-info cannot be created, should this return normal, empty response, and effectively go on to delete from index?  Or should it halt before removing from the index, and tell them to use a DELETE endpoint if they want to delete? Can index contain an empty doc for a Dataset with no files yet?
                # # @TODO-determine trade-off of holding a static file_info_blueprint vs calling endpoint.
                # # Connect to the database and retrieve the information for files
                # # descended from the entity.
                # dataset_files_info_response = fworker.get_dataset_file_infos(dataset_uuid)
                # files_info_list = dataset_files_info_response.get_json()
                # if not files_info_list:
                #     # @TODO-verify with [] response. Decide if Exception should actually be raised.
                #     raise Exception(f"Unexpected JSON content for dataset_uuid={dataset_uuid}")
                #
                # dataset_files_info_response_dict = {'dataset_uuid': dataset_uuid, 'file_count': len(files_info_list),
                #                                     'index_responses': {}}
                # #@TODO - determine if we should respond here with a "we're doing your queueing msg instead of building a response below.  Maybe a threshold test for file count..."
                # # Re-work the full dictionary of responses from each search-api /add operation into
                # # something more compact from the files-api.
                # index_response_dict = {}
                # index_response_dict["Request of adding <file_uuid> accepted"] = []
                # for file_info_dict in files_info_list:
                #     file_resp = fworker.write_or_update_doc(file_info_dict)
                #     if re.match(file_resp.text, f"Request of adding {file_info_dict['file_uuid']} accepted"):
                #         index_response_dict["Request of adding <file_uuid> accepted"].append(file_info_dict['file_uuid'])
                #     else:
                #         index_response_dict[file_info_dict['file_uuid']] = file_resp.text
                # dataset_files_info_response_dict['index_responses'] = index_response_dict

                return Response(json.dumps(dataset_files_info_response_dict)), 200
            else:
                try:
                    threading.Thread(target=fworker.index_dataset, args=[dataset_uuid]).start()
                    # discard the index_response_dict, as it will be logged at the INFO level by the FileWorker

                    logger.info(f"Started to clear documents for uuid '{dataset_uuid}' from ES index '{app_config['FILES_API_INDEX_NAME']}'.")
                except Exception as e:
                    logger.exception(e)
                    raise Exception(e)

                return f"Request to load file info documents for uuid '{dataset_uuid}' into ES index '{app_config['FILES_API_INDEX_NAME']}' accepted", 202

            # Determine if the Elastic Search index contains a document for the Dataset
            # GET /hm_dev_files/_doc/_count with payload
            # {"query": {"match": {
            #     "_id": "ffff2bda142ff4916b41cce8b0a34dfa"
            # }}}

            # Respond with a failure if a document is in the index, but the reindex option was not specified. Or
            # do we want to just overwrite (new version), or delete and replace?

            # If we get here, delete the document from the index if it exists.
            # From search-api index_base.py index_public_collection()
            # # Delete old doc for reindex
            # if reindex:
            #     self.eswriter.delete_document(public_index, uuid)
            #     self.eswriter.delete_document(private_index, uuid)


            # Insert the fresh dataset-file-info document for the specified Dataset
            # From search-api index_base.py index_public_collection()
            # self.eswriter.write_or_update_document(index_name=public_index, doc=json_data, uuid=uuid)
            # self.eswriter.write_or_update_document(index_name=private_index, doc=json_data, uuid=uuid)

        except requests.exceptions.HTTPError as he:
            eMsg = he.response.text if he.response.text is not None else 'Undescribed HTTPError'
            eCode = he.response.status_code if he.response.status_code is not None else 500
            logger.error(str(eCode) + ':' + eMsg, exc_info=True)
            return Response(eMsg, eCode)
        except Exception as e:
            logger.error(str(e), exc_info=True)
            return Response('Unexpected error: ' + str(e), 500)
        return Response(json.dumps(dataset_files_info_response_dict))

        # # Clear multiple documents from the specified index, in an app-appropriate way expressed in
        # # a Translator delete_docs() method
        # # Either delete just the documents for the Dataset with the specified UUID, or delete
        # # all the documents in the specified index
        #
        # if uuid:
        #     msgAck = f"Request to clear documents for uuid '{uuid}' from ES index '{index}' accepted"
        #     # Data Admin group not required to clear all documents for a Dataset from the ES index, but
        #     # will check write permission for the entity below.
        #     token = self.get_user_token(request.headers, admin_access_required=False)
        # else:
        #     msgAck = f"Request to clear all documents from ES index '{index}' accepted"
        #     # The token needs to belong to the Data Admin group to be able to clear all documents in the ES index.
        #     token = self.get_user_token(request.headers, admin_access_required=True)
        #
        # if request.is_json:
        #     bad_request_error(f"An unexpected JSON body was attached to the request to clear documents from ES index '{index}'.")
        #
        # # Check if query parameter is passed to used futures instead of threading
        # asynchronous = request.args.get('async')
        #
        # translator = self.init_translator(token)
        #
        #
        # if asynchronous:
        #     try:
        #         with concurrent.futures.\
        #                 ThreadPoolExecutor() as executor:
        #             future = executor.submit(translator.delete_docs, index, uuid)
        #             result = future.result()
        #     except Exception as e:
        #         logger.exception(e)
        #         internal_server_error(e)
        #
        #     return result, 202
        #
        # else:
        #     try:
        #         threading.Thread(target=translator.delete_docs, args=[index, uuid]).start()
        #
        #         logger.info(f"Started to clear documents for uuid '{uuid}' from ES index '{index}'.")
        #     except Exception as e:
        #         logger.exception(e)
        #         internal_server_error(e)
        #
        #     return msgAck, 202

    """
    Index all Datasets.
    Returns
    -------
    json
        A json containing details of the dataset indexing operation
    """
    @file_info_index_blueprint.route('/dataset-files-index', methods=['PUT'])
    @file_info_index_blueprint.route('/datasets/reindex_all', methods=['PUT'])
    def reindex_dataset_files_docs():
        inserted_datasets_list = []
        failed_datasets_list = []
        skipped_datasets_list = []

        fworker = FileWorker(globusGroups=globus_groups, appConfig=app_config, requestHeaders=request.headers)

        # Verify the user can index all datasets
        fworker.verify_indexable(aDataset=None)

        # Confirm the Elastic Search index exists? Or leave that to search-api?
        # https://www.elastic.co/guide/en/elasticsearch/reference/master/indices-exists.html
        # Need to add "HEAD id" to search-api, if not calling ES directly.

        asynchronous = request.args.get('async')

        if asynchronous:
            return fworker.index_all_datasets()
        else:
            try:
                threading.Thread(target=fworker.index_all_datasets, args=[]).start()
                # discard the index_response_dict, as it will be logged at the INFO level by the FileWorker

                logger.info(
                    f"Started to index file info documents for all Datasets to ES index '{app_config['FILES_API_INDEX_NAME']}'.")
            except Exception as e:
                logger.exception(e)
                raise Exception(e)

        return f"Request to index file info documents for all Datasets into ES index '{app_config['FILES_API_INDEX_NAME']}' accepted", 202

# end construct_blueprint()
    return file_info_index_blueprint
