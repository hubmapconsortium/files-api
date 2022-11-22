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
    @file_info_index_blueprint.route('/datasets/<dataset_id>/reindex', methods=['PUT'])
    def create_update_dataset_file_infos(dataset_id):
        try:
            fworker = FileWorker(globusGroups=globus_groups, appConfig=app_config, requestHeaders=request.headers)

            # Specify argument to confirm dataset_id is for a Dataset entity.
            theDataset = fworker.get_entity(entity_id=dataset_id, entity_type_check='DATASET')

            fworker.verify_op_permission(aDataset=theDataset)

            dataset_uuid = theDataset['uuid']

            # Confirm the Elastic Search index exists? Or leave that to search-api?
            # https://www.elastic.co/guide/en/elasticsearch/reference/master/indices-exists.html
            # Need to add "HEAD id" to search-api, if not calling ES directly.

            asynchronous = request.args.get('async')

            index_response_dict = {}

            if asynchronous:
                index_response_dict[dataset_uuid] = fworker.index_dataset(aDatasetUUID=dataset_uuid)

                # Consolidate the successful responses together into a subdictionary in the JSON response.
                dataset_files_info_response_dict = {'dataset_uuid': dataset_uuid,
                                                    'file_count': len(index_response_dict[dataset_uuid]),
                                                    'index_responses': {}}
                accepted_file_uuid_list = []
                for file_uuid in index_response_dict[dataset_uuid].keys():
                    if re.match(index_response_dict[dataset_uuid][file_uuid], f"Request of adding {file_uuid} accepted"):
                        accepted_file_uuid_list.append(file_uuid)
                    else:
                        dataset_files_info_response_dict['index_responses'][file_uuid] = index_response_dict[dataset_uuid][file_uuid]
                dataset_files_info_response_dict['index_responses']["Request of adding <file_uuid> accepted"] = accepted_file_uuid_list

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

        except requests.exceptions.HTTPError as he:
            eMsg = he.response.text if he.response.text is not None else 'Undescribed HTTPError'
            eCode = he.response.status_code if he.response.status_code is not None else 500
            logger.error(str(eCode) + ':' + eMsg, exc_info=True)
            return Response(eMsg, eCode)
        except Exception as e:
            logger.error(str(e), exc_info=True)
            return Response('Unexpected error: ' + str(e), 500)
        return Response(json.dumps(dataset_files_info_response_dict))

    """
    Index all Datasets.
    Returns
    -------
    json
        A json containing details of the dataset indexing operation
    """
    @file_info_index_blueprint.route('/datasets/reindex-all', methods=['PUT'])
    def reindex_dataset_files_docs():
        fworker = FileWorker(globusGroups=globus_groups, appConfig=app_config, requestHeaders=request.headers)

        # Verify the user is a Data Admin, who can index all datasets
        fworker.verify_op_permission(aDataset=None)

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
