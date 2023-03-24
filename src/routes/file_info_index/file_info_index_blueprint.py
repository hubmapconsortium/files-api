from flask import Blueprint, Response, request, jsonify
import json

import logging
import requests
import re
import threading

from file_worker import FileWorker

logger = logging.getLogger(__name__)

def construct_blueprint(app_config):
    file_info_index_blueprint = Blueprint('file_info_index', __name__)
    app_config = app_config

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
            fworker = FileWorker(app_config=app_config, request_headers=request.headers)

            # Specify argument to confirm dataset_id is for a Dataset entity.
            theDataset = fworker.get_entity(entity_id=dataset_id, entity_type_check=['DATASET','PUBLICATION'])

            if not fworker.verify_user_in_write_group(aDataset=theDataset) and not fworker.verify_user_is_data_admin():
                raise Exception(f"Permission denied for modifying ES index entries for '{theDataset['uuid']}'.")

            dataset_uuid = theDataset['uuid']

            # Confirm the Elastic Search index exists? Or leave that to search-api?
            # https://www.elastic.co/guide/en/elasticsearch/reference/master/indices-exists.html
            # Need to add "HEAD id" to search-api, if not calling ES directly.

            asynchronous = request.args.get('async')

            index_response_dict = {}

            if asynchronous:
                index_response_dict[dataset_uuid] = fworker.index_dataset(aDataset=theDataset)

                # Consolidate the successful responses together into a subdictionary in the JSON response.
                dataset_files_info_response_dict = {'dataset_uuid': dataset_uuid,
                                                    'file_count': len(index_response_dict[dataset_uuid]),
                                                    'index_responses': {}}
                for file_uuid in index_response_dict[dataset_uuid].keys():
                    if not file_uuid in dataset_files_info_response_dict['index_responses']:
                        dataset_files_info_response_dict['index_responses'][file_uuid] = []
                    for es_index_name in index_response_dict[dataset_uuid][file_uuid].keys():
                        dataset_files_info_response_dict['index_responses'][file_uuid].append(index_response_dict[dataset_uuid][file_uuid][es_index_name])

                return Response(json.dumps(dataset_files_info_response_dict)), 200
            else:
                try:
                    threading.Thread(target=fworker.index_dataset, args=[theDataset]).start()
                    # discard the index_response_dict, as it will be logged at the INFO level by the FileWorker

                    logger.info(f"Started to clear documents for uuid '{dataset_uuid}' from the Elasticsearch index.")
                except Exception as e:
                    logger.exception(e)
                    raise Exception(e)

                return f"Request to load file info documents for uuid '{dataset_uuid}' into the Elasticsearch index accepted", 202

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
        fworker = FileWorker(app_config=app_config, request_headers=request.headers)

        # Verify the user is a Data Admin, who can index all datasets
        if not fworker.verify_user_is_data_admin():
            raise Exception("Permission denied for requested operation.")

        asynchronous = request.args.get('async')

        if asynchronous:
            return fworker.index_all_datasets()
        else:
            try:
                threading.Thread(target=fworker.index_all_datasets, args=[]).start()
                # discard the index_response_dict, as it will be logged at the INFO level by the FileWorker

                logger.info("Started to index file info documents for all Datasets to the public and private Elasticsearch indices.")
            except Exception as e:
                logger.exception(e)
                raise Exception(e)

        return "Request to index file info documents for all Datasets into the public and private Elasticsearch indices accepted", 202

    @file_info_index_blueprint.route('/datasets/refresh-indices', methods=['PUT'])
    def refresh_indices():
        fworker = FileWorker(app_config=app_config, request_headers=request.headers)

        # Verify the user is a Data Admin, who can index all datasets
        if not fworker.verify_user_is_data_admin():
            raise Exception("Permission denied for requested operation.")

        asynchronous = request.args.get('async')

        if asynchronous:
            return fworker.refresh_indices()
        else:
            try:
                threading.Thread(target=fworker.refresh_indices, args=[]).start()
                # discard the index_response_dict, as it will be logged at the INFO level by the FileWorker
                logger.info(f"Started to refresh the public and private Elasticsearch indices with"
                            f" file info documents for added, changed, or deleted Datasets.")
            except requests.HTTPError as he:
                # OpenSearch errors come back in the JSON, so return them that way.
                return jsonify(he.response.json()), he.response.status_code
            except Exception as e:
                logger.exception(e)
                raise Exception(e)

        return  (   f"Request to refresh the public and private OpenSearch indices with file info documents for"
                    f" added, changed, or deleted Datasets accepted"), 202

    # end construct_blueprint()
    return file_info_index_blueprint
