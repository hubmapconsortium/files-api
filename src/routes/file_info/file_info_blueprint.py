from flask import Blueprint, Response, request
import logging
import requests

from file_worker import FileWorker

logger = logging.getLogger(__name__)

def construct_blueprint(globusGroups, appConfig):
    file_info_blueprint = Blueprint('file_info', __name__)
    globus_groups = globusGroups
    app_config = appConfig

    """
    Get a JSON array containing information from the UUID database about each file attached to the entity
    Returns
    -------
    json
        A JSON array containing the file details
    """
    @file_info_blueprint.route('/entities/<entity_id>/files', methods=['GET'])
    def get_file_info(entity_id):
        try:
            fworker = FileWorker(globusGroups=globus_groups, appConfig=app_config, requestHeaders=request.headers)

            # Use the uuid-api webservice to check the identifier format and
            # extract the uuid for the entity.
            entity_uuid = fworker.get_identifier_info(entity_id)

            # Connect to the database and retrieve the information for files
            # descended from the entity.
            file_info_response = fworker.get_file_info(entity_uuid)
            return file_info_response
        except requests.exceptions.HTTPError as he:
            eMsg = he.response.text if he.response.text is not None else 'Undescribed HTTPError'
            eCode = he.response.status_code if he.response.status_code is not None else 500
            logger.error(str(eCode) + ':' + eMsg, exc_info=True)
            return Response(eMsg, eCode)
        except Exception as e:
            eMsg = f"{type(e).__name__}: {str(e)}"
            logger.error(eMsg, exc_info=True)
            return Response(f"Unexpected error: {eMsg}", 500)

    """
    Get JSON containing the file info document for each file in a dataset
    Returns
    -------
    json
        A json containing the file info document for each file in the dataset
    """
    @file_info_blueprint.route('/datasets/<dataset_id>/construct-file-documents', methods=['GET'])
    def get_dataset_file_infos(dataset_id):
        try:
            fworker = FileWorker(globusGroups=globus_groups, appConfig=app_config, requestHeaders=request.headers)

            # Verify the user is a Data Admin, who can view file document constructs outside Elasticsearch
            fworker.verify_op_permission(aDataset=None)

            # Use the uuid-api webservice to check the identifier format and
            # extract the uuid for the entity.
            dataset_uuid = fworker.get_identifier_info(dataset_id,'DATASET')

            # Connect to the database and retrieve the information for files
            # descended from the entity.
            dataset_files_info_response = fworker.get_dataset_file_infos(dataset_uuid)
            return dataset_files_info_response
        except requests.exceptions.HTTPError as he:
            eMsg = he.response.text if he.response.text is not None else 'Undescribed HTTPError'
            eCode = he.response.status_code if he.response.status_code is not None else 500
            logger.error(str(eCode) + ':' + eMsg, exc_info=True)
            return Response(eMsg, eCode)
        except Exception as e:
            logger.error(str(e), exc_info=True)
            return Response('Unexpected error: ' + str(e), 500)

    # end construct_blueprint()
    return file_info_blueprint
