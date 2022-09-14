from flask import Blueprint, Response
import logging
import requests

logger = logging.getLogger(__name__)

def construct_blueprint(fworker):
    file_IDs_blueprint = Blueprint('file_IDs', __name__)

    """
    Show status of the current VERSION and BUILD
    Returns
    -------
    json
        A json containing the status details
    """
    @file_IDs_blueprint.route('/<entity_id>/files', methods=['GET'])
    def get_file_info(entity_id):
        try:
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
            logger.error(str(e), exc_info=True)
            return Response('Unexpected error: ' + str(e), 500)
    return file_IDs_blueprint
