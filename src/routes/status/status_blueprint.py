from flask import Blueprint, jsonify, request
from pathlib import Path
import logging
from requests.exceptions import HTTPError

from file_worker import FileWorker

logger = logging.getLogger(__name__)

def construct_blueprint(appConfig):
    status_blueprint = Blueprint('status', __name__)
    app_config = appConfig

    """
    Show status of the current VERSION and BUILD
    Returns
    -------
    json
        A json containing the status details
    """
    @status_blueprint.route('/status', methods=['GET'])
    def get_status():
        fworker = FileWorker(appConfig=app_config, requestHeaders=request.headers)
        search_api_status_dict = {
            'elasticsearch_connection': 'Error on search-api retrieval. See logs.'
            ,'elasticsearch_status': 'Error on search-api retrieval. See logs.'
        }
        try:
            search_api_status_dict = fworker.testOpenSearchConnection()
        except HTTPError as he:
            # Expect exception logged by method raising, and use default dictionary prepared before call
            pass

        status_data = {'version': (Path(__file__).absolute().parent.parent.parent.parent / 'VERSION').read_text().strip()
                       ,'build': (Path(__file__).absolute().parent.parent.parent.parent / 'BUILD').read_text().strip()
                       ,'mysql_connection': fworker.testMySQLConnection()
                       ,'elasticsearch_connection': search_api_status_dict['elasticsearch_connection']
                       ,'elasticsearch_status': search_api_status_dict['elasticsearch_status']}

        return jsonify(status_data)
    return status_blueprint
