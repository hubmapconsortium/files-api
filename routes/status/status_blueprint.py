from flask import Blueprint, jsonify, request
from pathlib import Path
import logging

from file_worker import FileWorker

logger = logging.getLogger(__name__)

def construct_blueprint(globusGroups, appConfig):
    status_blueprint = Blueprint('status', __name__)
    globus_groups = globusGroups
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
        fworker = FileWorker(globusGroups=globus_groups, appConfig=app_config, requestHeaders=request.headers)
        status_data = {'version': (Path(__file__).absolute().parent.parent.parent / 'VERSION').read_text().strip(),
                       'build': (Path(__file__).absolute().parent.parent.parent / 'BUILD').read_text().strip(),
                       'mysql_connection': fworker.testConnection()}

        return jsonify(status_data)
    return status_blueprint
