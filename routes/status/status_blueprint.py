from flask import Blueprint, jsonify
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def construct_blueprint(fworker):
    status_blueprint = Blueprint('status', __name__)

    """
    Show status of the current VERSION and BUILD
    Returns
    -------
    json
        A json containing the status details
    """
    @status_blueprint.route('/status', methods=['GET'])
    def get_status():
        status_data = {'version': (Path(__file__).absolute().parent.parent.parent / 'VERSION').read_text().strip(),
                       'build': (Path(__file__).absolute().parent.parent.parent / 'BUILD').read_text().strip(),
                       'mysql_connection': fworker.testConnection()}

        return jsonify(status_data)
    return status_blueprint
