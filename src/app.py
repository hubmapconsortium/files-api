from flask import Flask, jsonify
import os
import requests
# Don't confuse urllib (Python native library) with urllib3 (3rd-party library, requests also uses urllib3)
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from pathlib import Path
import logging
import json
import time

from routes.status import status_blueprint
from routes.file_info import file_info_blueprint
from routes.file_info_index import file_info_index_blueprint

# Specify the absolute path of the instance folder and use the config file relative to the instance path
app = Flask(__name__, instance_path=os.path.join(os.path.abspath(os.path.dirname(__file__)), 'instance'), instance_relative_config=True)
# Use configuration from instance/app.cfg, deployed per-app from examples in the repository.
try:
    app.config.from_pyfile('app.cfg')
except Exception as e:
    raise Exception("Failed to get configuration from instance/app.cfg")

# All the API logging is forwarded to the uWSGI server and gets written into the log file `uwsgi-files-api.log`
# Log rotation is handled via logrotate on the host system with a configuration file
# Do NOT handle log file and rotation via the Python logging to avoid issues with multi-worker processes
# Configure logging formats and level
LOG_FILE_NAME = f"{Path(__file__).absolute().parent.parent}/log/files-api-{time.strftime('%m-%d-%Y-%H-%M-%S')}.log"
log_file_handler = logging.FileHandler(filename=LOG_FILE_NAME)
logging_formatter = logging.Formatter(  fmt='[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
                                        ,datefmt='%Y-%m-%d %H:%M:%S')
log_file_handler.setFormatter(fmt=logging_formatter)
# Root logger configuration
# Use `getLogger()` instead of `getLogger(__name__)` to apply the config to the root logger
# will be inherited by the sub-module loggers
try:
    logger = logging.getLogger(name='files-api')
    logger.addHandler(hdlr=log_file_handler)
    logger.setLevel(level=logging.DEBUG)
    logger.info("logger {logger.name} set up for file {LOG_FILE_NAME}.")
except Exception as e:
    print("Error setting up global log file.")
    print(str(e))
try:
    logger.info("started")
except Exception as e:
    print("Error opening log file during startup")
    print(str(e))

# Register Blueprints
app.register_blueprint(status_blueprint.construct_blueprint(app_config=app.config))
app.register_blueprint(file_info_blueprint.construct_blueprint(app_config=app.config))
app.register_blueprint(file_info_index_blueprint.construct_blueprint(app_config=app.config))

# Remove trailing slash / from URL base to avoid "//" caused by config with trailing slash
app.config['UUID_API_URL'] = app.config['UUID_API_URL'].strip('/')
app.config['ENTITY_API_URL'] = app.config['ENTITY_API_URL'].strip('/')

# Suppress InsecureRequestWarning warning when requesting status on https with ssl cert verify disabled
requests.packages.urllib3.disable_warnings(category = InsecureRequestWarning)

####################################################################################################
## Register error handlers
####################################################################################################

# Error handler for 400 Bad Request with custom error message
@app.errorhandler(400)
def http_bad_request(e):
    return jsonify(error = str(e)), 400


# Error handler for 401 Unauthorized with custom error message
@app.errorhandler(401)
def http_unauthorized(e):
    return jsonify(error = str(e)), 401


# Error handler for 403 Forbidden with custom error message
@app.errorhandler(403)
def http_forbidden(e):
    return jsonify(error = str(e)), 403


# Error handler for 404 Not Found with custom error message
@app.errorhandler(404)
def http_not_found(e):
    return jsonify(error = str(e)), 404


# Error handler for 500 Internal Server Error with custom error message
@app.errorhandler(500)
def http_internal_server_error(e):
    return jsonify(error = str(e)), 500

####################################################################################################
## API Endpoints
####################################################################################################

# The only endpoint that should be in this file, all others should be route Blueprints...
@app.route('/', methods=['GET'])
def index():
    return "Hello! This is the File API service :)"

####################################################################################################
## For local development/testing
####################################################################################################

if __name__ == "__main__":
    try:
        app.run(host='0.0.0.0', port="5003")
    except Exception as e:
        print("Error during starting debug server.")
        print(str(e))
        logger.error(e, exc_info=True)
        print("Error during startup check the log file for further information")
