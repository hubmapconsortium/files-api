from flask import Flask, jsonify
import os
import requests
# Don't confuse urllib (Python native library) with urllib3 (3rd-party library, requests also uses urllib3)
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from pathlib import Path
import logging
import json
import time

# Atlas Consortia commons
from atlas_consortia_commons.ubkg import initialize_ubkg
from atlas_consortia_commons.rest import *
from atlas_consortia_commons.ubkg.ubkg_sdk import init_ontology
from lib.ontology import Ontology

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
## UBKG Ontology and REST initialization
####################################################################################################

try:
    for exception in get_http_exceptions_classes():
        app.register_error_handler(exception, abort_err_handler)

    if app.config.__contains__('UBKG_SERVER') and app.config.__contains__(
            'UBKG_ENDPOINT_VALUESET') and app.config.__contains__('UBKG_CODES'):
        app.ubkg = initialize_ubkg(app.config)
        with app.app_context():
            init_ontology()
    else:
        raise Exception(
            "UBKG configuration parameter(s) missing. Please check that 'UBKG_SERVER', 'UBKG_ENDPOINT_VALUESET', and 'UBKG_CODES' have been set.")

    logger.info("Initialized ubkg module successfully :)")
# Use a broad catch-all here
except Exception:
    msg = "Failed to initialize the ubkg module"
    # Log the full stack trace, prepend a line with our message
    logger.exception(msg)

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
