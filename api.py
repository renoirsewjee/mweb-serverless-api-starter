import os
import json
import pendulum
import time
import requests
import constants
import utils
from dynamodb import Repository
from flask import Flask, jsonify, request
from flask_cors import CORS
from typing import List, Dict, Any
from dataclasses import dataclass, field

# Create the logger for this module
logger = utils.create_logger(__name__)

# serverless-wsgi-plugin will set this environment if running locally
IS_OFFLINE = os.environ.get('IS_OFFLINE')

# Create the repository that will store the notices and notice types
SERVICE_TABLE_NAME = os.environ.get('SERVICE_TABLE_NAME')
assert SERVICE_TABLE_NAME is not None, 'SERVICE_TABLE_NAME environment variable must be set'

repository = Repository(is_offline = IS_OFFLINE is not None,
                        service_table_name=SERVICE_TABLE_NAME)


class RequestContext:
    def __init__(self, request):
        self.empty_dict = {}
        self.request = request
        self.requestContext = request.environ.get('event', self.empty_dict).get('requestContext', self.empty_dict)

    @property
    def source_ip_address(self) -> str:
        x_forwarded_for = self.request.headers.get('X-Forwarded-For')
        if x_forwarded_for:
            logger.info('getting ip address from x-forwarded-for header')
            tokens = x_forwarded_for.split(',')
            return tokens[0].strip()
        else:
            logger.info('getting ip address from sourceIP in request context')
            return self.requestContext.get('identity', self.empty_dict).get('sourceIp')

    @property
    def user_name(self) -> str:
        return self.requestContext.get('authorizer', self.empty_dict).get('sub')

    @property
    def account_number(self) -> str:
        return self.requestContext.get('authorizer', self.empty_dict).get('accountNumber')

    @property
    def roles(self) -> List[str]:
        roles_json = self.requestContext.get('authorizer', self.empty_dict).get('roles')
        if roles_json:
            return json.loads(roles_json)
        else:
            return []

    @property
    def agent(self) -> str:
        return self.requestContext.get('authorizer', self.empty_dict).get('agent')


@dataclass
class APIResponse:
    status: str
    result: Dict[str, Any] = field(default_factory=dict)
    messages: List[str] = field(default_factory=list)


def to_web(api_response, http_status_code=200):
    data = {constants.API_STATUS_KEY: api_response.status, 
            constants.API_RESULT_KEY: api_response.result, 
            constants.API_MESSAGES_KEY: api_response.messages}
    return jsonify(data), http_status_code


# Create a Flask App
app = Flask(__name__)
app.json_encoder = utils.DecimalEncoder 
CORS(app)

#
# Helper to get callers IP address
#
@app.route('/starter/myip')
def get_source_ip():
    logger.info(f'getting source ip address')

    # Get the lambda event to access the source ip address
    ip_address = RequestContext(request).source_ip_address
    logger.info(f'source ip address = {ip_address}')

    return to_web(APIResponse(status=constants.API_STATUS_OK, result={constants.IP_ADDRESS_KEY: ip_address}))

#
# User & IP whitelists
#
@app.route('/starter/sample')
def get_sample_data():
    logger.info(f'getting sample data')

    sample_data = repository.read_sample_data()
    return to_web(APIResponse(status=constants.API_STATUS_OK, result={constants.SAMPLE_DATA_KEY: sample_data}))


@app.route('/starter/sample', methods=['POST'])
def save_sample_data():
    logger.info(f'saving sample data = {json.dumps(request.json)}')

    successful = repository.save_sample_data(request.json)
    if not successful:
        logger.info('unsuccessful saving sample data')
        return to_web(APIResponse(status=constants.API_STATUS_FAIL), 500)
    else:
        return to_web(APIResponse(status=constants.API_STATUS_OK))
