import time

from flask import request, session
from flask_restx import Resource
from flask_restx import fields

from mindsdb.__about__ import __version__ as mindsdb_version
from mindsdb.api.http.namespaces.configs.default import ns_conf
from mindsdb.api.http.utils import http_error
from mindsdb.metrics.metrics import api_endpoint_metrics
from mindsdb.utilities.config import Config
from mindsdb.utilities import log


logger = log.getLogger(__name__)


def check_auth() -> bool:
    ''' checking whether current user is authenticated

        Returns:
            bool: True if user authentication is approved
    '''
    config = Config()
    if config['auth']['http_auth_enabled'] is False:
        return True

    if config['auth'].get('provider') == 'cloud':
        if isinstance(session.get('username'), str) is False:
            return False

        if config['auth']['oauth']['tokens']['expires_at'] < time.time():
            return False

        return True

    return session.get('username') == config['auth']['username']


@ns_conf.route('/login', methods=['POST'])
class LoginRoute(Resource):
    @ns_conf.doc(
        responses={
            200: 'Success',
            400: 'Error in username or password',
            401: 'Invalid username or password'
        },
        body=ns_conf.model('request_login', {
            'username': fields.String(description='Username'),
            'password': fields.String(description='Password')
        })
    )
    @api_endpoint_metrics('POST', '/default/login')
    def post(self):
        ''' Check user's credentials and creates a session
        '''
        username = request.json.get('username')
        password = request.json.get('password')
        if (
            isinstance(username, str) is False or len(username) == 0
            or isinstance(password, str) is False or len(password) == 0
        ):
            return http_error(
                400, 'Error in username or password',
                'Username and password should be string'
            )

        config = Config()
        inline_username = config['auth']['username']
        inline_password = config['auth']['password']

        if (
            username != inline_username
            or password != inline_password
        ):
            return http_error(
                401, 'Forbidden',
                'Invalid username or password'
            )

        session.clear()
        session['username'] = username
        session.permanent = True

        return '', 200


@ns_conf.route('/logout', methods=['POST'])
class LogoutRoute(Resource):
    @ns_conf.doc(
        responses={
            200: 'Success'
        }
    )
    @api_endpoint_metrics('POST', '/default/logout')
    def post(self):
        session.clear()
        return '', 200


@ns_conf.route('/status')
class StatusRoute(Resource):
    @ns_conf.doc(
        responses={
            200: 'Success'
        },
        model=ns_conf.model('response_status', {
            'environment': fields.String(description='The name of current environment: cloud, local or other'),
            'mindsdb_version': fields.String(description='Current version of mindsdb'),
            'auth': fields.Nested(
                ns_conf.model('response_status_auth', {
                    'confirmed': fields.Boolean(description='is current user authenticated'),
                    'required': fields.Boolean(description='is authenticated required'),
                    'provider': fields.Boolean(description='current authenticated provider: local of 3d-party')
                })
            )
        })
    )
    @api_endpoint_metrics('GET', '/default/status')
    def get(self):
        ''' returns auth and environment data
        '''
        environment = 'local'
        config = Config()

        environment = config.get('environment')
        if environment is None:
            if config.get('cloud', False):
                environment = 'cloud'
            elif config.get('aws_marketplace', False):
                environment = 'aws_marketplace'
            else:
                environment = 'local'

        auth_provider = 'disabled'
        if config['auth']['http_auth_enabled'] is True:
            if config['auth'].get('provider') is not None:
                auth_provider = config['auth'].get('provider')
            else:
                auth_provider = 'local'

        resp = {
            'mindsdb_version': mindsdb_version,
            'environment': environment,
            'auth': {
                'confirmed': check_auth(),
                'http_auth_enabled': config['auth']['http_auth_enabled'],
                'provider': auth_provider
            }
        }

        return resp
