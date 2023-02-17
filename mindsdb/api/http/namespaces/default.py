import base64
from pathlib import Path
import secrets
import urllib
import time

import requests
from flask import request, session, redirect, url_for
from flask_restx import Resource
from flask_restx import fields

from mindsdb.api.http.namespaces.configs.default import ns_conf
from mindsdb.utilities.config import Config
from mindsdb.api.http.utils import http_error
from mindsdb.utilities.log import get_log


log = get_log('http')


def get_access_token():
    return Config().get('auth', {}).get('oauth', {}).get('tokens', {}).get('access_token')


def request_user_info(access_token: str = None):
    ''' request user info from cloud

        Returns:
            dict: user data
    '''
    if access_token is None:
        access_token = get_access_token()
    if access_token is None:
        raise KeyError()

    auth_server = Config()['auth']['oauth']['server_host']

    response = requests.get(
        f'https://{auth_server}/auth/userinfo',
        headers={
            'Authorization': f'Bearer {access_token}'
        }
    )
    if response.status_code != 200:
        raise Exception(f'Wrong response: {response.status_code}, {response.text}')

    return response.json()


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


@ns_conf.route('/auth/callback', methods=['GET'])
@ns_conf.route('/auth/callback/cloud_home', methods=['GET'])
class Auth(Resource):
    def get(self):
        config = Config()
        code = request.args.get('code')

        aws_meta_data = config['aws_meta_data']
        public_hostname = aws_meta_data['public-hostname']
        instance_id = aws_meta_data['instance-id']

        oauth_meta = config['auth']['oauth']
        client_id = oauth_meta['client_id']
        client_secret = oauth_meta['client_secret']
        auth_server = oauth_meta['server_host']
        client_basic = base64.b64encode(
            f'{client_id}:{client_secret}'.encode()
        ).decode()

        redirect_uri = f'https://{public_hostname}{request.path}'
        response = requests.post(
            f'https://{auth_server}/auth/token',
            data={
                'code': code,
                'grant_type': 'authorization_code',
                'redirect_uri': redirect_uri
            },
            headers={
                'Authorization': f'Basic {client_basic}'
            }
        )
        tokens = response.json()
        if 'expires_in' in tokens:
            tokens['expires_at'] = round(time.time() + tokens['expires_in'] - 1)
            del tokens['expires_in']

        user_data = request_user_info(tokens['access_token'])

        previous_username = config['auth']['oauth'].get('username')
        new_username = user_data['name']
        if (
            previous_username is not None
            and new_username != previous_username
        ):
            return redirect('/forbidden')

        config.update({
            'auth': {
                'provider': 'cloud',
                'oauth': {
                    'username': new_username,
                    'tokens': tokens
                }
            }
        })

        try:
            resp = requests.put(
                f'https://{auth_server}/cloud/instance',
                json={
                    'instance_id': instance_id,
                    'public_hostname': public_hostname,
                    'ami_id': aws_meta_data.get('ami-id')
                },
                headers={
                    'Authorization': f'Bearer {tokens["access_token"]}'
                },
                timeout=5
            )
            if resp.status_code != 200:
                log.warn(f'Wrong response from cloud server: {resp.status_code}')
        except Exception as e:
            log.warn(f"Cant't send request to cloud server: {e}")

        session['username'] = user_data['name']
        session['auth_provider'] = 'cloud'
        session.permanent = True

        if request.path.endswith('/auth/callback/cloud_home'):
            return redirect(f'https://{auth_server}')
        else:
            return redirect(url_for('root_index'))


@ns_conf.route('/auth/cloud_login', methods=['GET'])
class CloudLoginRoute(Resource):
    @ns_conf.doc(
        responses={
            302: 'Redirect to auth server'
        }
    )
    def get(self):
        ''' redirect to cloud login form
        '''
        location = request.args.get('location')
        config = Config()
        aws_meta_data = config['aws_meta_data']
        public_hostname = aws_meta_data['public-hostname']
        auth_server = config['auth']['oauth']['server_host']
        if location == 'cloud_home':
            redirect_uri = f'https://{public_hostname}/api/auth/callback/cloud_home'
        else:
            redirect_uri = f'https://{public_hostname}/api/auth/callback'
        args = urllib.parse.urlencode({
            'client_id': config['auth']['oauth']['client_id'],
            'scope': 'openid profile aws_marketplace',
            'response_type': 'code',
            'nonce': secrets.token_urlsafe(),
            'redirect_uri': redirect_uri
        })
        return redirect(f'https://{auth_server}/auth/authorize?{args}')


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
            'auth': fields.Nested(
                ns_conf.model('response_status_auth', {
                    'confirmed': fields.Boolean(description='is current user autentificated'),
                    'required': fields.Boolean(description='is autentificated required'),
                    'provider': fields.Boolean(description='current autentification provider: local of 3d-party')
                })
            )
        })
    )
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
            'environment': environment,
            'auth': {
                'confirmed': check_auth(),
                'http_auth_enabled': config['auth']['http_auth_enabled'],
                'provider': auth_provider
            }
        }

        if environment != 'cloud':
            marker_file = Path(Config().paths['root']).joinpath('gui_first_launch.txt')
            if marker_file.is_file() is False:
                resp['is_first_launch'] = True
                marker_file.write_text('')

        return resp
