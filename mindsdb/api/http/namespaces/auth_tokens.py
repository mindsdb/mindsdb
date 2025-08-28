import time
from typing import Dict, Any

from flask import request, session
from flask_restx import Resource, fields

from mindsdb.api.http.namespaces.configs.auth_tokens import ns_conf
from mindsdb.api.http.namespaces.default import check_auth
from mindsdb.api.http.utils import http_error
from mindsdb.metrics.metrics import api_endpoint_metrics
from mindsdb.utilities.config import Config
from mindsdb.utilities import log
from mindsdb.utilities.a2a_auth import (
    generate_auth_token,
    store_auth_token,
    is_auth_token_valid,
    get_auth_required_status,
    cleanup_expired_tokens,
    get_all_auth_tokens,
    revoke_auth_token
)

logger = log.getLogger(__name__)


@ns_conf.route('/token', methods=['POST'])
class AuthTokenRoute(Resource):
    @ns_conf.doc(
        responses={
            200: 'Success',
            401: 'Unauthorized - HTTP authentication required',
            403: 'Forbidden - Invalid credentials'
        },
        body=ns_conf.model('request_a2a_token', {
            'description': fields.String(description='Optional description for the token', required=False)
        })
    )
    @api_endpoint_metrics('POST', '/auth_tokens/token')
    def post(self):
        ''' Generate an A2A token for authenticated users
        
        This endpoint generates a token that can be used to authenticate with the A2A server.
        The token is only generated if the user is authenticated to the HTTP API.
        If HTTP authentication is disabled, tokens are generated without authentication.
        '''
        config = Config()
        
        # Check if HTTP auth is enabled
        if config['auth']['http_auth_enabled'] is True:
            # Require authentication
            if not check_auth():
                return http_error(
                    401, 'Unauthorized',
                    'HTTP authentication is required to generate A2A tokens'
                )
        
        # Generate token
        token = generate_auth_token()
        description = request.json.get('description', '') if request.json else ''
        user_id = session.get('username', 'unknown') if config['auth']['http_auth_enabled'] else 'no_auth'
        
        # Store token with metadata
        store_auth_token(token, user_id, description)
        
        logger.info(f"Generated authentication token for user: {user_id}")
        
        return {
            'token': token,
            'expires_in': 86400,  # 24 hours in seconds
            'description': description
        }, 200


@ns_conf.route('/token/validate', methods=['POST'])
class AuthTokenValidateRoute(Resource):
    @ns_conf.doc(
        responses={
            200: 'Success',
            400: 'Bad Request - Token not provided',
            401: 'Unauthorized - Invalid or expired token'
        },
        body=ns_conf.model('validate_a2a_token', {
            'token': fields.String(description='A2A token to validate', required=True)
        })
    )
    @api_endpoint_metrics('POST', '/auth_tokens/token/validate')
    def post(self):
        ''' Validate an A2A token
        
        This endpoint validates if an A2A token is valid and not expired.
        '''
        if not request.json or 'token' not in request.json:
            return http_error(
                400, 'Bad Request',
                'Token is required'
            )
        
        token = request.json['token']
        
        if not is_auth_token_valid(token):
            return http_error(
                401, 'Unauthorized',
                'Invalid or expired token'
            )
        
        from mindsdb.utilities.a2a_auth import get_auth_token_data
        token_data = get_auth_token_data(token)
        return {
            'valid': True,
            'created_at': token_data['created_at'],
            'description': token_data['description'],
            'user_id': token_data['user_id']
        }, 200


@ns_conf.route('/tokens', methods=['GET'])
class AuthTokensListRoute(Resource):
    @ns_conf.doc(
        responses={
            200: 'Success',
            401: 'Unauthorized - HTTP authentication required'
        }
    )
    @api_endpoint_metrics('GET', '/auth_tokens/tokens')
    def get(self):
        ''' List all active A2A tokens (admin only)
        
        This endpoint lists all active A2A tokens. Only available when HTTP authentication is enabled.
        '''
        config = Config()
        
        # Only allow if HTTP auth is enabled and user is authenticated
        if config['auth']['http_auth_enabled'] is True:
            if not check_auth():
                return http_error(
                    401, 'Unauthorized',
                    'HTTP authentication is required to list A2A tokens'
                )
        
        # Clean up expired tokens and get all tokens
        cleanup_expired_tokens()
        all_tokens = get_all_auth_tokens()
        
        # Return active tokens (without the actual token value for security)
        tokens_list = []
        for token, data in all_tokens.items():
            tokens_list.append({
                'token_preview': f"{token[:8]}...{token[-8:]}",
                'created_at': data['created_at'],
                'description': data['description'],
                'user_id': data['user_id']
            })
        
        return {
            'tokens': tokens_list,
            'count': len(tokens_list)
        }, 200


@ns_conf.route('/token/<token>', methods=['DELETE'])
class AuthTokenDeleteRoute(Resource):
    @ns_conf.doc(
        responses={
            200: 'Success',
            401: 'Unauthorized - HTTP authentication required',
            404: 'Not Found - Token not found'
        }
    )
    @api_endpoint_metrics('DELETE', '/auth_tokens/token/<token>')
    def delete(self, token):
        ''' Revoke an A2A token
        
        This endpoint revokes an A2A token. Only available when HTTP authentication is enabled.
        '''
        config = Config()
        
        # Only allow if HTTP auth is enabled and user is authenticated
        if config['auth']['http_auth_enabled'] is True:
            if not check_auth():
                return http_error(
                    401, 'Unauthorized',
                    'HTTP authentication is required to revoke A2A tokens'
                )
        
        if not revoke_auth_token(token):
            return http_error(
                404, 'Not Found',
                'Token not found'
            )
        
        logger.info(f"Revoked authentication token: {token[:8]}...{token[-8:]}")
        
        return {'message': 'Token revoked successfully'}, 200


@ns_conf.route('/status', methods=['GET'])
class AuthTokensStatusRoute(Resource):
    @ns_conf.doc(
        responses={
            200: 'Success'
        }
    )
    @api_endpoint_metrics('GET', '/auth_tokens/status')
    def get(self):
        ''' Get A2A authentication status
        
        This endpoint returns the current A2A authentication configuration.
        For unauthenticated users, only basic configuration is returned.
        '''
        config = Config()
        
        # Base response with configuration info
        response = {
            'http_auth_enabled': config['auth']['http_auth_enabled'],
            'auth_required': get_auth_required_status()
        }
        
        # Only include token count if user is authenticated
        if config['auth']['http_auth_enabled'] is True:
            if check_auth():
                all_tokens = get_all_auth_tokens()
                response['active_tokens_count'] = len(all_tokens)
        
        return response, 200
