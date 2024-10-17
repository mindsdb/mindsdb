import json
from flask import request

import msal
from ..exceptions import AuthException

from mindsdb.integrations.utilities.handlers.api_utilities import MSGraphAPIBaseClient

from mindsdb.utilities import log

logger = log.getLogger(__name__)


class MSGraphAPIAuthManager:
    def __init__(self, handler_storage: str, scopes: list, client_id: str, client_secret: str, tenant_id: str, code: str = None) -> None:
        self.handler_storage = handler_storage
        self.scopes = scopes
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.code = code

        # get the redirect uri from handler storage if it exists
        if self.handler_storage.json_get('args'):
            self.redirect_uri = self.handler_storage.json_get('args').get('redirect_uri')
        # otherwise, get it from the request headers
        # this is done because when the chatbot task runs, it doesn't have access to the request headers because it does not come through a request
        else:
            self.redirect_uri = request.headers['ORIGIN'] + '/verify-auth'
            if '127.0.0.1' in self.redirect_uri:
                self.redirect_uri = self.redirect_uri.replace('127.0.0.1', 'localhost')

            self.handler_storage.json_set('args', {'redirect_uri': self.redirect_uri})

    def get_access_token(self):
        try:
            creds = json.loads(self.handler_storage.file_get('creds'))
            access_token = creds.get('access_token')

            if not self._check_access_token_validity(access_token):
                logger.info('Access token expired. Refreshing...')
                response = self._refresh_access_token(creds.get('refresh_token'))
                self.handler_storage.file_set('creds', json.dumps(response).encode('utf-8'))
                access_token = response.get('access_token')

            return access_token
        except Exception as e:
            logger.error(f'Error getting credentials from storage: {e}!')

        response = self._execute_ms_graph_api_auth_flow()
        if "access_token" in response:
            self.handler_storage.file_set('creds', json.dumps(response).encode('utf-8'))
            return response['access_token']
        else:
            raise AuthException(
                f'Error getting access token: {response.get("error_description")}',
                auth_url=response.get('auth_url')
            )

    def _get_msal_app(self):
        return msal.ConfidentialClientApplication(
            self.client_id,
            authority=f"https://login.microsoftonline.com/{self.tenant_id}",
            client_credential=self.client_secret,
        )

    def _save_credentials_to_file(self, creds, creds_file):
        with open(creds_file, 'w') as f:
            f.write(json.dumps(creds))

    def _execute_ms_graph_api_auth_flow(self):
        msal_app = self._get_msal_app()

        if self.code:
            response = msal_app.acquire_token_by_authorization_code(
                code=self.code,
                scopes=self.scopes,
                redirect_uri=self.redirect_uri,
            )

            return response
        else:
            auth_url = msal_app.get_authorization_request_url(
                scopes=self.scopes,
                redirect_uri=self.redirect_uri,
            )

            raise AuthException(f'Authorisation required. Please follow the url: {auth_url}', auth_url=auth_url)

    def _refresh_access_token(self, refresh_token: str):
        msal_app = self._get_msal_app()

        response = msal_app.acquire_token_by_refresh_token(
            refresh_token=refresh_token,
            scopes=self.scopes,
        )

        return response

    def _check_access_token_validity(self, access_token: str):
        msal_graph_api_client = MSGraphAPIBaseClient(access_token)
        try:
            msal_graph_api_client.get_user_profile()
            return True
        except Exception as e:
            if 'InvalidAuthenticationToken' in str(e):
                return False
            else:
                raise e
