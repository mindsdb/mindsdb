from typing import List, Text

import msal
from ..exceptions import AuthException

from mindsdb.utilities import log

logger = log.getLogger(__name__)


class MSGraphAPIDelegatedPermissionsManager:
    def __init__(
        self,
        client_id: Text,
        client_secret: Text,
        tenant_id: Text,
        cache: msal.SerializableTokenCache,
        scopes: List = ["https://graph.microsoft.com/.default"],
        code: Text = None,
    ) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.cache = cache
        self.scopes = scopes
        self.code = code

    def get_access_token(self):
        # Check if the cache already contains a valid access token
        msal_app = self._get_msal_app()
        accounts = msal_app.get_accounts()

        if accounts:
            response = msal_app.acquire_token_silent(self.scopes, account=accounts[0])
            if "access_token" in response:
                return response['access_token']

        # If no valid access token is found in the cache, run the auth flow
        response = self._execute_ms_graph_api_auth_flow()

        if "access_token" in response:
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
            token_cache=self.cache,
        )

    def _execute_ms_graph_api_auth_flow(self):
        msal_app = self._get_msal_app()

        if self.code:
            response = msal_app.acquire_token_by_authorization_code(
                code=self.code,
                scopes=self.scopes
            )

            return response
        else:
            auth_url = msal_app.get_authorization_request_url(
                scopes=self.scopes
            )

            raise AuthException(f'Authorisation required. Please follow the url: {auth_url}', auth_url=auth_url)
