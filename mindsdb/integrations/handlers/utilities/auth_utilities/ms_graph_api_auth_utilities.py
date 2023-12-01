import os
import json
import atexit
from flask import request

import msal
from msal.exceptions import MsalServiceError
from .exceptions import AuthException

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

    def get_access_token(self):
        try:
            creds = json.loads(self.handler_storage.file_get('creds'))
            access_token = creds.get('access_token')
            
            return access_token
        except Exception as e:
            logger.error(f'Error getting credentials from storage: {e}!')

        response = self._execute_ms_graph_api_auth_flow()
        if "access_token" in response:
            self.handler_storage.file_set('creds', json.dumps(response).encode('utf-8'))
            return response['access_token']
        else:
            raise AuthException(f'Error getting access token: {response.get("error_description")}', auth_url=response.get('auth_url'))            

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
            )

            return response
        else:
            # TODO: Pass the redirect_uri as a parameter when getting the auth url
            redirect_uri = request.headers['ORIGIN'] + '/verify-auth'

            auth_url = msal_app.get_authorization_request_url(
                scopes=self.scopes,
                # redirect_uri=request.headers['ORIGIN'] + '/verify-auth',
            )

            raise AuthException(f'Authorisation required. Please follow the url: {auth_url}', auth_url=auth_url)


class MSGraphAPIApplicationPermissionsManager:
    def __init__(self, client_id: str, client_secret: str, tenant_id: str, refresh_token: str = None):
        """
        Initializes the class with the client_id, client_secret and tenant_id
        :param client_id: The client_id of the app
        :param client_secret: The client_secret of the app
        :param tenant_id: The tenant_id of the app
        :param refresh_token: The refresh_token of the app
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.refresh_token = refresh_token
        self.msal_app = self._get_msal_app()

    def _get_msal_app(self):
        return msal.ConfidentialClientApplication(
            self.client_id,
            authority=f"https://login.microsoftonline.com/{self.tenant_id}",
            client_credential=self.client_secret,
        )
    
    def get_access_token(self):
        scope = ["https://graph.microsoft.com/.default"]
        if self.refresh_token:
            result = self.msal_app.acquire_token_by_refresh_token(self.refresh_token, scopes=scope)
        else:
            result = self.msal_app.acquire_token_for_client(scopes=scope)
        if "access_token" in result:
            return result["access_token"]
        else:
            raise MsalServiceError(error=result.get("error"), error_description=result.get("error_description"))
        

class MSGraphAPIDelegatedPermissionsManager:
    scopes = ["https://graph.microsoft.com/.default"]

    def __init__(self, client_id: str, tenant_id: str):
        """
        Initializes the class with the client_id, client_secret and tenant_id
        :param client_id: The client_id of the app
        :param tenant_id: The tenant_id of the app
        """
        self.client_id = client_id
        self.tenant_id = tenant_id
        self.msal_app = self._get_msal_app()

    def _get_msal_app(self):
        return msal.PublicClientApplication(
            self.client_id,
            authority=f"https://login.microsoftonline.com/{self.tenant_id}",
            token_cache=self._get_or_create_token_cache(),
        )
    
    def _get_or_create_token_cache(self):
        cache = msal.SerializableTokenCache()

        if os.path.exists("token_cache(.bin"):
            cache.deserialize(open("token_cache(.bin", "r").read())
        else:
            open("token_cache.bin", "w+").close()

        atexit.register(lambda:
            open("token_cache(.bin", "w").write(cache.serialize())
            if cache.has_state_changed else None
            )

        return cache
    
    def _execute_auth_flow(self):
        result = None
        accounts = self.msal_app.get_accounts()
        if accounts:
            # TODO: Is accounts[0] always the right one?
            result = self.msal_app.acquire_token_silent(self.scopes, account=accounts[0])

        if not result:
            flow = self.msal_app.initiate_device_flow(scopes=self.scopes)
            if "user_code" not in flow:
                raise ValueError(
                    "Failed to create device flow. Err: %s" % json.dumps(flow, indent=4)
                )
            logger.info(flow["message"])
            result = self.msal_app.acquire_token_by_device_flow(flow)

        return result
    
    def get_access_token(self):
        result = self._execute_auth_flow()

        if "access_token" in result:
            return result["access_token"]
        else:
            raise MsalServiceError(error=result.get("error"), error_description=result.get("error_description"))

    

