import os
import json
import atexit

import msal
from msal.exceptions import MsalServiceError


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
            result = msal_app.acquire_token_by_device_flow(flow)

        return result
    
    def get_access_token(self):
        result = self._execute_auth_flow()

        if "access_token" in result:
            return result["access_token"]
        else:
            raise MsalServiceError(error=result.get("error"), error_description=result.get("error_description"))

    

