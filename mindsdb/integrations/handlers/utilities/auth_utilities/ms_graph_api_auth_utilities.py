import msal
from msal.exceptions import MsalServiceError


class MSGraphAPIAuthManager:
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

    def _get_msal_app(self):
        return msal.ConfidentialClientApplication(
            self.client_id,
            authority=f"https://login.microsoftonline.com/{self.tenant_id}",
            client_credential=self.client_secret,
        )
    
    def get_access_token(self):
        msal_app = self._get_msal_app()

        scope = ["https://graph.microsoft.com/.default"]
        if self.refresh_token:
            result = msal_app.acquire_token_by_refresh_token(self.refresh_token, scopes=scope)
        else:
            result = msal_app.acquire_token_for_client(scopes=scope)
        if "access_token" in result:
            return result["access_token"]
        else:
            raise MsalServiceError(error=result.get("error"), error_description=result.get("error_description"))