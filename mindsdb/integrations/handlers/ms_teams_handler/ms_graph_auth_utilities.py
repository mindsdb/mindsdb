import msal
from msal.exceptions import MsalServiceError


class MSGraphAuthUtilities:
    """
    This class contains the utilities to authenticate with Microsoft Graph API
    """

    def __init__(self, client_id, client_secret, tenant_id):
        """
        Initializes the class with the client_id, client_secret and tenant_id
        :param client_id: The client_id of the app
        :param client_secret: The client_secret of the app
        :param tenant_id: The tenant_id of the app
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id

    def get_msal_app(self):
        """
        Returns the msal app object
        :return: msal app object
        """
        return msal.ConfidentialClientApplication(
            self.client_id,
            authority=f"https://login.microsoftonline.com/{self.tenant_id}",
            client_credential=self.client_secret,
        )