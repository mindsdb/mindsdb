from abc import ABC, abstractmethod
from typing import Dict, List, Text

from flask import request
import msal

from mindsdb.integrations.utilities.handlers.auth_utilities.exceptions import AuthException
from mindsdb.utilities import log


logger = log.getLogger(__name__)


class MSGraphAPIPermissionsManager(ABC):
    """
    The base class for managing the delegated permissions for the Microsoft Graph API.
    """
    def __init__(
        self,
        client_id: Text,
        client_secret: Text,
        tenant_id: Text,
        cache: msal.SerializableTokenCache,
        scopes: List = ["https://graph.microsoft.com/.default"],
    ) -> None:
        """
        Initializes the permissions manager.

        Args:
            client_id (Text): The client ID of the application registered in Microsoft Entra ID.
            client_secret (Text): The client secret of the application registered in Microsoft Entra ID.
            tenant_id (Text): The tenant ID of the application registered in Microsoft Entra ID.
            cache (msal.SerializableTokenCache): The token cache for storing the access token.
            scopes (List): The scopes for the Microsoft Graph API.
            code (Text): The authentication code for acquiring the access token.
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.cache = cache
        self.scopes = scopes

    @abstractmethod
    def get_access_token(self) -> Text:
        """
        Retrieves an access token for the Microsoft Graph API.

        Returns:
            Text: The access token for the Microsoft Graph API.
        """
        pass

    def _get_msal_app(self) -> msal.ConfidentialClientApplication:
        """
        Returns an instance of the MSAL ConfidentialClientApplication.

        Returns:
            msal.ConfidentialClientApplication: An instance of the MSAL ConfidentialClientApplication.
        """
        return msal.ConfidentialClientApplication(
            self.client_id,
            authority=f"https://login.microsoftonline.com/{self.tenant_id}",
            client_credential=self.client_secret,
            token_cache=self.cache,
        )


class MSGraphAPIDelegatedPermissionsManager(MSGraphAPIPermissionsManager):
    """
    The class for managing the delegated permissions for the Microsoft Graph API.
    """
    def __init__(
        self,
        client_id: Text,
        client_secret: Text,
        tenant_id: Text,
        cache: msal.SerializableTokenCache,
        scopes: List = ["https://graph.microsoft.com/.default"],
        code: Text = None,
    ) -> None:
        """
        Initializes the delegated permissions manager.

        Args:
            client_id (Text): The client ID of the application registered in Microsoft Entra ID.
            client_secret (Text): The client secret of the application registered in Microsoft Entra ID.
            tenant_id (Text): The tenant ID of the application registered in Microsoft Entra ID.
            cache (msal.SerializableTokenCache): The token cache for storing the access token.
            scopes (List): The scopes for the Microsoft Graph API.
            code (Text): The authentication code for acquiring the access token.
        """
        super().__init__(client_id, client_secret, tenant_id, cache, scopes)
        self.code = code
        self.redirect_uri = None
        self._set_redirect_uri()

    def _set_redirect_uri(self) -> None:
        """
        Sets the redirect URI based on the request origin.

        Raises:
            AuthException: If the request origin could not be determined.
        """
        # Set the redirect URI based on the request origin.
        # If the request origin is 127.0.0.1 (localhost), replace it with localhost.
        # This is done because the only HTTP origin allowed in Microsoft Entra ID app registration is localhost.
        try:
            request_origin = request.headers.get('ORIGIN') or (request.scheme + '://' + request.host)
            if not request_origin:
                raise AuthException('Request origin could not be determined!')
        except RuntimeError:
            # if it is outside of request context (streaming in agent)
            request_origin = ''

        request_origin = request_origin.replace('127.0.0.1', 'localhost') if 'http://127.0.0.1' in request_origin else request_origin
        self.redirect_uri = request_origin + '/verify-auth'

    def get_access_token(self) -> Text:
        """
        Retrieves an access token for the Microsoft Graph API.
        If a valid access token is found in the cache, it is returned.
        Otherwise, the authentication flow is executed.

        Returns:
            Text: The access token for the Microsoft Graph API.
        """
        # Check if a valid access token is already in the cache for the signed-in user.
        msal_app = self._get_msal_app()
        accounts = msal_app.get_accounts()

        if accounts:
            response = msal_app.acquire_token_silent(self.scopes, account=accounts[0])
            if "access_token" in response:
                return response['access_token']

        # If no valid access token is found in the cache, run the authentication flow.
        response = self._execute_ms_graph_api_auth_flow()

        if "access_token" in response:
            return response['access_token']
        # If no access token is returned, raise an exception.
        # This is the expected behaviour when the user attempts to authenticate for the first time.
        else:
            raise AuthException(
                f'Error getting access token: {response.get("error_description")}',
                auth_url=response.get('auth_url')
            )

    def _execute_ms_graph_api_auth_flow(self) -> Dict:
        """
        Executes the authentication flow for the Microsoft Graph API.
        If the authentication code is provided, the token is acquired by authorization code.
        Otherwise, the authorization request URL is returned.

        Raises:
            AuthException: If the authentication code is not provided

        Returns:
            Dict: The response from the Microsoft Graph API authentication flow.
        """
        msal_app = self._get_msal_app()

        # If the authentication code is provided, acquire the token by authorization code.
        if self.code:
            response = msal_app.acquire_token_by_authorization_code(
                code=self.code,
                scopes=self.scopes,
                redirect_uri=self.redirect_uri
            )

            return response

        # If the authentication code is not provided, get the authorization request URL.
        else:
            auth_url = msal_app.get_authorization_request_url(
                scopes=self.scopes,
                redirect_uri=self.redirect_uri
            )

            raise AuthException(f'Authorisation required. Please follow the url: {auth_url}', auth_url=auth_url)


class MSGraphAPIApplicationPermissionsManager(MSGraphAPIPermissionsManager):
    """
    The class for managing application permissions for the Microsoft Graph API.
    """

    def get_access_token(self) -> Text:
        """
        Retrieves an access token for the Microsoft Graph API using the client credentials flow.

        Returns:
            Text: The access token for the Microsoft Graph API.
        """
        msal_app = self._get_msal_app()

        # Check if a valid access token is already in the cache.
        accounts = msal_app.get_accounts()
        if accounts:
            response = msal_app.acquire_token_silent(self.scopes, account=accounts[0])
            if "access_token" in response:
                return response["access_token"]

        # If no valid access token is found in the cache, acquire a new token using client credentials.
        response = msal_app.acquire_token_for_client(scopes=self.scopes)

        if "access_token" in response:
            return response["access_token"]
        else:
            raise AuthException(
                f"Error getting access token: {response.get('error_description')}"
            )
