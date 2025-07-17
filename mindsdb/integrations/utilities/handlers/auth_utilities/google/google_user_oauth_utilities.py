import json
from pathlib import Path
import requests
import datetime as dt
from flask import request

from mindsdb.utilities import log

from ..exceptions import AuthException

from google_auth_oauthlib.flow import Flow

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

logger = log.getLogger(__name__)


class GoogleUserOAuth2Manager:
    def __init__(self, handler_stroage: str, scopes: list, credentials_file: str = None, credentials_url: str = None, code: str = None):
        self.handler_storage = handler_stroage
        self.scopes = scopes
        self.credentials_file = credentials_file
        self.credentials_url = credentials_url
        self.code = code

    def get_oauth2_credentials(self):
        creds = None

        if self.credentials_file or self.credentials_url:
            oauth_user_info = self.handler_storage.encrypted_json_get('oauth_user_info')

            if oauth_user_info:
                creds = Credentials.from_authorized_user_info(oauth_user_info, self.scopes)

            if not creds or not creds.valid:
                logger.debug("Credentials do not exist or are invalid, attempting to authorize again")

                oauth_user_info = self._download_oauth_user_info()

                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                    logger.debug("Credentials refreshed successfully")
                else:
                    creds = self._execute_google_auth_flow(oauth_user_info)
                    logger.debug("New credentials obtained")

                self.handler_storage.encrypted_json_set('oauth_user_info', self._convert_credentials_to_dict(creds))
                logger.debug("Saving credentials to storage")

        return creds

    def _download_oauth_user_info(self):
        # if credentials_url is set, attempt to download the contents of the files
        # this will be given preference over credentials_file
        if self.credentials_url:
            response = requests.get(self.credentials_url)
            if response.status_code == 200:
                return response.json()
            else:
                logger.error("Failed to get credentials from URL", response.status_code)

        # if credentials_file is set, attempt to read the contents of the file
        if self.credentials_file:
            path = Path(self.credentials_file).expanduser()
            if path.exists():
                with open(path, 'r') as f:
                    return json.load(f)
            else:
                logger.error("Credentials file does not exist")

        raise ValueError('OAuth2 credentials could not be found')

    def _execute_google_auth_flow(self, oauth_user_info: dict):
        flow = Flow.from_client_config(
            oauth_user_info,
            scopes=self.scopes
        )

        flow.redirect_uri = request.headers['ORIGIN'] + '/verify-auth'

        if self.code:
            flow.fetch_token(code=self.code)
            creds = flow.credentials
            return creds
        else:
            auth_url = flow.authorization_url()[0]
            raise AuthException(f'Authorisation required. Please follow the url: {auth_url}', auth_url=auth_url)

    def _convert_credentials_to_dict(self, credentials):
        return {
            'token': credentials.token,
            'refresh_token': credentials.refresh_token,
            'token_uri': credentials.token_uri,
            'client_id': credentials.client_id,
            'client_secret': credentials.client_secret,
            'scopes': credentials.scopes,
            'expiry': dt.datetime.strftime(credentials.expiry, '%Y-%m-%dT%H:%M:%S')
        }
