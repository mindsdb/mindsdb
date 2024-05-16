import os
import json
import requests
import datetime as dt
from flask import request
from shutil import copyfile

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
            # get the current directory and checks tokens & creds
            curr_dir = self.handler_storage.folder_get('config')

            creds_file = os.path.join(curr_dir, 'creds.json')
            secret_file = os.path.join(curr_dir, 'secret.json')

            if os.path.isfile(creds_file):
                creds = Credentials.from_authorized_user_file(creds_file, self.scopes)

            if not creds or not creds.valid:
                logger.debug("Credentials do not exist or are invalid, attempting to authorize again")

                if self._download_secret_file(secret_file):
                    # save to storage
                    self.handler_storage.folder_sync('config')
                else:
                    raise ValueError('No valid Gmail Credentials filepath or S3 url found.')

                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                    logger.debug("Credentials refreshed successfully")
                else:
                    creds = self._execute_google_auth_flow(secret_file, self.scopes, self.code)
                    logger.debug("New credentials obtained")

                self._save_credentials_to_file(creds, creds_file)
                logger.debug(f"saved session credentials to {creds_file}")
                self.handler_storage.folder_sync('config')

        return creds

    def _download_secret_file(self, secret_file):
        # if credentials_url is set, attempt to download the file
        # this will be given preference over credentials_file
        if self.credentials_url:
            response = requests.get(self.credentials_url)
            if response.status_code == 200:
                with open(secret_file, 'w') as creds:
                    creds.write(response.text)
                return True
            else:
                logger.error("Failed to get credentials from S3", response.status_code)

        # if credentials_file is set, attempt to copy the file
        if self.credentials_file and os.path.isfile(self.credentials_file):
            copyfile(self.credentials_file, secret_file)
            return True
        return False

    def _execute_google_auth_flow(self, secret_file, scopes, code=None):
        flow = Flow.from_client_secrets_file(secret_file, scopes)

        flow.redirect_uri = request.headers['ORIGIN'] + '/verify-auth'

        if code:
            flow.fetch_token(code=code)
            creds = flow.credentials
            return creds
        else:
            auth_url = flow.authorization_url()[0]
            raise AuthException(f'Authorisation required. Please follow the url: {auth_url}', auth_url=auth_url)

    def _save_credentials_to_file(self, creds, file_path):
        with open(file_path, 'w') as token:
            data = self._convert_credentials_to_dict(creds)
            token.write(json.dumps(data))

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
