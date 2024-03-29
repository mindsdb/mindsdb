import json
import requests
from typing import Union
from google.oauth2 import service_account

from mindsdb.utilities import log

from ..exceptions import NoCredentialsException, AuthException


logger = log.getLogger(__name__)


class GoogleServiceAccountOAuth2Manager:
    def __init__(self, credentials_url: str = None, credentials_file: str = None, credentials_json: Union[dict, str] = None) -> None:
        # if no credentials provided, raise an exception
        if not any([credentials_url, credentials_file, credentials_json]):
            raise NoCredentialsException('No valid Google Service Account credentials provided.')
        self.credentials_url = credentials_url
        self.credentials_file = credentials_file
        if credentials_json:
            self.credentials_json = self._parse_credentials_json(credentials_json)
        else:
            self.credentials_json = None

    def get_oauth2_credentials(self):
        try:
            if self.credentials_url:
                creds = service_account.Credentials.from_service_account_info(self._download_credentials_file())
                return creds

            if self.credentials_file:
                creds = service_account.Credentials.from_service_account_file(self.credentials_file)
                return creds
            
            if self.credentials_json:
                creds = service_account.Credentials.from_service_account_info(self.credentials_json)
                return creds
        except Exception as e:
            raise AuthException(f"Authentication failed: {e}")


    def _download_credentials_file(self):
        response = requests.get(self.credentials_url)
        if response.status_code == 200:
            return self._parse_credentials_json(response.json())

        else:
            logger.error(f"Failed to get credentials from {self.credentials_url}", response.status_code)

    def _parse_credentials_json(self, credentials_json: str) -> dict:
        if isinstance(credentials_json, str):
            # convert to JSON
            return json.loads(credentials_json)
        else:
            # unescape new lines in private_key
            credentials_json['private_key'] = credentials_json['private_key'].replace('\\n', '\n')
            return credentials_json