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
        # raise a HTTPError if the status is 4xx or 5xx
        response.raise_for_status()

        return self._parse_credentials_json(response.json())

    def _parse_credentials_json(self, credentials_json: str) -> dict:
        if isinstance(credentials_json, str):
            try:
                # attempt to convert to JSON
                return json.loads(credentials_json)
            except json.JSONDecodeError:
                raise ValueError("Failed to parse credentials provided. Please provide a valid service account key.")
        else:
            # unescape new lines in private_key
            credentials_json['private_key'] = credentials_json['private_key'].replace('\\n', '\n')
            return credentials_json
