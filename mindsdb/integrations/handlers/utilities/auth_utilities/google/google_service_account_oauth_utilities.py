import requests
from google.oauth2 import service_account

from mindsdb.utilities import log

from ..exceptions import NoCredentialsException


logger = log.getLogger(__name__)


class GoogleServiceAccountOauth2Utilities:
    def __init__(self, credentials_url: str = None, credentials_file: str = None, credentials_json: dict = None) -> None:
        # if no credentials provided, raise an exception
        if not any([credentials_url, credentials_file, credentials_json]):
            raise NoCredentialsException('No valid Google Service Account credentials provided.')
        self.credentials_url = credentials_url
        self.credentials_file = credentials_file
        self.credentials_json = credentials_json

    def get_oauth2_credentials(self):
        if self.credenetials_url:
            pass

        if self.credentials_file:
            creds = service_account.Credentials.from_service_account_file(self.credentials_file)
            return creds
        
        if self.credentials_json:
            creds = service_account.Credentials.from_service_account_info(self.credentials_json)
            return creds
        
    def _download_credentials_file(self):
        response = requests.get(self.credentials_url)
        if response.status_code == 200:
            return response.json()

        else:
            logger.error(f"Failed to get credentials from {self.credentials_url}", response.status_code)