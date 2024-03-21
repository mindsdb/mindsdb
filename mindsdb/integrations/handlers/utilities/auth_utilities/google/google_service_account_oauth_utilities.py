from mindsdb.utilities import log

from ..exceptions import NoCredentialsException

from google.oauth2 import service_account


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