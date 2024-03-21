from google.oauth2 import service_account

from mindsdb.utilities import log


logger = log.getLogger(__name__)


class GoogleServiceAccountOauth2Utilities:
    def __init__(self, credentials_url: str = None, credentials_file: str = None, credentials_json: dict = None) -> None:
        self.credentials_url = credentials_url
        self.credentials_file = credentials_file
        self.credentials_json = credentials_json