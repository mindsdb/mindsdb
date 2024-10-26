import unittest

from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from mindsdb.integrations.handlers.google_cloud_storage_handler.google_cloud_storage_handler import \
    GoogleCloudStorageHandler


class GCSHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "service_account_keys": 'C:/Users/Talaat/Documents/Github/integration/creds/credentials.json',
            "bucket": 'easy_tour_bucket',
            "prefix": 'ai',
            "gcs_access_key_id": 'AKIAIOSFODNN7EXAMPLE',
            "gcs_secret_access_key": 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
        }
        cls.handler = GoogleCloudStorageHandler('test_gcs_handler', cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_get_tables(self):
        result = self.handler.get_tables()
        assert result.type is RESPONSE_TYPE.TABLE


if __name__ == '__main__':
    unittest.main()
