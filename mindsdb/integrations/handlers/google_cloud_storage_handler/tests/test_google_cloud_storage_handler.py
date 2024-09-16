import unittest
from mindsdb.integrations.handlers.google_cloud_storage_handler.google_cloud_storage_handler import \
    GoogleCloudStorageHandler


class GCSHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "service_account_keys": 'C:/Users/Talaat/Documents/Github/integration/creds/credentials.json',
            "bucket": 'easy_tour_bucket',
            'project_id': 'easytour-422214'
        }
        cls.handler = GoogleCloudStorageHandler('test_gcs_handler', cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()


if __name__ == '__main__':
    unittest.main()
