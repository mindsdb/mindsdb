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
            "gcs_access_key_id": 'AQAXEQK89OX07YS34OP',
            "gcs_secret_access_key": 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
        }
        cls.handler = GoogleCloudStorageHandler('test_gcs_handler', cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_native_query_select(self):
        query = "SELECT * FROM GCSObject"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_2_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.ERROR

    def test_3_get_columns(self):
        columns = self.handler.get_columns()
        assert columns.type is not RESPONSE_TYPE.ERROR


if __name__ == '__main__':
    unittest.main()
