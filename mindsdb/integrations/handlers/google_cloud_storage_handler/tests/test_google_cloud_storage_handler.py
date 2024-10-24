import unittest
from mindsdb.integrations.handlers.google_cloud_storage_handler.google_cloud_storage_handler import GoogleCloudStorageHandler
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE


class GoogleCloudStorageHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_data": {
                "keyfile": "keyfile.json",
            }
        }
        cls.handler = GoogleCloudStorageHandler("test_google_cloud_storage_handler", **cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_get_tables(self):
        tables = self.handler.get_tables()
        print(tables)
        assert tables.type is not RESPONSE_TYPE.ERROR

    def test_2_select_query(self):
        result = self.handler.native_query(
            query="SELECT * FROM storage.buckets WHERE name = 'mindsdb-cloud-storage-test'"
        )
        assert result.type is RESPONSE_TYPE.TABLE

    def test_3_get_columns(self):
        columns = self.handler.get_columns("id")
        assert columns.type is not RESPONSE_TYPE.ERROR

    def test_4_insert_query(self):
        result = self.handler.native_query(
            query="INSERT INTO storage.buckets VALUES ('test-bucket', 'test-project', 'us', 'STANDARD_STORAGE_CLASS')"
        )
        assert result.type is not RESPONSE_TYPE.ERROR

    def test_5_update_query(self):
        result = self.handler.native_query(
            query="UPDATE storage.buckets SET storageClass = 'STANDARD_STORAGE_CLASS' WHERE name = 'test-bucket'"
        )
        assert result.type is not RESPONSE_TYPE.ERROR

    def test_6_delete_query(self):
        result = self.handler.native_query(
            query="DELETE FROM storage.buckets WHERE name = 'test-bucket'"
        )
        assert result.type is not RESPONSE_TYPE.ERROR


if __name__ == "__main__":
    unittest.main()
