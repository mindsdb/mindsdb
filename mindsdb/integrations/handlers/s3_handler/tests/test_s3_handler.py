import unittest
from botocore.exceptions import ClientError
from mindsdb.integrations.handlers.s3_handler.s3_handler import S3Handler
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


class S3HandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "aws_access_key_id": "PCAQ2LJDOSWLNSQKOCPW",
            "aws_secret_access_key": "U/VjewPlNopsDmmwItl34r2neyC6WhZpUiip57i",
            "region_name": "us-east-1",
            "bucket": "mindsdb-bucket",
            "key": "iris.csv",
            "input_serialization": "{'CSV': {'FileHeaderInfo': 'NONE'}}"
        }
        cls.handler = S3Handler('test_s3_handler', cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_native_query_select(self):
        query = "SELECT * FROM S3Object"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_2_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.ERROR

    def test_3_get_columns(self):
        columns = self.handler.get_columns()
        assert columns.type is not RESPONSE_TYPE.ERROR

    def test_4_read_as_table_with_region(self):
        # This test verifies that providing a region prevents the HTTP 400 error.
        # We expect a ClientError because the file doesn't exist, which is correct.
        # The key is to NOT get a duckdb HTTPException about a missing region.
        handler = S3Handler('test_s3_handler_with_region', {
            "aws_access_key_id": "PCAQ2LJDOSWLNSQKOCPW",
            "aws_secret_access_key": "U/VjewPlNopsDmmwItl34r2neyC6WhZpUiip57i",
            "bucket_name": "mindsdb-bucket",
            "region_name": "us-east-1"
        })
        with self.assertRaises(ClientError):
            # This call will trigger _connect_duckdb, which was the source of the bug.
            # By providing region_name, our patch should prevent the duckdb HTTPException.
            # We expect a ClientError instead because the bucket/file is not real.
            handler.read_as_table('nonexistent_file.csv')


if __name__ == '__main__':
    unittest.main()
