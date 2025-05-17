import unittest
import os
import boto3
from botocore.config import Config
import time
from mindsdb.integrations.handlers.s3_handler.s3_handler import S3Handler
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from urllib.parse import urlparse


class S3EndpointHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Skip AWS tests for now as they require valid credentials
        cls.aws_handler = None

        # Test with MinIO (custom endpoint)
        cls.minio_kwargs = {
            "aws_access_key_id": "minioadmin",
            "aws_secret_access_key": "minioadmin",
            "bucket": "test-bucket",
            "endpoint_url": "http://localhost:9000"
        }
        cls.minio_handler = S3Handler('test_s3_minio', cls.minio_kwargs)

        # Setup MinIO client for direct operations
        cls.minio_client = boto3.client(
            's3',
            endpoint_url='http://localhost:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin',
            config=Config(connect_timeout=5, read_timeout=5)
        )

        # Create test file in MinIO
        try:
            test_data = "col1,col2\n1,2\n3,4"
            cls.minio_client.put_object(
                Bucket='test-bucket',
                Key='test.csv',
                Body=test_data
            )
            print("Successfully created test file in MinIO")
        except Exception as e:
            print(f"Error creating test file in MinIO: {e}")

    @unittest.skip("AWS tests require valid credentials")
    def test_aws_connection(self):
        """Test AWS S3 connection"""
        pass

    @unittest.skip("AWS tests require valid credentials")
    def test_aws_list_buckets(self):
        """Test listing AWS S3 buckets"""
        pass

    def test_minio_connection(self):
        """Test MinIO connection"""
        start_time = time.time()
        result = self.minio_handler.check_connection()
        if time.time() - start_time > 10:  # 10 second timeout
            self.fail("MinIO connection test timed out")
        if not result.success:
            print(f"MinIO Connection Error: {result.error_message}")
        self.assertTrue(result.success)
    
    def test_endpoint_parsing(self):
        test_cases = {
            "http://localhost:9000": "localhost:9000",
            "https://example.com:443": "example.com:443",
            "localhost:8000": "localhost:8000",
            "https://s3.wasabisys.com": "s3.wasabisys.com",
            "http://127.0.0.1:9000/": "127.0.0.1:9000"
        }

        for raw_url, expected in test_cases.items():
            with self.subTest(url=raw_url):
                parsed = urlparse(raw_url)
                endpoint = parsed.netloc or parsed.path
                self.assertEqual(endpoint.rstrip('/'), expected)

    def test_minio_list_buckets(self):
        """Test listing MinIO buckets"""
        start_time = time.time()
        result = self.minio_handler.get_tables()
        if time.time() - start_time > 10:  # 10 second timeout
            self.fail("MinIO list buckets test timed out")
        self.assertEqual(result.type, RESPONSE_TYPE.TABLE)
        tables = result.data_frame.to_dict('records')
        print(f"Found tables: {tables}")  # Debug output
        # Check for the file in the correct format
        self.assertTrue(any(table['table_name'] == '`test.csv`' for table in tables))

    def test_minio_read_file(self):
        """Test reading a file from MinIO"""
        start_time = time.time()
        # Use the correct table name format
        query = "SELECT * FROM `test.csv`"
        result = self.minio_handler.native_query(query)
        if time.time() - start_time > 10:  # 10 second timeout
            self.fail("MinIO read file test timed out")
        self.assertEqual(result.type, RESPONSE_TYPE.TABLE)
        self.assertGreater(len(result.data_frame), 0)

    def test_minio_write_file(self):
        """Test writing a file to MinIO"""
        start_time = time.time()
        import pandas as pd
        df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        
        # First ensure the bucket exists
        try:
            self.minio_client.head_bucket(Bucket='test-bucket')
        except Exception as e:
            print(f"Bucket check error: {e}")
            self.minio_client.create_bucket(Bucket='test-bucket')
        
        # Write with correct path format
        self.minio_handler.add_data_to_table('write_test.csv', df)
        
        result = self.minio_handler.get_tables()
        if time.time() - start_time > 10:  # 10 second timeout
            self.fail("MinIO write file test timed out")
        tables = result.data_frame.to_dict('records')
        self.assertTrue(any(table['table_name'] == '`write_test.csv`' for table in tables))

    @classmethod
    def tearDownClass(cls):
        # Clean up MinIO test files
        try:
            cls.minio_client.delete_object(Bucket='test-bucket', Key='test.csv')
            cls.minio_client.delete_object(Bucket='test-bucket', Key='write_test.csv')
        except Exception as e:
            print(f"Cleanup error: {e}")


if __name__ == '__main__':
    unittest.main() 