import unittest
from unittest.mock import patch, MagicMock, Mock
from collections import OrderedDict
from mindsdb.integrations.handlers.athena_handler.athena_handler import AthenaHandler


class CursorContextManager(Mock):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    description = [['a']]

    def fetchall(self):
        return [[1]]


class AthenaHandlerTest(unittest.TestCase):
    dummy_connection_data = OrderedDict(
        aws_access_key_id='aws_access_key_id',
        aws_secret_access_key='aws_secret_access_key',
        region_name='us-east-1',
        database='default',
        workgroup='my_workgroup',
        catalog='AwsDataCatalog',
        results_output_location='s3://bucket-path/athena-query-results',
        check_interval=0
    )

    def setUp(self):
        self.patcher = patch('boto3.client')
        self.mock_client = self.patcher.start()
        self.mock_client.return_value = MagicMock()
        self.handler = AthenaHandler('athena', connection_data=self.dummy_connection_data)

    def tearDown(self):
        self.patcher.stop()

    def test_connect_success(self):
        connection = self.handler.connect()
        self.assertIsNotNone(connection)
        self.assertTrue(self.handler.is_connected)

    def test_get_columns(self):
        self.handler.native_query = MagicMock()

        table_name = "mock_table"
        self.handler.get_columns(table_name)

        expected_query = f"""
            select
                column_name as "Field",
                data_type as "Type"
            from
                information_schema.columns
            where
                table_name = '{table_name}'
        """

        self.handler.native_query.assert_called_once_with(expected_query)

    def test_get_tables(self):
        self.handler.native_query = MagicMock()

        self.handler.get_tables()

        expected_query = """
            select
                table_schema,
                table_name,
                table_type
            from
                information_schema.tables
            where
                table_schema not in ('information_schema')
            and table_type in ('BASE TABLE', 'VIEW')
        """

        self.handler.native_query.assert_called_once_with(expected_query)


if __name__ == '__main__':
    unittest.main()
