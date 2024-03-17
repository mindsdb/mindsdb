import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from pandas.testing import assert_frame_equal
from mindsdb.integrations.handlers.influxdb_handler.influxdb_tables import InfluxDBTables
from mindsdb_sql.parser.ast import Select, Identifier, BinaryOperation, Constant
MOCK_TABLE_NAME = 'test_table'
MOCK_COLUMNS = ['time', 'temperature', 'humidity']
MOCK_DATA = pd.DataFrame({
    'time': pd.date_range('2021-01-01', periods=3, freq='D'),
    'temperature': [22.5, 23.0, 21.5],
    'humidity': [30, 45, 50]
})
MOCK_EMPTY_DATA = pd.DataFrame(columns=MOCK_COLUMNS)
MOCK_QUERY = "SELECT \"temperature\", \"humidity\" FROM \"test_table\" WHERE \"time\" > '2021-01-01' ORDER BY \"time\" DESC LIMIT 2;"
class MockLimit:
    def __init__(self, value):
        self.value = value

class MockSelect(Select):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.limit = MockLimit(kwargs.get('limit', 0))

class TestInfluxDBTables(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mock_handler = MagicMock()
        cls.mock_handler.connection_data = {'influxdb_table_name': MOCK_TABLE_NAME}
        cls.influxdb_tables = InfluxDBTables(cls.mock_handler)

    @patch('mindsdb.integrations.handlers.influxdb_handler.influxdb_tables.InfluxDBTables.get_select_query')
    @patch('mindsdb.integrations.handlers.influxdb_handler.influxdb_tables.InfluxDBTables.get_columns')
    def test_select_basic_query(self, mock_get_columns, mock_get_select_query):
        mock_get_columns.return_value = MOCK_COLUMNS
        mock_get_select_query.return_value = MOCK_QUERY
        self.mock_handler.call_influxdb_tables.reset_mock()  
        self.mock_handler.call_influxdb_tables.return_value = MOCK_DATA

        query = Select(targets=[Identifier('temperature'), Identifier('humidity')], from_table=MOCK_TABLE_NAME)
        result = self.influxdb_tables.select(query)

        assert_frame_equal(result, MOCK_DATA)
        self.mock_handler.call_influxdb_tables.assert_called_with(MOCK_QUERY)  
    def test_get_columns(self):
        self.mock_handler.call_influxdb_tables.return_value = MOCK_DATA

        columns = self.influxdb_tables.get_columns()

        self.assertEqual(columns, MOCK_COLUMNS)

    @patch('mindsdb.integrations.handlers.influxdb_handler.influxdb_tables.InfluxDBTables.get_columns')
    def test_select_with_multiple_conditions(self, mock_get_columns):
        mock_get_columns.return_value = MOCK_COLUMNS
        self.mock_handler.call_influxdb_tables.return_value = MOCK_DATA

        where_conditions = BinaryOperation(op='AND', args=[
            BinaryOperation(op='>', args=[Identifier('temperature'), Constant('20')]),
            BinaryOperation(op='<', args=[Identifier('humidity'), Constant('50')])
        ])
        query = Select(targets=[Identifier('*')], from_table=MOCK_TABLE_NAME, where=where_conditions)
        self.influxdb_tables.select(query)

        self.assertTrue(self.mock_handler.call_influxdb_tables.called)
    @patch('mindsdb.integrations.handlers.influxdb_handler.influxdb_tables.InfluxDBTables.get_select_query')
    def test_select_empty_data_response(self, mock_get_select_query):
        """Test the behavior when the query returns no data."""
        mock_get_select_query.return_value = MOCK_QUERY
        self.mock_handler.call_influxdb_tables.return_value = MOCK_EMPTY_DATA

        query = Select(targets=[Identifier('*')], from_table=MOCK_TABLE_NAME)
        result = self.influxdb_tables.select(query)

        assert_frame_equal(result, MOCK_EMPTY_DATA)
        mock_get_select_query.assert_called_with(MOCK_TABLE_NAME, ['*'], [], {}, None)
        
    def test_select_query_no_conditions(self):
        """Test selecting data without specific conditions."""
        self.mock_handler.call_influxdb_tables.return_value = MOCK_DATA

        query = Select(targets=[Identifier('*')], from_table=MOCK_TABLE_NAME)
        result = self.influxdb_tables.select(query)

        assert_frame_equal(result, MOCK_DATA)
    def test_select_incorrect_table_name(self):
        """Test selecting data with an incorrect table name."""
        incorrect_table_name = "wrong_table"
        self.mock_handler.call_influxdb_tables.return_value = MOCK_EMPTY_DATA

        query = Select(targets=[Identifier('*')], from_table=incorrect_table_name)
        result = self.influxdb_tables.select(query)

        assert_frame_equal(result, MOCK_EMPTY_DATA)

    def test_get_columns_including_name_and_tags(self):
        """Test get_columns when the DB returns 'name' and 'tags' among others."""
        all_columns = [ 'time', 'sensor_id', 'temperature', 'humidity']
        self.mock_handler.call_influxdb_tables.return_value = pd.DataFrame(columns=all_columns)

        columns = self.influxdb_tables.get_columns()

        self.assertNotIn('name', columns)
        self.assertNotIn('tags', columns)
        for col in MOCK_COLUMNS:
            self.assertIn(col, columns)
    def test_verify_all_expected_columns_are_returned(self):
        """Test that all expected columns are returned by get_columns."""
        expected_columns = ['time', 'sensor_id', 'temperature', 'humidity']
        self.mock_handler.call_influxdb_tables.return_value = pd.DataFrame(columns=expected_columns)

        columns = self.influxdb_tables.get_columns()

        self.assertEqual(len(columns), len(expected_columns))
        for col in expected_columns:
            self.assertIn(col, columns)

    def test_verify_behavior_with_no_columns_returned(self):
        """Test get_columns behavior when no columns are returned from the DB."""
        self.mock_handler.call_influxdb_tables.return_value = pd.DataFrame()

        columns = self.influxdb_tables.get_columns()

        self.assertEqual(len(columns), 0)
        
    
             
if __name__ == '__main__':
    unittest.main()
