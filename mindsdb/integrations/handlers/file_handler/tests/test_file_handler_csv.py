# def native_query(self, query: Any) -> HandlerResponse:
#     """Receive raw query and act upon it somehow.
#     Args:
#         query (Any): query in native format (str for sql databases,
#             dict for mongo, etc)
#     Returns:
#         HandlerResponse
#     """
# def get_tables(self) -> HandlerResponse:
#     """ Return list of entities
#     Return list of entities that will be accesible as tables.
#     Returns:
#         HandlerResponse: shoud have same columns as information_schema.tables
#             (https://dev.mysql.com/doc/refman/8.0/en/information-schema-tables-table.html)
#             Column 'TABLE_NAME' is mandatory, other is optional.
#     """

# def get_columns(self, table_name: str) -> HandlerResponse:
#     """ Returns a list of entity columns
#     Args:
#         table_name (str): name of one of tables returned by self.get_tables()
#     Returns:
#         HandlerResponse: shoud have same columns as information_schema.columns
#             (https://dev.mysql.com/doc/refman/8.0/en/information-schema-columns-table.html)
#             Column 'COLUMN_NAME' is mandatory, other is optional. Hightly
#             recomended to define also 'DATA_TYPE': it should be one of
#             python data types (by default it str).
#     """


import io
import os
from io import StringIO, BytesIO
import pandas as pd
import pytest
from mindsdb.integrations.handlers.file_handler.file_handler import FileHandler
from unittest.mock import Mock

# from libs.response import HandlerResponse
from mindsdb.integrations.handlers.file_handler.tests.conftest import (
    assert_not_identified_as,
    assert_identified_as,
    read_csv_file,
)
import magic
from mindsdb.integrations.libs.response import HandlerResponse
from unittest.mock import MagicMock

class TestIsItParquet:

    @staticmethod
    def test_valid_parquet_file():
        # Valid Parquet file signature (PAR1 at the beginning and end)
        parquet_data = BytesIO(b'PAR1' + b'valid_parquet_data' + b'PAR1')
        assert FileHandler.is_it_parquet(parquet_data) is True

    @staticmethod
    def test_invalid_parquet_file():
        # Invalid Parquet file signature (PAR1 at the beginning but not at the end)
        parquet_data = BytesIO(b'PAR1' + b'invalid_parquet_data')
        assert FileHandler.is_it_parquet(parquet_data) is False

    @staticmethod
    def test_empty_input():
        # Empty input
        parquet_data = BytesIO(b'')
        assert FileHandler.is_it_parquet(parquet_data) is False

    @staticmethod
    def test_non_parquet_data():
        # Non-Parquet data
        non_parquet_data = BytesIO(b'Non-Parquet Data')
        assert FileHandler.is_it_parquet(non_parquet_data) is False

    @staticmethod
    def test_mixed_data():
        # Valid Parquet signature at the beginning, but non-Parquet data at the end
        mixed_data = BytesIO(b'PAR1' + b'mixed_data')
        assert FileHandler.is_it_parquet(mixed_data) is False

    @staticmethod
    def test_is_it_parquet_using_csv(csv_file_path: str):
        with open(csv_file_path, "rb") as csv_file:
            # Check if the CSV data is identified as Parquet (should return False)
            is_parquet = FileHandler.is_it_parquet(csv_file)
        # Assert that the CSV data is not identified as Parquet
        assert_not_identified_as("Parquet", csv_file_path, is_parquet)

if __name__ == '__main__':
    pytest.main()


def test_is_it_xlsx_using_csv(csv_file_path: str):
    file_type = magic.from_file(csv_file_path, mime=True)
    is_xlsx = file_type == [
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.ms-excel",
    ]
    # Assert that the CSV data is not identified as XLSX
    assert_not_identified_as("XLSX", csv_file_path, is_xlsx)


def test_json_data_using_csv(csv_file_path: str):
    csv_data = read_csv_file(csv_file_path)
    # Create a StringIO object with the CSV data
    data_io = StringIO(csv_data.decode("utf-8"))
    # Check if the CSV data is identified as JSON (should return False)
    is_json = FileHandler.is_it_json(data_io)
    # Assert that the CSV data is not identified as JSON
    assert_not_identified_as("JSON", csv_file_path, is_json)


def test_is_it_csv(csv_file_path: str):
    csv_data = read_csv_file(csv_file_path)
    data_io = StringIO(csv_data.decode("utf-8"))
    is_csv = FileHandler.is_it_csv(data_io)
    assert_identified_as("CSV", csv_file_path, is_csv)


def test_get_file_path_with_csv(mocker, csv_file_path: str):
    # Test when the input path is a URL
    path = csv_file_path
    mocker.patch.object(FileHandler, "_fetch_url", return_value=csv_file_path)
    result = FileHandler._get_file_path(path)
    assert result == csv_file_path


def test_handle_source_with_csv(csv_file_path: str):
    df, col_map = FileHandler._handle_source(csv_file_path)
    # Assert that df is a DataFrame
    assert type(df) == pd.DataFrame
    # Assert that col_map is a dictionary
    assert type(col_map) == dict


def test_check_valid_dialect_coma(csv_file_path: str):
    csv_data = read_csv_file(csv_file_path)
    # Create a file-like object from the bytes data
    csv_data_filelike = io.BytesIO(csv_data)
    dialect = FileHandler._get_csv_dialect(csv_data_filelike)
    assert dialect.delimiter == ","

def test_get_data_io_csv(csv_file_path: str):
    data_io, file_format, dialect = FileHandler._get_data_io(csv_file_path)
    assert file_format == 'csv'
    assert dialect is not None

