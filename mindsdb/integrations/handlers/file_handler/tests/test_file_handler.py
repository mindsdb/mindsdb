import io
import os
import tempfile
from io import BytesIO, StringIO

import magic
import pandas
import pandas as pd
import pytest

from mindsdb.integrations.handlers.file_handler.file_handler import FileHandler
from mindsdb.integrations.handlers.file_handler.tests.conftest import (
    assert_identified_as,
    assert_not_identified_as,
    read_csv_file,
)

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


def check_file_format(file_path, expected_format, checker_function):
    with open(file_path, "rb") as file:
        file_data = file.read()

    data_io = StringIO(file_data.decode("utf-8"))
    is_expected_format = checker_function(data_io)

    return is_expected_format


def read_csv_file(csv_file_path):
    with open(csv_file_path, "rb") as csv_file:
        csv_data = csv_file.read()
    return csv_data


def assert_not_identified_as(data_format, file_path, is_identified):
    assert not is_identified, f"{file_path} should not be identified as {data_format}"


def assert_identified_as(data_format, file_path, is_identified):
    assert is_identified, f"{file_path} should be identified as {data_format}"


# All files will have the following content:
#
# |  col_one  |  col_two  | col_three |
# |-----------|-----------|-----------|
# |     1     |     -1    |    0.1    |
# |     2     |     -2    |    0.2    |
# |     3     |     -3    |    0.3    |

test_file_content = [
    ["col_one", "col_two", "col_three"],
    [1, -1, 0.1],
    [2, -2, 0.2],
    [3, -3, 0.3],
]


@pytest.fixture()
def temp_dir():
    return tempfile.mkdtemp(prefix="test_file_handler")


@pytest.fixture
def csv_file(temp_dir) -> str:
    file_path = os.path.join(temp_dir, "test_data.csv")
    df = pandas.DataFrame(test_file_content)
    df.to_csv(file_path, index=False)
    return file_path


@pytest.fixture
def xlsx_file(temp_dir) -> str:
    file_path = os.path.join(temp_dir, "test_data.xlsx")
    df = pandas.DataFrame(test_file_content)
    df.to_excel(file_path, index=False)
    return file_path


@pytest.fixture
def json_file(temp_dir) -> str:
    file_path = os.path.join(temp_dir, "test_data.json")
    df = pandas.DataFrame(test_file_content)
    df.to_json(file_path, index=False)
    return file_path


@pytest.fixture
def parquet_file(temp_dir) -> str:
    file_path = os.path.join(temp_dir, "test_data.parquet")
    df = pandas.DataFrame(test_file_content)
    df.to_parquet(file_path, index=False)
    return file_path


class TestIsItX:
    """Tests all of the 'is_it_x()' functions to determine a file's type"""

    def test_is_it_csv(self, csv_file, xlsx_file, json_file, parquet_file):
        assert FileHandler.is_it_csv(csv_file)
        assert not FileHandler.is_it_csv(xlsx_file)
        assert not FileHandler.is_it_csv(json_file)
        assert not FileHandler.is_it_csv(parquet_file)

    def test_is_it_xlsx(self, csv_file, xlsx_file, json_file, parquet_file):
        assert not FileHandler.is_it_xlsx(csv_file)
        assert FileHandler.is_it_xlsx(xlsx_file)
        assert not FileHandler.is_it_xlsx(json_file)
        assert not FileHandler.is_it_xlsx(parquet_file)

    def test_is_it_json(self, csv_file, xlsx_file, json_file, parquet_file):
        assert not FileHandler.is_it_json(csv_file)
        assert not FileHandler.is_it_json(xlsx_file)
        assert FileHandler.is_it_json(json_file)
        assert not FileHandler.is_it_json(parquet_file)

    def test_is_it_parquet(self, csv_file, xlsx_file, json_file, parquet_file):
        with open(csv_file, "rb") as fh:
            assert not FileHandler.is_it_parquet(BytesIO(fh.read()))
        with open(xlsx_file, "rb") as fh:
            assert not FileHandler.is_it_parquet(BytesIO(fh.read()))
        with open(json_file, "rb") as fh:
            assert not FileHandler.is_it_parquet(BytesIO(fh.read()))
        with open(parquet_file, "rb") as fh:
            assert FileHandler.is_it_parquet(BytesIO(fh.read()))


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
    assert file_format == "csv"
    assert dialect is not None


def test_query():  # (self, query: ASTNode) -> Response:
    pass


def test_native_query():  # (self, query: str) -> Response:
    pass


def test_handle_source():  # (file_path, clean_rows=True, custom_parser=None):
    pass


def test_get_data_io():  # (file_path):
    pass


def test_get_file_path():  # (path) -> str:
    pass


def test_fetch_url():  # (url: str) -> str:
    pass


def test_get_tables():  # (self) -> Response:
    pass


def test_get_columns():  # (self, table_name) -> Response:
    pass
