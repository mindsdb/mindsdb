import io
import os
import tempfile
from io import BytesIO, StringIO
from unittest.mock import patch

import pandas
import pytest
import responses
from mindsdb_sql.parser.ast.drop import DropTables
from mindsdb_sql.parser.ast.select import Identifier
from pytest_lazyfixture import lazy_fixture

from mindsdb.integrations.handlers.file_handler.file_handler import FileHandler
from mindsdb.integrations.libs.response import RESPONSE_TYPE, HandlerResponse
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse

# Define a table to use as content for all of the file types
test_file_content = [
    ["col_one", "col_two", "col_three", "col_four"],
    [1, -1, 0.1, "A"],
    [2, -2, 0.2, "B"],
    [3, -3, 0.3, "C"],
]

test_dataframe = pandas.DataFrame(test_file_content[1:], columns=test_file_content[0])

file_records = [("one", 1, test_file_content[0]), ("two", 2, test_file_content[0])]


class MockFileController:
    def get_files(self):
        return [
            {
                "name": record[0],
                "row_count": record[1],
                "columns": record[2],
            }
            for record in file_records
        ]

    def get_file_meta(self, *args, **kwargs):
        return self.get_files()[0]

    def delete_file(self, name):
        return True


@pytest.fixture()
def temp_dir():
    return tempfile.mkdtemp(prefix="test_file_handler_")


@pytest.fixture
def csv_file(temp_dir) -> str:
    file_path = os.path.join(temp_dir, "test_data.csv")
    test_dataframe.to_csv(file_path, index=False)
    return file_path


@pytest.fixture
def xlsx_file(temp_dir) -> str:
    file_path = os.path.join(temp_dir, "test_data.xlsx")
    test_dataframe.to_excel(file_path, index=False)
    print(file_path)
    return file_path


@pytest.fixture
def json_file(temp_dir) -> str:
    file_path = os.path.join(temp_dir, "test_data.json")
    test_dataframe.to_json(file_path)
    print(file_path)
    return file_path


@pytest.fixture
def parquet_file(temp_dir) -> str:
    file_path = os.path.join(temp_dir, "test_data.parquet")
    df = pandas.DataFrame(test_file_content[1:], columns=test_file_content[0]).astype(
        str
    )
    df.to_parquet(file_path)
    return file_path


class TestIsItX:
    """Tests all of the 'is_it_x()' functions to determine a file's type"""

    # We can't test xlsx or parquet here because they're binary files
    @pytest.mark.parametrize(
        "file_path,result",
        [(lazy_fixture("csv_file"), True), (lazy_fixture("json_file"), False)],
    )
    def test_is_it_csv(self, file_path, result):
        with open(file_path, "r") as fh:
            assert FileHandler.is_it_csv(StringIO(fh.read())) is result

    @pytest.mark.parametrize(
        "file_path,result",
        [
            (lazy_fixture("csv_file"), False),
            (lazy_fixture("xlsx_file"), True),
            (lazy_fixture("json_file"), False),
            (lazy_fixture("parquet_file"), False),
        ],
    )
    def test_is_it_xlsx(self, file_path, result):
        assert FileHandler.is_it_xlsx(file_path) is result

    # We can't test xlsx or parquet here because they're binary files
    @pytest.mark.parametrize(
        "file_path,result",
        [
            (lazy_fixture("csv_file"), False),
            (lazy_fixture("json_file"), True),
        ],
    )
    def test_is_it_json(self, file_path, result):
        with open(file_path, "r") as fh:
            assert FileHandler.is_it_json(StringIO(fh.read())) is result

    @pytest.mark.parametrize(
        "file_path,result",
        [
            (lazy_fixture("csv_file"), False),
            (lazy_fixture("xlsx_file"), False),
            (lazy_fixture("json_file"), False),
            (lazy_fixture("parquet_file"), True),
        ],
    )
    def test_is_it_parquet(self, file_path, result):
        with open(file_path, "rb") as fh:
            assert FileHandler.is_it_parquet(BytesIO(fh.read())) is result


def test_get_file_path_with_file_path():
    file_path = "example.txt"
    result = FileHandler._get_file_path(file_path)
    assert result == file_path


@patch("mindsdb.integrations.handlers.file_handler.file_handler.FileHandler._fetch_url")
def test_get_file_path_with_url(mock_fetch_url):
    mock_url = "http://example.com/file.txt"
    expected_result = "http://example.com/file.txt"
    mock_fetch_url.return_value = mock_url

    result = FileHandler._get_file_path(mock_url)

    assert result == expected_result
    mock_fetch_url.assert_called_with(mock_url)


class TestHandleSource:
    def test_handle_source_csv(self, csv_file):
        df, col_map = FileHandler._handle_source(csv_file)
        assert isinstance(df, pandas.DataFrame)
        assert len(df) >= 0
        assert df.columns.tolist() == ["col_one", "col_two", "col_three", "col_four"]

    def test_handle_source_xlsx(self, xlsx_file):
        df, col_map = FileHandler._handle_source(xlsx_file)
        assert isinstance(df, pandas.DataFrame)
        assert len(df) >= 0
        assert df.columns.tolist() == ["col_one", "col_two", "col_three", "col_four"]

    def test_handle_source_json(self, json_file):
        df, col_map = FileHandler._handle_source(json_file)
        assert isinstance(df, pandas.DataFrame)
        assert len(df) >= 0  # Assuming the JSON file contains 2 records
        assert df.columns.tolist() == ["col_one", "col_two", "col_three", "col_four"]

    # TODO: not finished
    def test_handle_source_parquet(self, parquet_file):
        df, col_map = FileHandler._handle_source(parquet_file)
        assert isinstance(df, pandas.DataFrame)
        # Add assertions specific to the Parquet file format


def test_check_valid_dialect_coma(csv_file_path: str):
    with open(csv_file_path, "rb") as csv_file:
        csv_data = csv_file.read()
    # Create a file-like object from the bytes data
    csv_data_filelike = io.BytesIO(csv_data)
    dialect = FileHandler._get_csv_dialect(csv_data_filelike)
    assert dialect.delimiter == ","


def test_get_data_io_csv(csv_file_path: str):
    data_io, file_format, dialect = FileHandler._get_data_io(csv_file_path)
    assert file_format == "csv"
    assert dialect is not None


def test_query_drop(monkeypatch):
    def mock_delete():
        return True

    monkeypatch.setattr(MockFileController, "delete_file", mock_delete)
    file_controller = MockFileController()
    file_handler = FileHandler(file_controller=MockFileController())
    response = file_handler.query(DropTables([Identifier(parts=["one"])]))

    assert response.type == RESPONSE_TYPE.OK


def test_query_drop_bad_delete(monkeypatch):
    def mock_delete():
        return False

    monkeypatch.setattr(MockFileController, "delete_file", mock_delete)
    file_controller = MockFileController()
    file_handler = FileHandler(file_controller=MockFileController())
    response = file_handler.query(DropTables([Identifier(parts=["one"])]))

    assert response.type == RESPONSE_TYPE.ERROR


# TODO
def test_query_drop_bad_table():
    pass


def test_query_select():
    pass


def test_query_bad_type():  # a query that's not drop or select
    pass


@responses.activate
def test_fetch_url():
    file_content = "Fake File Content 1234567890"
    file_url = "https://test.fake/robots.txt"
    responses.add(
        responses.GET, file_url, body=file_content, status=200
    )  # mock the response

    file_path = FileHandler._fetch_url(file_url)
    with open(file_path, "r") as fh:
        saved_file_content = fh.read()

    assert saved_file_content == file_content


@responses.activate
def test_fetch_url_raises():
    responses.add(responses.GET, "https://google.com", status=404)

    with pytest.raises(Exception):
        FileHandler._fetch_url("obvious_broken_url")
    with pytest.raises(Exception):
        FileHandler._fetch_url("https://google.com")  # will get 404 response


def test_get_tables():
    file_handler = FileHandler(file_controller=MockFileController())
    response = file_handler.get_tables()

    assert response.type == RESPONSE_TYPE.TABLE

    expected_df = pandas.DataFrame(
        [
            {"TABLE_NAME": x[0], "TABLE_ROWS": x[1], "TABLE_TYPE": "BASE TABLE"}
            for x in file_records
        ]
    )

    assert response.data_frame.equals(expected_df)


def test_get_columns():
    file_handler = FileHandler(file_controller=MockFileController())
    response = file_handler.get_columns("mock")

    assert response.type == RESPONSE_TYPE.TABLE

    expected_df = pandas.DataFrame(
        [{"Field": x, "Type": "str"} for x in file_records[0][2]]
    )

    assert response.data_frame.equals(expected_df)
