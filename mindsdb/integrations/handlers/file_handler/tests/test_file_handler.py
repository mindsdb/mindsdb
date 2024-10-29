import json
import os
import shutil
import tempfile
from io import BytesIO, StringIO
from unittest.mock import patch

import pandas
import pytest
import responses
from mindsdb_sql.exceptions import ParsingException
from mindsdb_sql.parser.ast import CreateTable, DropTables, Identifier, Insert, Select, Star, TableColumn, Update
from pytest_lazyfixture import lazy_fixture

from mindsdb.integrations.handlers.file_handler.file_handler import FileHandler
from mindsdb.integrations.libs.response import RESPONSE_TYPE
from mindsdb.interfaces.file.file_controller import FileController

# Define a table to use as content for all of the file types
# This data needs to match that saved in the files in the ./data/ dir (except pdf and txt files)
test_file_content = [
    ["col_one", "col_two", "col_three", "col_four"],
    [1, -1, 0.1, "A"],
    [2, -2, 0.2, "B"],
    [3, -3, 0.3, "C"],
]

file_records = [("one", 1, test_file_content[0]), ("two", 2, test_file_content[0])]


class MockFileController:
    """
    Pretends to be a file controller. Gives details of 'file_records' above, and mocks file deletion.
    We're not testing the file controller here, so we don't need to rely on it.
    """

    def get_files(self):
        return [
            {
                "name": record[0],
                "row_count": record[1],
                "columns": record[2],
            }
            for record in file_records
        ]

    def get_files_names(self):
        return [file["name"] for file in self.get_files()]

    def get_file_path(self, name):
        return True

    def get_file_meta(self, *args, **kwargs):
        return self.get_files()[0]

    def delete_file(self, name):
        return True

    def save_file(self, name, file_path, file_name=None):
        return True


def curr_dir():
    return os.path.dirname(os.path.realpath(__file__))


# Fixtures to get a path to a partiular type of file
@pytest.fixture
def csv_file() -> str:
    return os.path.join(curr_dir(), "data", "test.csv")


@pytest.fixture
def xlsx_file() -> str:
    return os.path.join(curr_dir(), "data", "test.xlsx")


@pytest.fixture
def json_file() -> str:
    return os.path.join(curr_dir(), "data", "test.json")


@pytest.fixture
def parquet_file() -> str:
    return os.path.join(curr_dir(), "data", "test.parquet")


@pytest.fixture
def pdf_file() -> str:
    return os.path.join(curr_dir(), "data", "test.pdf")


@pytest.fixture
def txt_file() -> str:
    return os.path.join(curr_dir(), "data", "test.txt")


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
            (lazy_fixture("txt_file"), False),
            (lazy_fixture("pdf_file"), False),
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
            (lazy_fixture("txt_file"), False),
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
            (lazy_fixture("txt_file"), False),
            (lazy_fixture("pdf_file"), False),
        ],
    )
    def test_is_it_parquet(self, file_path, result):
        with open(file_path, "rb") as fh:
            assert FileHandler.is_it_parquet(BytesIO(fh.read())) is result


class TestQuery:
    """Tests all of the scenarios relating to the query() function"""

    def test_query_drop(self, monkeypatch):
        """Test a valid drop table query"""

        def mock_delete(self, name):
            return True

        monkeypatch.setattr(MockFileController, "delete_file", mock_delete)
        file_handler = FileHandler(file_controller=MockFileController())
        response = file_handler.query(DropTables([Identifier(parts=["one"])]))

        assert response.type == RESPONSE_TYPE.OK

    def test_query_drop_bad_delete(self, monkeypatch):
        """Test an invalid drop table query"""

        def mock_delete(self, name):
            raise Exception("File delete error")

        monkeypatch.setattr(MockFileController, "delete_file", mock_delete)
        file_handler = FileHandler(file_controller=MockFileController())
        response = file_handler.query(DropTables([Identifier(parts=["one"])]))

        assert response.type == RESPONSE_TYPE.ERROR

    def test_query_select(self, csv_file):
        """Test a valid select query"""
        expected_df = pandas.read_csv(csv_file)

        # This is temporary because the file controller currently absconds with our file when we save it:
        # https://github.com/mindsdb/mindsdb/issues/8141
        csv_tmp = os.path.join(tempfile.gettempdir(), "test.csv")
        if os.path.exists(csv_tmp):
            os.remove(csv_tmp)
        shutil.copy(csv_file, csv_tmp)

        # Configure mindsdb and set up the file controller
        # Ideally this would be a lot simpler..
        db_file = tempfile.mkstemp(prefix="mindsdb_db_")[1]
        config = {"storage_db": "sqlite:///" + db_file}
        fdi, cfg_file = tempfile.mkstemp(prefix="mindsdb_conf_")
        with os.fdopen(fdi, "w") as fd:
            json.dump(config, fd)
        os.environ["MINDSDB_CONFIG_PATH"] = cfg_file

        from mindsdb.utilities.config import Config

        Config()
        from mindsdb.interfaces.storage import db

        db.init()
        db.session.rollback()
        db.Base.metadata.drop_all(db.engine)

        # create
        db.Base.metadata.create_all(db.engine)

        # fill with data
        r = db.Integration(name="files", data={}, engine="files")
        db.session.add(r)
        db.session.flush()
        # Config #

        file_controller = FileController()
        file_controller.save_file(
            os.path.splitext(os.path.basename(csv_file))[0], csv_tmp
        )

        file_handler = FileHandler(file_controller=file_controller)
        response = file_handler.query(
            Select(
                targets=[Star()],
                from_table=Identifier(
                    parts=[os.path.splitext(os.path.basename(csv_file))[0]]
                ),
            )
        )

        assert response.type == RESPONSE_TYPE.TABLE
        assert response.error_code == 0
        assert response.error_message is None
        assert expected_df.equals(response.data_frame)

    def test_query_insert(self, csv_file, monkeypatch):
        """Test an invalid insert query"""
        # Create a temporary file to save the csv file to.
        csv_tmp = os.path.join(tempfile.gettempdir(), "test.csv")
        if os.path.exists(csv_tmp):
            os.remove(csv_tmp)
        shutil.copy(csv_file, csv_tmp)

        def mock_get_file_path(self, name):
            return csv_tmp
        monkeypatch.setattr(MockFileController, "get_file_path", mock_get_file_path)

        file_handler = FileHandler(file_controller=MockFileController())
        response = file_handler.query(
            Insert(
                table=Identifier(parts=["someTable"]),
                columns=[
                    "col_one",
                    "col_two",
                    "col_three",
                    "col_four",
                ],
                values=[
                    [1, -1, 0.1, "A"],
                    [2, -2, 0.2, "B"],
                    [3, -3, 0.3, "C"],
                ],
            )
        )

        assert response.type == RESPONSE_TYPE.OK

    def test_query_create(self):
        """Test a valid create table query"""
        file_handler = FileHandler(file_controller=MockFileController())
        response = file_handler.query(
            CreateTable(
                name=Identifier(parts=["someTable"]),
                columns=[TableColumn(name="col1"), TableColumn(name="col2")],
            )
        )

        assert response.type == RESPONSE_TYPE.OK

    def test_query_create_or_replace(self):
        """Test a valid create or replace table query"""
        file_handler = FileHandler(file_controller=MockFileController())
        response = file_handler.query(
            CreateTable(
                name=Identifier(parts=["someTable"]),
                columns=[TableColumn(name="col1"), TableColumn(name="col2")],
                is_replace=True,
            )
        )

        assert response.type == RESPONSE_TYPE.OK

    def test_query_bad_type(self):
        """Test an invalid query type for files"""
        file_handler = FileHandler(file_controller=MockFileController())
        response = file_handler.query(Update([Identifier(parts=["someTable"])]))

        assert response.type == RESPONSE_TYPE.ERROR

    def test_native_query(self, monkeypatch):
        """Test a valid native table query"""

        def mock_delete(self, name):
            return True

        monkeypatch.setattr(MockFileController, "delete_file", mock_delete)
        file_handler = FileHandler(file_controller=MockFileController())
        response = file_handler.native_query("DROP TABLE one")

        assert response.type == RESPONSE_TYPE.OK

    def test_invalid_native_query(self):
        file_handler = FileHandler(file_controller=MockFileController())
        with pytest.raises(ParsingException):
            file_handler.native_query("INVALID QUERY")


def test_get_file_path_with_file_path():
    """Test an valid native table query"""
    file_path = "example.txt"
    result = FileHandler._get_file_path(file_path)
    assert result == file_path


@patch("mindsdb.integrations.handlers.file_handler.file_handler.FileHandler._fetch_url")
def test_get_file_path_with_url(mock_fetch_url):
    url = "http://example.com/file.txt"
    expected_result = "some_file_path"
    # we test _fetch_url separately below. Mock it for this test
    mock_fetch_url.return_value = expected_result

    result = FileHandler._get_file_path(url)

    assert result == expected_result
    mock_fetch_url.assert_called_with(url)


@pytest.mark.parametrize(
    "file_path,expected_columns",
    [
        (lazy_fixture("csv_file"), test_file_content[0]),
        (lazy_fixture("xlsx_file"), test_file_content[0]),
        (lazy_fixture("json_file"), test_file_content[0]),
        (lazy_fixture("parquet_file"), test_file_content[0]),
        (lazy_fixture("pdf_file"), ["content", "metadata"]),
        (lazy_fixture("txt_file"), ["content", "metadata"]),
    ],
)
def test_handle_source(file_path, expected_columns):
    df, col_map = FileHandler._handle_source(file_path)
    assert isinstance(df, pandas.DataFrame)
    assert df.columns.tolist() == expected_columns

    # The pdf and txt files have some different content
    if not file_path.endswith(".pdf") and not file_path.endswith(".txt"):
        assert len(df) == len(test_file_content) - 1
        assert df.values.tolist() == test_file_content[1:]


@pytest.mark.parametrize(
    "file_path,expected_file_type,expected_delimiter,expected_data_type",
    [
        (lazy_fixture("csv_file"), "csv", ",", StringIO),
        (lazy_fixture("xlsx_file"), "xlsx", None, BytesIO),
        (lazy_fixture("json_file"), "json", None, StringIO),
        (lazy_fixture("parquet_file"), "parquet", None, BytesIO),
        (lazy_fixture("pdf_file"), "pdf", None, BytesIO),
        (lazy_fixture("txt_file"), "txt", None, BytesIO),
    ],
)
def test_get_data_io(
    file_path, expected_file_type, expected_delimiter, expected_data_type
):
    data_io, file_type, file_dialect = FileHandler._get_data_io(file_path)
    assert file_type == expected_file_type
    assert type(data_io) == expected_data_type
    if expected_delimiter is None:
        assert file_dialect is None
    else:
        assert file_dialect.delimiter == expected_delimiter


@pytest.mark.parametrize(
    "csv_string,delimiter",
    [
        (StringIO("example,csv,file"), ","),
        (StringIO("example;csv;file"), ";"),
        (StringIO("example\tcsv\tfile"), "\t"),
    ],
)
def test_check_valid_dialects(csv_string, delimiter):
    dialect = FileHandler._get_csv_dialect(csv_string)
    assert dialect.delimiter == delimiter


def test_check_invalid_dialects():
    with pytest.raises(Exception):
        FileHandler._get_csv_dialect("example csv file")
    with pytest.raises(Exception):
        FileHandler._get_csv_dialect("example\ncsv\nfile")
    with pytest.raises(Exception):
        FileHandler._get_csv_dialect("example|csv|file")


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
