import os
import shutil
import tempfile
from io import BytesIO, StringIO
from pathlib import Path

import pandas
import pytest
from mindsdb_sql_parser.exceptions import ParsingException
from mindsdb_sql_parser.ast import CreateTable, DropTables, Identifier, Insert, TableColumn, Update
from pytest_lazyfixture import lazy_fixture

from mindsdb.integrations.handlers.file_handler.file_handler import FileHandler
from mindsdb.integrations.libs.response import RESPONSE_TYPE

from mindsdb.integrations.utilities.files.file_reader import FileReader


# Define a table to use as content for all of the file types
# This data needs to match that saved in the files in the ./data/ dir (except pdf and txt files)
test_file_content = [
    ["col_one", "col_two", "col_three", "col_four"],
    [1, -1, 0.1, "A"],
    [2, -2, 0.2, "B"],
    [3, -3, 0.3, "C"],
]

test_excel_sheet_content = [
    ["Sheet_Name"],
    ["Sheet1"],
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

    def get_file_data(self, name, page_name=None):
        return pandas.DataFrame(test_file_content[1:], columns=test_file_content[0])

    def set_file_data(self, name, df, page_name=None):
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
            assert FileReader.is_csv(StringIO(fh.read())) is result

    @pytest.mark.parametrize(
        "file_path,result",
        [
            (lazy_fixture("csv_file"), 'csv'),
            (lazy_fixture("xlsx_file"), 'xlsx'),
            (lazy_fixture("json_file"), 'json'),
            (lazy_fixture("parquet_file"), 'parquet'),
            (lazy_fixture("txt_file"), 'txt'),
            (lazy_fixture("pdf_file"), 'pdf'),
        ],
    )
    def test_format(self, file_path, result):
        assert FileReader(path=file_path).get_format() == result

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
            assert FileReader.is_json(StringIO(fh.read())) is result

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
            assert FileReader.is_parquet(BytesIO(fh.read())) is result


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

    def get_reader(file_path):
        # using path
        reader = FileReader(path=file_path)
        yield reader

        # using file descriptor
        with open(file_path, 'rb') as fd:
            reader = FileReader(file=fd)
            yield reader
            fd.seek(0)
            content = fd.read()

        # using bytesio
        fd = BytesIO(content)
        reader = FileReader(file=fd, name=Path(file_path).name)
        yield reader

    # using different methods to create reader
    for reader in get_reader(file_path):
        df = reader.get_page_content()
        assert isinstance(df, pandas.DataFrame)

        assert df.columns.tolist() == expected_columns

        # The pdf and txt files have some different content
        if reader.get_format() not in ("pdf", "txt"):
            assert len(df) == len(test_file_content) - 1
            assert df.values.tolist() == test_file_content[1:]


@pytest.mark.parametrize(
    "csv_string,delimiter",
    [
        (StringIO("example,csv,file"), ","),
        (StringIO("example;csv;file"), ";"),
        (StringIO("example\tcsv\tfile"), "\t"),
    ],
)
def test_check_valid_dialects(csv_string, delimiter):
    dialect = FileReader._get_csv_dialect(csv_string)
    assert dialect.delimiter == delimiter


def test_tsv():
    file = BytesIO(b"example;csv;file\tname")

    reader = FileReader(file=file, name='test.tsv')
    assert reader.get_format() == 'csv'
    assert reader.parameters['delimiter'] == '\t'

    df = reader.get_page_content()
    assert len(df.columns) == 2


def test_check_invalid_dialects():
    with pytest.raises(Exception):
        FileHandler._get_csv_dialect("example csv file")
    with pytest.raises(Exception):
        FileHandler._get_csv_dialect("example\ncsv\nfile")
    with pytest.raises(Exception):
        FileHandler._get_csv_dialect("example|csv|file")


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
