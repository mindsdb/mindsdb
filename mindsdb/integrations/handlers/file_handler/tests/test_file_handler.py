import json
import os
import tempfile
from io import BytesIO, StringIO
from unittest.mock import patch, Mock


import pandas
import pandas as pd
import pytest
import responses
from mindsdb_sql.parser.ast import CreateTable, DropTables, Identifier, Select, Star
from pytest_lazyfixture import lazy_fixture
import PyPDF2
import pyarrow.parquet as pq

from mindsdb.integrations.handlers.file_handler.file_handler import FileHandler
from mindsdb.integrations.libs.response import RESPONSE_TYPE
from mindsdb.interfaces.file.file_controller import FileController

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


@pytest.fixture
def csv_file() -> str:
    curr_dir = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(curr_dir, "data", "test.csv")


@pytest.fixture
def xlsx_file() -> str:
    curr_dir = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(curr_dir, "data", "test.xlsx")


@pytest.fixture
def json_file() -> str:
    curr_dir = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(curr_dir, "data", "test.json")


@pytest.fixture
def parquet_file() -> str:
    curr_dir = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(curr_dir, "data", "test.parquet")


@pytest.fixture
def pdf_file() -> str:
    curr_dir = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(curr_dir, "data", "test.pdf")


@pytest.fixture
def txt_file() -> str:
    curr_dir = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(curr_dir, "data", "test.txt")


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


class TestQuery:
    """Tests all of the scenarios relating to the query() function"""

    def test_query_drop(self, monkeypatch):
        def mock_delete(self, name):
            return True

        monkeypatch.setattr(MockFileController, "delete_file", mock_delete)
        file_handler = FileHandler(file_controller=MockFileController())
        response = file_handler.query(DropTables([Identifier(parts=["one"])]))

        assert response.type == RESPONSE_TYPE.OK

    def test_query_drop_bad_delete(self, monkeypatch):
        def mock_delete(self, name):
            raise Exception("File delete error")

        monkeypatch.setattr(MockFileController, "delete_file", mock_delete)
        file_handler = FileHandler(file_controller=MockFileController())
        response = file_handler.query(DropTables([Identifier(parts=["one"])]))

        assert response.type == RESPONSE_TYPE.ERROR

    def test_query_select(self, csv_file):
        db_file = tempfile.mkstemp(prefix="mindsdb_db_")[1]
        config = {
            "storage_db": "sqlite:///" + db_file,
            "paths": {"content": os.path.dirname(os.path.dirname(csv_file))},
        }
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

        file_handler = FileHandler(file_controller=FileController())
        file_handler.query(
            Select(
                targets=[Star()],
                from_table=Identifier(parts=[os.path.basename(csv_file)]),
            )
        )

        assert False

    def test_query_bad_type(self):
        file_handler = FileHandler(file_controller=MockFileController())
        response = file_handler.query(CreateTable([Identifier(parts=["someTable"])]))

        assert response.type == RESPONSE_TYPE.ERROR


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


def test_handle_source(
    csv_file, xlsx_file, json_file, parquet_file, pdf_file, txt_file
):
    supported_file_types = [".csv", ".json", ".parquet", ".pdf", ".txt", ".xlsx"]

    file_paths = [csv_file, xlsx_file, json_file, parquet_file, pdf_file, txt_file]

    for file_path in file_paths:
        # Extract the file extension from the file_path
        file_extension = os.path.splitext(file_path)[1].lower()

        # Check if the file extension is supported
        assert (
            file_extension in supported_file_types
        ), f"Unsupported file type: {file_extension}"

        if file_extension == ".csv":
            # Attempt to read the .csv file using Pandas
            try:
                df = pd.read_csv(file_path)
                assert not df.empty, ".csv file is empty."
                assert len(df) >= 0, "Unexpected number of rows."

            except Exception as e:
                assert False, f"Error reading .csv file: {str(e)}"

        elif file_extension == ".json":
            try:
                with open(file_path, "r") as json_file:
                    json_data = json.load(json_file)
                assert isinstance(json_data, dict), "JSON data is not a dictionary."

            except Exception as e:
                assert False, f"Error reading JSON file: {str(e)}"

        elif file_extension == ".parquet":
            try:
                table = pq.read_table(file_path)
                assert table.num_rows > 0, "Parquet file is empty."
                assert len(table) >= 0, "Unexpected number of rows."

            except Exception as e:
                assert False, f"Error reading Parquet file: {str(e)}"

        elif file_extension == ".xlsx":
            try:
                df = pd.read_excel(
                    file_path, sheet_name="Sheet1"
                )  # Adjust sheet_name as needed
                assert not df.empty, "XLSX file is empty."
                assert len(df) >= 0, "Unexpected number of rows."

            except Exception as e:
                assert False, f"Error reading XLSX file: {str(e)}"

        elif file_extension == ".pdf":
            with open(file_path, "rb") as pdf_file:
                pdf_reader = PyPDF2.PdfReader(pdf_file)
                pdf_text = ""
                for page_num in range(len(pdf_reader.pages)):
                    pdf_text += pdf_reader.pages[page_num].extract_text()
                assert pdf_text.strip() != "", "PDF text extraction failed."

        elif file_extension == ".txt":
            with open(file_path, "r") as text_file:
                file_contents = text_file.read()
            assert file_contents.strip() != "", "Text file is empty."


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
