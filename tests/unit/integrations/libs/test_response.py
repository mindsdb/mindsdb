"""Unit tests for response classes in mindsdb.integrations.libs.response module.

This module tests all response types used by handlers:
- TableResponse: for queries that return data (SELECT, SHOW, etc.)
- OkResponse: for successful operations without data (CREATE, DROP, etc.)
- ErrorResponse: for error cases
- HandlerStatusResponse: for connection status checks
- normalize_response: for converting legacy HandlerResponse to new types
"""

import pandas as pd

from mindsdb.integrations.libs.response import (
    TableResponse,
    OkResponse,
    ErrorResponse,
    HandlerStatusResponse,
    HandlerResponse,
    normalize_response,
    RESPONSE_TYPE,
    DataHandlerResponse,
)
from mindsdb.utilities.types.column import Column
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import MYSQL_DATA_TYPE


class TestHandlerStatusResponse:
    """Tests for HandlerStatusResponse class."""

    def test_init_success(self):
        """Test initialization with success status."""
        redirect_url = "https://example.com/auth"
        copy_storage = "s3://bucket/path"
        response = HandlerStatusResponse(success=True, redirect_url=redirect_url, copy_storage=copy_storage)
        
        assert response.success is True
        assert response.error_message is None
        assert response.redirect_url == redirect_url
        assert response.copy_storage == copy_storage

        json_data = response.to_json()
        assert json_data["success"] is True
        assert json_data['error'] is None
        assert json_data["redirect_url"] == redirect_url
        assert json_data["copy_storage"] == copy_storage

    def test_init_failure(self):
        """Test initialization with failure status."""
        error_msg = "Connection failed"
        response = HandlerStatusResponse(success=False, error_message=error_msg)
        
        assert response.success is False
        assert response.error_message == error_msg
        assert response.redirect_url is None
        assert response.copy_storage is None

        json_data = response.to_json()
        assert json_data["success"] is False
        assert json_data['error'] == error_msg
        assert "redirect_url" not in json_data
        assert "copy_storage" not in json_data


class TestErrorResponse:
    """Unit tests for ErrorResponse class."""

    def test_init_basic(self):
        """Test basic initialization."""
        response = ErrorResponse(error_code=1, error_message="Test error", is_expected_error=True)
        
        assert response.type == RESPONSE_TYPE.ERROR
        assert response.resp_type == RESPONSE_TYPE.ERROR
        assert response.error_code == 1
        assert response.error_message == "Test error"
        assert response.is_expected_error is True
        assert response.exception is None
        assert isinstance(response, DataHandlerResponse)

    def test_exception_capture(self):
        """Test that exception is captured from current context."""
        try:
            raise ValueError("Test exception")
        except ValueError:
            response = ErrorResponse(error_message="Caught exception")
            assert response.exception is not None
            assert isinstance(response.exception, ValueError)


class TestOkResponse:
    """Unit tests for OkResponse class."""

    def test_init(self):
        """Test initialization with affected rows count."""
        response = OkResponse(affected_rows=5)
        
        assert response.type == RESPONSE_TYPE.OK
        assert response.resp_type == RESPONSE_TYPE.OK
        assert response.affected_rows == 5
        assert isinstance(response, DataHandlerResponse)

    def test_init_without_affected_rows(self):
        """Test initialization without affected rows."""
        response = OkResponse()
        
        assert response.affected_rows is None


class TestTableResponse:
    """Unit tests for TableResponse class."""

    def test_init_with_data(self):
        """Test initialization with DataFrame."""
        df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        response = TableResponse(data=df)

        assert response.type == RESPONSE_TYPE.TABLE
        assert response.resp_type == RESPONSE_TYPE.TABLE
        assert response._fetched is True
        pd.testing.assert_frame_equal(response._data, df)
        # 'columns' was not provided as attr, so should be as in df
        assert [c.name for c in response.columns] == ['id', 'name']

    def test_complex_init_with_generator(self):
        """Test initialization with data generator."""
        column1 = Column(name="id", type=MYSQL_DATA_TYPE.INT)
        column2 = Column(name="name", type=MYSQL_DATA_TYPE.VARCHAR)
        columns = [column1, column2]
        df = pd.DataFrame({"id": [0, 1], "name": ["a", "b"]})
        df1 = pd.DataFrame({"id": [2, 3], "name": ["d", "e"]})
        df2 = pd.DataFrame({"id": [4, 5], "name": ["f", "g"]})

        def data_gen():
            yield df1
            yield df2

        response = TableResponse(data=df, data_generator=data_gen(), columns=columns)

        assert response.columns[0] is column1
        assert response.columns[1] is column2
        assert response.data_generator is not None
        pd.testing.assert_frame_equal(response._data, df)
        assert response._fetched is False
        pieces = []
        while isinstance(el := response.fetchmany(), pd.DataFrame):
            pieces.append(el)
        pd.testing.assert_frame_equal(pieces[0], df1)
        pd.testing.assert_frame_equal(pieces[1], df2)
        pd.testing.assert_frame_equal(response._data, pd.concat([df, df1, df2]))
        assert response._fetched is True
        assert response.data_generator is None

    def test_data_frame_property(self):
        """Test initialization with explicit columns."""
        columns = [
            Column(name="id", type=MYSQL_DATA_TYPE.INT),
            Column(name="name", type=MYSQL_DATA_TYPE.VARCHAR)
        ]
        df = pd.DataFrame({"id": [0, 1], "name": ["a", "b"]})
        df1 = pd.DataFrame({"id": [2, 3], "name": ["d", "e"]})
        df2 = pd.DataFrame({"id": [4, 5], "name": ["f", "g"]})

        def data_gen():
            yield df1
            yield df2

        response = TableResponse(data=df, data_generator=data_gen(), columns=columns)
        assert response._fetched is False
        pd.testing.assert_frame_equal(response._data, df)
        pd.testing.assert_frame_equal(response.data_frame, pd.concat([df, df1, df2]))
        assert response._fetched is True

        # should not change result
        response.fetchall()
        pd.testing.assert_frame_equal(response.data_frame, pd.concat([df, df1, df2]))

    def test_init_with_affected_rows(self):
        """Test initialization with affected_rows."""
        df = pd.DataFrame({"id": [1, 2, 3]})
        response = TableResponse(data=df, affected_rows=100)
        
        assert response.affected_rows == 100

    def test_iterate_no_save_no_generator(self):
        """Test iterate_no_save yields existing data."""
        df = pd.DataFrame({"id": [1, 2, 3]})
        # Need to provide a generator (even empty) to avoid TypeError
        response = TableResponse(data=df, data_generator=iter([]))
        
        chunks = list(response.iterate_no_save())
        
        assert len(chunks) == 1
        pd.testing.assert_frame_equal(chunks[0], df)
        pd.testing.assert_frame_equal(response.data_frame, df)

    def test_iterate_no_save_with_generator(self):
        """Test iterate_no_save yields all chunks without saving."""
        df1 = pd.DataFrame({"id": [4, 5]})
        df2 = pd.DataFrame({"id": [6, 7]})
        def data_gen():
            yield df1
            yield df2
        df = pd.DataFrame({"id": [1, 2, 3]})
        response = TableResponse(data=df, data_generator=data_gen())
        chunks = list(response.iterate_no_save())
        
        assert len(chunks) == 3
        pd.testing.assert_frame_equal(chunks[0], df)
        pd.testing.assert_frame_equal(chunks[1], df1)
        pd.testing.assert_frame_equal(chunks[2], df2)
        pd.testing.assert_frame_equal(response.data_frame, df)


class TestNormalizeResponse:
    """Unit tests for normalize_response function."""

    def test_normalize_table_response(self):
        """Test that TableResponse is returned as-is."""
        original = TableResponse(data=pd.DataFrame({"id": [1, 2]}))
        result = normalize_response(original)
        
        assert result is original

    def test_normalize_ok_response(self):
        """Test that OkResponse is returned as-is."""
        original = OkResponse(affected_rows=5)
        result = normalize_response(original)
        
        assert result is original

    def test_normalize_error_response(self):
        """Test that ErrorResponse is returned as-is."""
        original = ErrorResponse(error_message="Test error")
        result = normalize_response(original)
        
        assert result is original

    def test_normalize_legacy_error_response(self):
        """Test conversion of legacy HandlerResponse with ERROR type."""
        legacy = HandlerResponse(
            resp_type=RESPONSE_TYPE.ERROR,
            error_code=1,
            error_message="Legacy error"
        )
        result = normalize_response(legacy)
        
        assert isinstance(result, ErrorResponse)
        assert result.error_code == 1
        assert result.error_message == "Legacy error"

    def test_normalize_legacy_ok_response(self):
        """Test conversion of legacy HandlerResponse with OK type."""
        legacy = HandlerResponse(
            resp_type=RESPONSE_TYPE.OK,
            affected_rows=10
        )
        result = normalize_response(legacy)
        
        assert isinstance(result, OkResponse)
        assert result.affected_rows == 10

    def test_normalize_legacy_table_response(self):
        """Test conversion of legacy HandlerResponse with TABLE type."""
        df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        legacy = HandlerResponse(
            resp_type=RESPONSE_TYPE.TABLE,
            data_frame=df
        )
        result = normalize_response(legacy)
        
        assert isinstance(result, TableResponse)
        pd.testing.assert_frame_equal(result.data_frame, df)

    def test_normalize_legacy_table_response_with_mysql_types(self):
        """Test conversion preserves mysql_types as column types."""
        df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        mysql_types = [MYSQL_DATA_TYPE.INT, MYSQL_DATA_TYPE.VARCHAR]
        legacy = HandlerResponse(
            resp_type=RESPONSE_TYPE.TABLE,
            data_frame=df,
            mysql_types=mysql_types
        )
        result = normalize_response(legacy)
        
        assert isinstance(result, TableResponse)
        assert len(result.columns) == 2
        assert result.columns[0].type == MYSQL_DATA_TYPE.INT
        assert result.columns[1].type == MYSQL_DATA_TYPE.VARCHAR

    def test_normalize_legacy_table_response_empty_dataframe(self):
        """Test conversion with empty DataFrame."""
        df = pd.DataFrame()
        legacy = HandlerResponse(
            resp_type=RESPONSE_TYPE.TABLE,
            data_frame=df
        )
        result = normalize_response(legacy)
        
        assert isinstance(result, TableResponse)
        assert len(result.columns) == 0
