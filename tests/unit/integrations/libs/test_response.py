"""Unit tests for response classes in mindsdb.integrations.libs.response module.

This module tests all response types used by handlers:
- TableResponse: for queries that return data (SELECT, SHOW, etc.)
- OkResponse: for successful operations without data (CREATE, DROP, etc.)
- ErrorResponse: for error cases
- HandlerStatusResponse: for connection status checks
- normalize_response: for converting legacy HandlerResponse to new types
- _safe_pandas_concat: memory-safe DataFrame concatenation
"""

from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from mindsdb.integrations.libs.response import (
    TableResponse,
    OkResponse,
    ErrorResponse,
    HandlerStatusResponse,
    HandlerResponse,
    normalize_response,
    _safe_pandas_concat,
    RESPONSE_TYPE,
    DataHandlerResponse,
)
from mindsdb.utilities.types.column import Column
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import MYSQL_DATA_TYPE


def _mock_virtual_memory(available_kb: int):
    """Create a mock for psutil.virtual_memory() with given available memory in KB."""
    mock_mem = MagicMock()
    mock_mem.available = available_kb << 10  # convert KB back to bytes
    return mock_mem


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
        assert json_data["error"] is None
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
        assert json_data["error"] == error_msg
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
        assert [c.name for c in response.columns] == ["id", "name"]

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
        columns = [Column(name="id", type=MYSQL_DATA_TYPE.INT), Column(name="name", type=MYSQL_DATA_TYPE.VARCHAR)]
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

        # after `iterate_no_save` result should be invalid
        with pytest.raises(ValueError):
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

        # after `iterate_no_save` result should be invalid
        with pytest.raises(ValueError):
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
        legacy = HandlerResponse(resp_type=RESPONSE_TYPE.ERROR, error_code=1, error_message="Legacy error")
        result = normalize_response(legacy)

        assert isinstance(result, ErrorResponse)
        assert result.error_code == 1
        assert result.error_message == "Legacy error"

    def test_normalize_legacy_ok_response(self):
        """Test conversion of legacy HandlerResponse with OK type."""
        legacy = HandlerResponse(resp_type=RESPONSE_TYPE.OK, affected_rows=10)
        result = normalize_response(legacy)

        assert isinstance(result, OkResponse)
        assert result.affected_rows == 10

    def test_normalize_legacy_table_response(self):
        """Test conversion of legacy HandlerResponse with TABLE type."""
        df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        legacy = HandlerResponse(resp_type=RESPONSE_TYPE.TABLE, data_frame=df)
        result = normalize_response(legacy)

        assert isinstance(result, TableResponse)
        pd.testing.assert_frame_equal(result.data_frame, df)

    def test_normalize_legacy_table_response_with_mysql_types(self):
        """Test conversion preserves mysql_types as column types."""
        df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        mysql_types = [MYSQL_DATA_TYPE.INT, MYSQL_DATA_TYPE.VARCHAR]
        legacy = HandlerResponse(resp_type=RESPONSE_TYPE.TABLE, data_frame=df, mysql_types=mysql_types)
        result = normalize_response(legacy)

        assert isinstance(result, TableResponse)
        assert len(result.columns) == 2
        assert result.columns[0].type == MYSQL_DATA_TYPE.INT
        assert result.columns[1].type == MYSQL_DATA_TYPE.VARCHAR

    def test_normalize_legacy_table_response_empty_dataframe(self):
        """Test conversion with empty DataFrame."""
        df = pd.DataFrame()
        legacy = HandlerResponse(resp_type=RESPONSE_TYPE.TABLE, data_frame=df)
        result = normalize_response(legacy)

        assert isinstance(result, TableResponse)
        assert len(result.columns) == 0


class TestSafePandasConcat:
    """Unit tests for _safe_pandas_concat function."""

    @patch("mindsdb.integrations.libs.response.psutil")
    def test_concat_with_enough_memory(self, mock_psutil):
        """Test successful concatenation when sufficient memory is available."""
        mock_psutil.virtual_memory.return_value = _mock_virtual_memory(available_kb=1_000_000)

        df1 = pd.DataFrame({"id": [1, 2]})
        df2 = pd.DataFrame({"id": [3, 4]})
        result = _safe_pandas_concat([df1, df2])

        pd.testing.assert_frame_equal(result, pd.concat([df1, df2]))

    @patch("mindsdb.integrations.libs.response.psutil")
    def test_concat_raises_memory_error_when_not_enough_memory(self, mock_psutil):
        """Test MemoryError is raised when available memory is too low."""
        # Set available memory to essentially 0
        mock_psutil.virtual_memory.return_value = _mock_virtual_memory(available_kb=10)

        df1 = pd.DataFrame({"x": list(range(1000))})
        df2 = pd.DataFrame({"x": list(range(1000))})

        with pytest.raises(MemoryError):
            _safe_pandas_concat([df1, df2])

    @patch("mindsdb.integrations.libs.response.psutil")
    def test_concat_single_piece(self, mock_psutil):
        """Test concatenation with a single DataFrame."""
        mock_psutil.virtual_memory.return_value = _mock_virtual_memory(available_kb=1_000_000)

        df = pd.DataFrame({"id": [1, 2, 3]})
        result = _safe_pandas_concat([df])

        pd.testing.assert_frame_equal(result, df)


class TestRaiseIfLowMemory:
    """Unit tests for TableResponse._raise_if_low_memory method."""

    @patch("mindsdb.integrations.libs.response.psutil")
    def test_with_known_affected_rows_enough_memory(self, mock_psutil):
        """Test no error when affected_rows is known and memory is sufficient."""
        mock_psutil.virtual_memory.return_value = _mock_virtual_memory(available_kb=1_000_000)

        response = TableResponse(data=pd.DataFrame({"id": [1, 2]}), affected_rows=100)
        response._last_data_piece = pd.DataFrame({"id": list(range(10))})
        response.rows_fetched = 10

        # Should not raise
        response._raise_if_low_memory()

    @patch("mindsdb.integrations.libs.response.psutil")
    def test_with_known_affected_rows_not_enough_memory(self, mock_psutil):
        """Test MemoryError when affected_rows is known and memory is insufficient."""
        mock_psutil.virtual_memory.return_value = _mock_virtual_memory(available_kb=1)

        # Use strings to ensure DataFrame memory > 1KB after >> 10
        large_piece = pd.DataFrame({"text": ["x" * 200 for _ in range(100)]})
        response = TableResponse(data=pd.DataFrame({"text": ["a"]}), affected_rows=1000)
        response._last_data_piece = large_piece
        response.rows_fetched = 100

        with pytest.raises(MemoryError, match="Not enough memory"):
            response._raise_if_low_memory()

    @patch("mindsdb.integrations.libs.response.psutil")
    def test_with_unknown_affected_rows_enough_memory(self, mock_psutil):
        """Test no error when affected_rows is None and memory is sufficient."""
        mock_psutil.virtual_memory.return_value = _mock_virtual_memory(available_kb=1_000_000)

        response = TableResponse(data=pd.DataFrame({"id": [1, 2]}))
        response._last_data_piece = pd.DataFrame({"id": list(range(10))})

        # Should not raise
        response._raise_if_low_memory()

    @patch("mindsdb.integrations.libs.response.psutil")
    def test_with_unknown_affected_rows_not_enough_memory(self, mock_psutil):
        """Test MemoryError when affected_rows is None and memory is insufficient."""
        mock_psutil.virtual_memory.return_value = _mock_virtual_memory(available_kb=1)

        # Use strings to ensure DataFrame memory > 1KB after >> 10
        large_piece = pd.DataFrame({"text": ["x" * 200 for _ in range(100)]})
        response = TableResponse(data=pd.DataFrame({"text": ["a"]}))
        response._last_data_piece = large_piece

        with pytest.raises(MemoryError, match="Not enough memory"):
            response._raise_if_low_memory()

    @patch("mindsdb.integrations.libs.response.psutil")
    def test_all_rows_already_fetched(self, mock_psutil):
        """Test no error when all rows have been fetched (rows_expected = 0)."""
        mock_psutil.virtual_memory.return_value = _mock_virtual_memory(available_kb=0)

        response = TableResponse(data=pd.DataFrame({"id": [1, 2]}), affected_rows=10)
        response._last_data_piece = pd.DataFrame({"id": list(range(10))})
        response.rows_fetched = 10  # all rows fetched

        # rows_expected = min(10 - 10, 10) = 0, should not raise
        response._raise_if_low_memory()


class TestIterateWithMemoryCheck:
    """Unit tests for TableResponse._iterate_with_memory_check method."""

    def test_none_generator_yields_nothing(self):
        """Test that no chunks are yielded when data_generator is None."""
        response = TableResponse(data=pd.DataFrame({"id": [1]}))
        assert response._data_generator is None

        chunks = list(response._iterate_with_memory_check())
        assert chunks == []

    @patch("mindsdb.integrations.libs.response.psutil")
    def test_normal_iteration(self, mock_psutil):
        """Test that all chunks are yielded during normal iteration."""
        mock_psutil.virtual_memory.return_value = _mock_virtual_memory(available_kb=1_000_000)

        df1 = pd.DataFrame({"id": [1, 2]})
        df2 = pd.DataFrame({"id": [3, 4]})

        def data_gen():
            yield df1
            yield df2

        columns = [Column(name="id")]
        response = TableResponse(data_generator=data_gen(), columns=columns)

        chunks = list(response._iterate_with_memory_check())

        assert len(chunks) == 2
        pd.testing.assert_frame_equal(chunks[0], df1)
        pd.testing.assert_frame_equal(chunks[1], df2)

    @patch("mindsdb.integrations.libs.response.psutil")
    def test_memory_error_stops_iteration_after_first_chunk(self, mock_psutil):
        """Test that MemoryError is raised after the first chunk when memory runs out.

        The pre-loop _raise_if_low_memory() is a no-op (since _last_data_piece is None),
        so the first real psutil.virtual_memory() call happens at the post-yield check.
        """
        # Use strings to ensure DataFrame memory > 1KB after >> 10
        df1 = pd.DataFrame({"text": ["x" * 200 for _ in range(100)]})
        df2 = pd.DataFrame({"text": ["y" * 200 for _ in range(100)]})

        def data_gen():
            yield df1
            yield df2

        columns = [Column(name="text")]
        response = TableResponse(data_generator=data_gen(), columns=columns)

        gen = response._iterate_with_memory_check()

        # First chunk succeeds — post-yield check will be the first real psutil call
        mock_psutil.virtual_memory.return_value = _mock_virtual_memory(available_kb=1)
        first = next(gen)
        pd.testing.assert_frame_equal(first, df1)

        # Resuming the generator triggers _raise_if_low_memory with 0 available memory
        with pytest.raises(MemoryError):
            next(gen)

    @patch("mindsdb.integrations.libs.response.psutil")
    def test_updates_last_data_piece_and_rows_fetched(self, mock_psutil):
        """Test that _last_data_piece and rows_fetched are updated during iteration."""
        mock_psutil.virtual_memory.return_value = _mock_virtual_memory(available_kb=1_000_000)

        df1 = pd.DataFrame({"id": [1, 2, 3]})
        df2 = pd.DataFrame({"id": [4, 5]})

        def data_gen():
            yield df1
            yield df2

        columns = [Column(name="id")]
        response = TableResponse(data_generator=data_gen(), columns=columns)
        assert response.rows_fetched == 0

        list(response._iterate_with_memory_check())

        pd.testing.assert_frame_equal(response._last_data_piece, df2)
        assert response.rows_fetched == 5


class TestTableResponseFetchallEdgeCases:
    """Additional edge-case tests for TableResponse.fetchall."""

    def test_fetchall_no_generator_returns_existing_data(self):
        """Test fetchall returns existing data when no generator is set."""
        df = pd.DataFrame({"id": [1, 2, 3]})
        response = TableResponse(data=df)

        result = response.fetchall()
        pd.testing.assert_frame_equal(result, df)

    @patch("mindsdb.integrations.libs.response.psutil")
    def test_fetchall_generator_only_no_initial_data(self, mock_psutil):
        """Test fetchall with generator but no initial data."""
        mock_psutil.virtual_memory.return_value = _mock_virtual_memory(available_kb=1_000_000)

        df1 = pd.DataFrame({"id": [1, 2]})
        df2 = pd.DataFrame({"id": [3, 4]})

        def data_gen():
            yield df1
            yield df2

        columns = [Column(name="id")]
        response = TableResponse(data_generator=data_gen(), columns=columns)

        result = response.fetchall()
        pd.testing.assert_frame_equal(result, pd.concat([df1, df2]))
        assert response._fetched is True
        assert response._data_generator is None

    @patch("mindsdb.integrations.libs.response.psutil")
    def test_fetchall_empty_generator_creates_empty_df(self, mock_psutil):
        """Test fetchall with empty generator creates DataFrame with column names."""
        mock_psutil.virtual_memory.return_value = _mock_virtual_memory(available_kb=1_000_000)

        columns = [Column(name="id"), Column(name="name")]
        response = TableResponse(data_generator=iter([]), columns=columns)

        result = response.fetchall()
        assert list(result.columns) == ["id", "name"]
        assert len(result) == 0

    def test_fetchall_raises_if_invalid(self):
        """Test fetchall raises ValueError if data was already consumed by iterate_no_save."""
        df = pd.DataFrame({"id": [1]})
        response = TableResponse(data=df, data_generator=iter([]))
        list(response.iterate_no_save())

        with pytest.raises(ValueError, match="Data has already been fetched"):
            response.fetchall()


class TestTableResponseFetchmanyEdgeCases:
    """Additional edge-case tests for TableResponse.fetchmany."""

    @patch("mindsdb.integrations.libs.response.psutil")
    def test_fetchmany_first_piece_with_no_initial_data(self, mock_psutil):
        """Test fetchmany sets _data directly when no initial data exists."""
        mock_psutil.virtual_memory.return_value = _mock_virtual_memory(available_kb=1_000_000)

        df1 = pd.DataFrame({"id": [1, 2]})
        columns = [Column(name="id")]
        response = TableResponse(data_generator=iter([df1]), columns=columns)

        piece = response.fetchmany()
        pd.testing.assert_frame_equal(piece, df1)
        pd.testing.assert_frame_equal(response._data, df1)

    @patch("mindsdb.integrations.libs.response.psutil")
    def test_fetchmany_accumulates_data(self, mock_psutil):
        """Test fetchmany accumulates pieces in _data."""
        mock_psutil.virtual_memory.return_value = _mock_virtual_memory(available_kb=1_000_000)

        df = pd.DataFrame({"id": [0]})
        df1 = pd.DataFrame({"id": [1]})
        df2 = pd.DataFrame({"id": [2]})

        def data_gen():
            yield df1
            yield df2

        columns = [Column(name="id")]
        response = TableResponse(data=df, data_generator=data_gen(), columns=columns)

        response.fetchmany()  # df1
        response.fetchmany()  # df2

        pd.testing.assert_frame_equal(response._data, pd.concat([df, df1, df2]))

    @patch("mindsdb.integrations.libs.response.psutil")
    def test_fetchmany_returns_none_when_exhausted(self, mock_psutil):
        """Test fetchmany returns None and marks response as fetched when generator is empty."""
        mock_psutil.virtual_memory.return_value = _mock_virtual_memory(available_kb=1_000_000)

        df1 = pd.DataFrame({"id": [1]})
        columns = [Column(name="id")]
        response = TableResponse(data_generator=iter([df1]), columns=columns)

        piece1 = response.fetchmany()
        assert isinstance(piece1, pd.DataFrame)

        piece2 = response.fetchmany()
        assert piece2 is None
        assert response._fetched is True
        assert response._data_generator is None

    def test_fetchmany_raises_if_invalid(self):
        """Test fetchmany raises ValueError after iterate_no_save."""
        df = pd.DataFrame({"id": [1]})
        response = TableResponse(data=df, data_generator=iter([]))
        list(response.iterate_no_save())

        with pytest.raises(ValueError, match="Data has already been fetched"):
            response.fetchmany()


class TestMemoryErrorPropagation:
    """Tests for MemoryError propagation through fetchall, fetchmany, and iterate_no_save."""

    @patch("mindsdb.integrations.libs.response.psutil")
    def test_fetchall_raises_memory_error(self, mock_psutil):
        """Test MemoryError propagates through fetchall."""
        # Enough memory for first chunk, then out of memory
        mock_psutil.virtual_memory.side_effect = [
            _mock_virtual_memory(available_kb=1_000_000),  # pre-loop check
            _mock_virtual_memory(available_kb=0),  # post-yield check
        ]

        df1 = pd.DataFrame({"x": list(range(1000))})
        df2 = pd.DataFrame({"x": list(range(1000))})

        def data_gen():
            yield df1
            yield df2

        columns = [Column(name="x")]
        response = TableResponse(data_generator=data_gen(), columns=columns)

        with pytest.raises(MemoryError):
            response.fetchall()

    @patch("mindsdb.integrations.libs.response.psutil")
    def test_fetchmany_raises_memory_error(self, mock_psutil):
        """Test MemoryError propagates through fetchmany on second call."""
        df1 = pd.DataFrame({"x": list(range(1000))})
        df2 = pd.DataFrame({"x": list(range(1000))})

        def data_gen():
            yield df1
            yield df2

        columns = [Column(name="x")]
        response = TableResponse(data_generator=data_gen(), columns=columns)

        # First fetchmany: enough memory (pre-loop check is no-op since _last_data_piece is None)
        mock_psutil.virtual_memory.return_value = _mock_virtual_memory(available_kb=1_000_000)
        response.fetchmany()

        # Second fetchmany: pre-loop check fails because we now have _last_data_piece set
        mock_psutil.virtual_memory.return_value = _mock_virtual_memory(available_kb=0)
        with pytest.raises(MemoryError):
            response.fetchmany()

    @patch("mindsdb.integrations.libs.response.psutil")
    def test_iterate_no_save_raises_memory_error(self, mock_psutil):
        """Test MemoryError propagates through iterate_no_save."""
        mock_psutil.virtual_memory.side_effect = [
            _mock_virtual_memory(available_kb=1_000_000),  # pre-loop check
            _mock_virtual_memory(available_kb=0),  # post-yield check after first chunk
        ]

        df1 = pd.DataFrame({"x": list(range(1000))})
        df2 = pd.DataFrame({"x": list(range(1000))})

        def data_gen():
            yield df1
            yield df2

        columns = [Column(name="x")]
        response = TableResponse(data_generator=data_gen(), columns=columns)

        with pytest.raises(MemoryError):
            list(response.iterate_no_save())
