"""
Test for QueryError exception handling fix for issue #11767
"""

import pytest
from mindsdb.utilities.exception import QueryError


class TestQueryErrorFix:
    """Test QueryError constructor robustness"""

    def test_query_error_with_valid_parameters(self):
        """Test that QueryError works with all valid parameters"""
        error = QueryError(
            db_name="test_db",
            db_type="test_type",
            db_error_msg="test message",
            failed_query="SELECT * FROM test",
            is_external=True,
            is_expected=True,
        )
        assert error.db_name == "test_db"
        assert error.db_type == "test_type"
        assert error.is_expected

    def test_query_error_with_defaults(self):
        """Test that QueryError works with default parameters"""
        error = QueryError(db_name="test_db", db_error_msg="test message")
        assert not error.is_expected  # default value
        assert error.is_external  # default value

    def test_query_error_invalid_parameter(self):
        """Test that QueryError rejects invalid parameters properly"""
        with pytest.raises(TypeError) as exc_info:
            QueryError(db_name="test_db", invalid_param="should_fail")

        # Verify the error message mentions the unexpected argument
        assert "unexpected keyword argument" in str(exc_info.value)

    def test_query_error_string_representation(self):
        """Test QueryError string representation"""
        error = QueryError(
            db_name="test_db", db_type="PostgreSQL", db_error_msg="Connection failed", failed_query="SELECT 1"
        )
        error_str = str(error)
        assert "test_db" in error_str
        assert "PostgreSQL" in error_str
        assert "Connection failed" in error_str
        assert "SELECT 1" in error_str
