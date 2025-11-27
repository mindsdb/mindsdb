import io
import logging
import pytest

from mindsdb.utilities.log import StreamSanitizingHandler


MASK = "********"
SECRET = "Pa$Sw0rd"


class TestStreamSanitizingHandler:
    """Test StreamSanitizingHandler class"""

    @pytest.fixture
    def logger_with_handler(self):
        """Create logger with StreamSanitizingHandler and string buffer"""
        logger = logging.getLogger("test_logger")
        logger.handlers.clear()
        logger.setLevel(logging.INFO)
        
        # Create string buffer to capture output
        stream = io.StringIO()
        handler = StreamSanitizingHandler(stream)
        handler.setFormatter(logging.Formatter('%(message)s'))
        logger.addHandler(handler)
        
        return logger, stream

    def test_handler_sanitizes_string_message(self, logger_with_handler):
        """Test that handler sanitizes string messages"""
        logger, stream = logger_with_handler

        logger.info(f"Login with password={SECRET}")
        logger.info(f'CREATE DATABASE test WITH PARAMETERS={{"ToKeN": "{SECRET}"}}')
        logger.info(f'CREATE DATABASE test USING api_KEY =  {SECRET}')
        output = stream.getvalue()

        assert SECRET not in output
        assert MASK in output

    def test_handler_sanitizes_dict_message(self, logger_with_handler):
        """Test that handler sanitizes dictionary messages"""
        logger, stream = logger_with_handler
        
        logger.info({"user": "john", "password": SECRET, "token": SECRET})
        output = stream.getvalue()

        assert SECRET not in output
        assert MASK in output

    def test_handler_sanitizes_formatted_message(self, logger_with_handler):
        """Test that handler sanitizes formatted messages with args"""
        logger, stream = logger_with_handler
        
        logger.info("CREATE MODEL test WITH password = %s", SECRET)
        output = stream.getvalue()
        
        assert SECRET not in output
        assert MASK in output

    def test_normal_messages(self, logger_with_handler):
        """Test that handler preserves non-sensitive messages"""
        logger, stream = logger_with_handler
        
        logger.info('CREATE MODEL test WITH ENGINE = "%s"', "postgres")
        output = stream.getvalue()
        
        assert output.strip("\n") == 'CREATE MODEL test WITH ENGINE = "postgres"'
        assert MASK not in output

    def test_handler_sanitizes_multiple_sensitive_keys(self, logger_with_handler):
        """Test that handler sanitizes multiple types of sensitive data"""
        logger, stream = logger_with_handler
        
        logger.info(f"Connecting: password={SECRET}, api_key = {SECRET}, token: {SECRET}")
        output = stream.getvalue()
        
        assert SECRET not in output
        assert MASK in output
