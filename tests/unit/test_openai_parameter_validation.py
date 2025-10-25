"""
Integration test for OpenAI handler parameter validation improvements.
Tests the fix for issue #11767.
"""

import pytest
from mindsdb.integrations.handlers.openai_handler.openai_handler import OpenAIHandler


class TestOpenAIParameterValidation:
    """Test improved OpenAI handler parameter validation"""

    def test_openai_validation_with_dashscope_style_params(self):
        """Test validation with DashScope-style parameters that should provide helpful hints"""

        # Mock handler storage
        class MockHandlerStorage:
            def get_connection_args(self):
                return {}

        target = "test_column"
        args = {
            "using": {
                "model_name": "qwen3-max-2025-09-23",
                "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",  # Should suggest api_base
                "api_key": "fake-api-key",  # Should suggest openai_api_key
                "prompt": "test prompt",
            }
        }

        with pytest.raises(ValueError) as exc_info:
            OpenAIHandler.create_validation(target=target, args=args, handler_storage=MockHandlerStorage())

        error_msg = str(exc_info.value)

        # Verify the error includes helpful hints
        assert "api_key, base_url" in error_msg  # Should list the unknown args
        assert "openai_api_key" in error_msg  # Should suggest the correct parameter
        assert "api_base" in error_msg  # Should suggest the correct parameter
        assert "Hint:" in error_msg  # Should provide hints

    def test_openai_validation_with_correct_params(self):
        """Test that validation works with correct parameters"""

        class MockHandlerStorage:
            def get_connection_args(self):
                return {}

        target = "test_column"
        args = {
            "using": {
                "model_name": "gpt-3.5-turbo",
                "openai_api_key": "fake-api-key",  # Correct parameter name
                "api_base": "https://api.openai.com/v1",  # Correct parameter name
                "prompt": "test prompt",
            }
        }

        # This should raise an exception due to invalid API key, but NOT due to parameter validation
        with pytest.raises(Exception) as exc_info:
            OpenAIHandler.create_validation(target=target, args=args, handler_storage=MockHandlerStorage())

        # The error should NOT be about unknown arguments
        error_msg = str(exc_info.value)
        assert "Unknown arguments" not in error_msg
        assert "Hint:" not in error_msg

    def test_openai_validation_parameter_error_type(self):
        """Test that parameter validation raises ValueError specifically"""

        class MockHandlerStorage:
            def get_connection_args(self):
                return {}

        target = "test_column"
        args = {"using": {"model_name": "gpt-3.5-turbo", "invalid_param": "should_fail", "prompt": "test prompt"}}

        # Should raise ValueError (not generic Exception)
        with pytest.raises(ValueError):
            OpenAIHandler.create_validation(target=target, args=args, handler_storage=MockHandlerStorage())
