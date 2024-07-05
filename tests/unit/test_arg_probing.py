import pytest
from mindsdb.integrations.handlers.openai_handler.openai_handler import OpenAIHandler
from mindsdb.integrations.handlers.statsforecast_handler.statsforecast_handler import StatsForecastHandler
from mindsdb.integrations.libs.base import ArgProbeMixin

@pytest.fixture
def mock_handler_class():
    class MockHandler(ArgProbeMixin):
        def __init__(self, **kwargs):
            pass

        def create(self, args):
            args["test_required"]
            args.get("test_optional", "default")
            args.get("test_optional2")
            args["test_required2"] = "default"
            args.setdefault("test_optional3", "default")

        def predict(self, args):
            args["test_required_at_some_point"]
            args.get("test_optional", "default")
            args.get("test_optional2")
            args["test_required2"] = "default"
            args.setdefault("test_optional3", "default")
            args.get("test_required_at_some_point", "but_not_always")

            _ = args.get("this_is_actually_required", "default")
            _ = args["this_is_actually_required"]

    return MockHandler

@pytest.fixture
def mock_openai_handler_class():
    class MockOpenAIHandler(OpenAIHandler, ArgProbeMixin):
        def __init__(self, **kwargs):
            pass

    return MockOpenAIHandler

def test_arg_probing(mock_handler_class):
    handler = mock_handler_class

    prediction_args = sorted(handler.prediction_args(), key=lambda x: x["name"])
    expected_prediction_args = [
        {"name": "test_optional", "required": False},
        {"name": "test_optional2", "required": False},
        {"name": "test_required_at_some_point", "required": False},
        {"name": "this_is_actually_required", "required": False},
    ]
    assert prediction_args == expected_prediction_args

    creation_args = sorted(handler.creation_args(), key=lambda x: x["name"])
    expected_creation_args = [
        {"name": "test_optional", "required": False},
        {"name": "test_optional2", "required": False},
    ]
    assert creation_args == expected_creation_args

def test_openai_handler_probing(mock_openai_handler_class):
    handler = mock_openai_handler_class

    known_args = [
        {"name": "mode", "required": False},
        {"name": "temperature", "required": False},
    ]
    prediction_args = handler.prediction_args()
    
    for arg in known_args:
        assert arg in prediction_args

    assert {"name": "unknown_arg", "required": False} not in prediction_args
    assert {"name": "engine", "required": False} not in prediction_args

def test_statsforecast_handler_probing():
    class MockClass(StatsForecastHandler, ArgProbeMixin):
        def __init__(self, **kwargs):
            pass

    handler = MockClass
    assert not handler.prediction_args()
    assert not handler.creation_args()
