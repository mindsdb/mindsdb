from unittest.mock import Mock, patch

import pytest

from mindsdb.integrations.handlers.openai_handler.openai_handler import OpenAIHandler
from mindsdb.integrations.handlers.statsforecast_handler.statsforecast_handler import (
    StatsForecastHandler,
)
from mindsdb.integrations.libs.base import ArgProbeMixin

"""
Tests for the arg probing mixin
"""


@pytest.fixture
def mock_handler_class():
    class MockHandler(ArgProbeMixin):
        def __init__(self, **kwargs):
            ...

        def create(self, args):
            args["test_required"]
            args.get("test_optional", "default")
            args.get("test_optional2")
            args["test_required2"] = "default"  # this should be ignored
            args.setdefault("test_optional3", "default")  # this should be ignored

        def predict(self, args):
            args["test_required"]
            args.get("test_optional", "default")
            args.get("test_optional2")
            args["test_required2"] = "default"  # this should be ignored
            args.setdefault("test_optional3", "default")  # this should be ignored

    return MockHandler


@pytest.fixture
def mock_openai_handler_class():
    # let the openai handler use the arg probing mixin
    class MockOpenAIHandler(OpenAIHandler, ArgProbeMixin):
        def __init__(self, **kwargs):
            ...

    return MockOpenAIHandler


def test_arg_probing(mock_handler_class):
    handler = mock_handler_class

    # Test create
    prediction_args = handler.prediction_args()
    # sort
    prediction_args = sorted(prediction_args, key=lambda x: x["name"])

    assert prediction_args == [
        {
            "name": "test_optional",
            "required": False,
        },
        {
            "name": "test_optional2",
            "required": False,
        },
        {
            "name": "test_required",
            "required": True,
        },
    ]

    creation_args = handler.creation_args()
    # sort
    creation_args = sorted(creation_args, key=lambda x: x["name"])
    assert creation_args == [
        {
            "name": "test_optional",
            "required": False,
        },
        {
            "name": "test_optional2",
            "required": False,
        },
        {
            "name": "test_required",
            "required": True,
        },
    ]


def test_openai_handler_probing(mock_openai_handler_class):
    handler = mock_openai_handler_class

    # Test create
    known_args = [
        {
            "name": "mode",
            "required": False,
        },
        {
            "name": "temperature",
            "required": False,
        },
    ]
    # Check that the known args are in the creation args
    for arg in known_args:
        assert arg in handler.prediction_args()

    # Check that some unknown args are not in the creation args
    assert {
        "name": "unknown_arg",
        "required": False,
    } not in handler.prediction_args()

    # inspiring example in  https://github.com/mindsdb/mindsdb/issues/6846
    assert {
        "name": "enigne",
        "required": False,
    } not in handler.prediction_args()


def test_statsforecast_handler_probing():
    class MockClass(StatsForecastHandler, ArgProbeMixin):
        def __init__(self, **kwargs):
            ...

    handler = MockClass
    assert len(handler.prediction_args()) == 0
    assert len(handler.creation_args()) > 0
