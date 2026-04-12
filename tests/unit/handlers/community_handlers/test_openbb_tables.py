from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
import pytest

try:
    from mindsdb.integrations.handlers.openbb_handler.openbb_tables import OpenBBtable
except ImportError:
    OpenBBtable = None

pytestmark = pytest.mark.skipif(OpenBBtable is None, reason="openbb_handler not installed")


class _DummyOpenBBResponse:
    def __init__(self, payload):
        self.payload = payload

    def to_df(self):
        return pd.DataFrame([self.payload])


class _DummyPrice:
    def historical(self, **kwargs):
        return _DummyOpenBBResponse(kwargs)


class _DummyEquity:
    def __init__(self):
        self.price = _DummyPrice()


class _DummyCoverage:
    def __init__(self):
        self.commands = {".equity.price.historical": {}}


class _DummyObb:
    def __init__(self):
        self.equity = _DummyEquity()
        self.coverage = _DummyCoverage()


class _DummyHandler:
    def __init__(self):
        self.obb = _DummyObb()


def test_openbb_command_resolution_returns_callable():
    table = OpenBBtable(_DummyHandler())

    function = table._resolve_openbb_command("obb.equity.price.historical")
    result = function(symbol="AAPL").to_df()

    assert result.iloc[0]["symbol"] == "AAPL"


def test_openbb_select_treats_params_as_data():
    table = OpenBBtable(_DummyHandler())
    malicious_value = "__import__('os').system('echo hacked')"
    query = SimpleNamespace(where=object())

    with patch(
        "mindsdb.integrations.handlers.openbb_handler.openbb_tables.extract_comparison_conditions",
        return_value=[["=", "cmd", "obb.equity.price.historical"], ["=", "symbol", malicious_value]],
    ):
        result = table.select(query)

    assert result.iloc[0]["symbol"] == malicious_value


def test_openbb_command_resolution_rejects_private_segments():
    table = OpenBBtable(_DummyHandler())

    with pytest.raises(ValueError, match="Invalid OpenBB command segment"):
        table._resolve_openbb_command("obb.__class__")


def test_openbb_select_coerces_literal_string_params():
    table = OpenBBtable(_DummyHandler())
    query = SimpleNamespace(where=object())

    with patch(
        "mindsdb.integrations.handlers.openbb_handler.openbb_tables.extract_comparison_conditions",
        return_value=[
            ["=", "cmd", "obb.equity.price.historical"],
            ["=", "limit", "123"],
            ["=", "adjusted", "true"],
            ["=", "symbol", "'AAPL'"],
            ["=", "ids", "[1, 2]"],
            ["=", "raw_symbol", "AAPL"],
        ],
    ):
        result = table.select(query)

    row = result.iloc[0]
    assert row["limit"] == 123
    assert bool(row["adjusted"]) is True
    assert row["symbol"] == "AAPL"
    assert row["ids"] == [1, 2]
    assert row["raw_symbol"] == "AAPL"
