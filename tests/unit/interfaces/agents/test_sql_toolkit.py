from unittest.mock import Mock

import pandas as pd

from mindsdb.interfaces.agents.utils.sql_toolkit import (
    MindsDBQuery,
    TablesCollection,
)


def _make_query(tables):
    """Build a MindsDBQuery without invoking SessionController/DB."""
    query = MindsDBQuery.__new__(MindsDBQuery)
    query.tables = TablesCollection(tables)
    query.knowledge_bases = TablesCollection([])
    query.command_executor = Mock()
    query._cache = None
    return query


def _make_handler(table_names):
    """Mock data handler whose `get_tables` returns a TableResponse-like object."""
    handler = Mock()
    df = pd.DataFrame({"table_name": table_names})
    response = Mock()
    response.data_frame = df
    handler.get_tables.return_value = response
    return handler


class TestGetUsableTableNames:
    def test_single_database_wildcard(self):
        query = _make_query(["db1.*"])
        handler = _make_handler(["t1", "t2"])
        query.command_executor.session.integration_controller.get_data_handler.return_value = handler

        result = query.get_usable_table_names(lazy=False)

        names = [tuple(ident.parts) for ident in result]
        assert ("db1", "t1") in names
        assert ("db1", "t2") in names
        assert len(names) == 2

    def test_multi_database_wildcard_returns_all(self):
        """Regression test: tables from all databases should be returned,
        not just the last one iterated."""
        query = _make_query(["db1.*", "db2.*"])

        handlers = {
            "db1": _make_handler(["a", "b"]),
            "db2": _make_handler(["c", "d", "e"]),
        }

        def get_handler(name):
            return handlers[name]

        query.command_executor.session.integration_controller.get_data_handler.side_effect = get_handler

        result = query.get_usable_table_names(lazy=False)

        names = {tuple(ident.parts) for ident in result}
        assert names == {
            ("db1", "a"),
            ("db1", "b"),
            ("db2", "c"),
            ("db2", "d"),
            ("db2", "e"),
        }

    def test_lazy_returns_items_as_is(self):
        query = _make_query(["db1.users", "db2.orders"])

        result = query.get_usable_table_names(lazy=True)

        names = {tuple(ident.parts) for ident in result}
        assert names == {("db1", "users"), ("db2", "orders")}

    def test_no_tables_returns_empty(self):
        query = _make_query([])
        assert query.get_usable_table_names(lazy=False) == []

    def test_mixed_wildcard_and_specific_table(self):
        """Wildcard databases should expand fully, while specific entries
        should still be filtered down to the exact table that matches."""
        query = _make_query(["db1.users", "db2.*"])

        handlers = {
            "db1": _make_handler(["users", "orders", "invoices"]),
            "db2": _make_handler(["a", "b"]),
        }
        query.command_executor.session.integration_controller.get_data_handler.side_effect = lambda name: handlers[name]

        result = query.get_usable_table_names(lazy=False)

        names = {tuple(ident.parts) for ident in result}
        assert names == {
            ("db1", "users"),
            ("db2", "a"),
            ("db2", "b"),
        }
        # Tables present in db1 but not listed in `data.tables` must NOT leak through.
        assert ("db1", "orders") not in names
        assert ("db1", "invoices") not in names
