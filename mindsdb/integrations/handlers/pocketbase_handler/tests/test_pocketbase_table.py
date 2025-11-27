from mindsdb.integrations.handlers.pocketbase_handler.pocketbase_table import PocketbaseTable
from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator


class DummyHandler:
    def __init__(self):
        self.connected = False
        self.page_calls = 0

    def connect(self):
        self.connected = True

    def _request(self, method, endpoint, params=None, **kwargs):
        self.page_calls += 1
        items = [
            {"id": str(self.page_calls), "title": f"title-{self.page_calls}", "collectionName": "posts"},
        ]
        total_items = 2
        return {
            "page": self.page_calls,
            "perPage": params.get("perPage", 1) if params else 1,
            "totalItems": total_items,
            "items": items if self.page_calls <= total_items else [],
        }


def make_table():
    collection = {"name": "posts", "schema": [{"name": "title"}, {"name": "body"}]}
    handler = DummyHandler()
    return PocketbaseTable(handler=handler, collection=collection), handler


def test_build_columns_includes_schema_fields():
    table, _ = make_table()
    assert "title" in table.get_columns()
    assert "body" in table.get_columns()
    assert "id" in table.get_columns()


def test_build_filter_pushes_basic_condition():
    table, _ = make_table()
    condition = FilterCondition("title", FilterOperator.EQUAL, "hello")
    expression = table._build_filter([condition])
    assert expression == "title='hello'"
    assert condition.applied is True


def test_resolve_record_ids_supports_equal_and_in():
    table, _ = make_table()
    equal_id = FilterCondition("id", FilterOperator.EQUAL, "123")
    in_ids = FilterCondition("id", FilterOperator.IN, ["456", "789"])
    record_ids = table._resolve_record_ids([equal_id, in_ids])
    assert record_ids == ["123", "456", "789"]


def test_list_respects_limit_and_returns_dataframe():
    table, handler = make_table()
    df = table.list(limit=2)
    assert handler.connected is True
    assert len(df) == 2
    assert set(df.columns) >= {"id", "title"}
