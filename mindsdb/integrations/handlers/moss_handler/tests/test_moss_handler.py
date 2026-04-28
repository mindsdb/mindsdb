import unittest
from unittest.mock import MagicMock, patch
import pandas as pd

from mindsdb.integrations.handlers.moss_handler.moss_handler import MossHandler
from mindsdb.integrations.libs.response import RESPONSE_TYPE
from mindsdb.integrations.libs.vectordatabase_handler import FilterCondition, FilterOperator, TableField


HANDLER_KWARGS = {
    "connection_data": {
        "project_id": "test-project-id",
        "project_key": "test-project-key",
        "alpha": "0.8",
    }
}


def _make_handler():
    return MossHandler(name="test_moss", **HANDLER_KWARGS)


def _mock_doc(id="doc-1", text="hello world", metadata=None, score=0.9):
    doc = MagicMock()
    doc.id = id
    doc.text = text
    doc.metadata = metadata or {}
    doc.score = score
    return doc


def _mock_index(name="my_index", status="Ready", doc_count=1):
    idx = MagicMock()
    idx.name = name
    idx.status = status
    idx.doc_count = doc_count
    return idx


def _mock_search_result(docs):
    result = MagicMock()
    result.docs = docs
    return result


class TestMossHandlerConnection(unittest.TestCase):
    @patch("mindsdb.integrations.handlers.moss_handler.moss_handler.MossClient")
    def test_connect(self, MockClient):
        handler = _make_handler()
        handler.connect()
        MockClient.assert_called_once_with("test-project-id", "test-project-key")
        self.assertTrue(handler.is_connected)

    @patch("mindsdb.integrations.handlers.moss_handler.moss_handler.MossClient")
    def test_connect_idempotent(self, MockClient):
        handler = _make_handler()
        handler.connect()
        handler.connect()
        MockClient.assert_called_once()

    @patch("mindsdb.integrations.handlers.moss_handler.moss_handler._run")
    @patch("mindsdb.integrations.handlers.moss_handler.moss_handler.MossClient")
    def test_check_connection_success(self, MockClient, mock_run):
        mock_run.return_value = []
        handler = _make_handler()
        res = handler.check_connection()
        self.assertTrue(res.success)

    @patch("mindsdb.integrations.handlers.moss_handler.moss_handler._run")
    @patch("mindsdb.integrations.handlers.moss_handler.moss_handler.MossClient")
    def test_check_connection_failure(self, MockClient, mock_run):
        mock_run.side_effect = Exception("auth failed")
        handler = _make_handler()
        res = handler.check_connection()
        self.assertFalse(res.success)
        self.assertIn("auth failed", res.error_message)


class TestMossHandlerInsert(unittest.TestCase):
    def _make_connected_handler(self, mock_run):
        handler = _make_handler()
        handler._client = MagicMock()
        handler.is_connected = True
        mock_run.side_effect = self._run_side_effects(mock_run)
        return handler

    @staticmethod
    def _run_side_effects(mock_run):
        # list_indexes → [] (index doesn't exist yet), get_index → Ready
        ready_index = _mock_index(status="Ready")
        calls = iter([[], None, ready_index])
        return lambda coro: next(calls)

    @patch("mindsdb.integrations.handlers.moss_handler.moss_handler._run")
    def test_insert_creates_index_when_new(self, mock_run):
        handler = _make_handler()
        handler._client = MagicMock()
        handler.is_connected = True

        # list_indexes returns empty → _index_exists = False
        # create_index returns None
        # get_index returns Ready
        ready = _mock_index(status="Ready")
        mock_run.side_effect = [[], None, ready]

        df = pd.DataFrame({
            TableField.ID.value: ["doc-1"],
            TableField.CONTENT.value: ["MindsDB unifies AI and data"],
            TableField.METADATA.value: ['{"category": "docs"}'],
        })
        res = handler.insert("my_index", df)

        handler._client.create_index.assert_called_once()
        self.assertEqual(res.resp_type, RESPONSE_TYPE.OK)

    @patch("mindsdb.integrations.handlers.moss_handler.moss_handler._run")
    def test_insert_upserts_when_index_exists(self, mock_run):
        handler = _make_handler()
        handler._client = MagicMock()
        handler.is_connected = True

        existing = _mock_index(name="my_index", status="Ready")
        ready = _mock_index(status="Ready")
        # list_indexes → [existing], add_docs → None, get_index → Ready
        mock_run.side_effect = [[existing], None, ready]

        df = pd.DataFrame({
            TableField.ID.value: ["doc-1"],
            TableField.CONTENT.value: ["updated content"],
            TableField.METADATA.value: [None],
        })
        handler.insert("my_index", df)
        handler._client.add_docs.assert_called_once()

    @patch("mindsdb.integrations.handlers.moss_handler.moss_handler._run")
    def test_insert_generates_id_when_missing(self, mock_run):
        handler = _make_handler()
        handler._client = MagicMock()
        handler.is_connected = True

        ready = _mock_index(status="Ready")
        mock_run.side_effect = [[], None, ready]

        df = pd.DataFrame({
            TableField.CONTENT.value: ["no id provided"],
        })
        handler.insert("my_index", df)

        call_args = handler._client.create_index.call_args
        docs = call_args[0][1]
        self.assertEqual(len(docs), 1)
        self.assertIsNotNone(docs[0].id)
        self.assertNotEqual(docs[0].id, "")


class TestMossHandlerSelect(unittest.TestCase):
    @patch("mindsdb.integrations.handlers.moss_handler.moss_handler._run")
    def test_select_semantic_search(self, mock_run):
        handler = _make_handler()
        handler._client = MagicMock()
        handler.is_connected = True
        handler._loaded_indexes.add("my_index")

        results = [_mock_doc("doc-1", "MindsDB is great", score=0.95)]
        mock_run.return_value = _mock_search_result(results)

        conditions = [FilterCondition(
            column=TableField.CONTENT.value,
            op=FilterOperator.EQUAL,
            value="what is MindsDB?",
        )]
        df = handler.select("my_index", conditions=conditions, limit=5)

        handler._client.query.assert_called_once()
        self.assertIn(TableField.CONTENT.value, df.columns)
        self.assertIn(TableField.DISTANCE.value, df.columns)
        self.assertAlmostEqual(df[TableField.DISTANCE.value].iloc[0], 0.05)

    @patch("mindsdb.integrations.handlers.moss_handler.moss_handler._run")
    def test_select_score_to_distance_mapping(self, mock_run):
        handler = _make_handler()
        handler._client = MagicMock()
        handler.is_connected = True
        handler._loaded_indexes.add("my_index")

        mock_run.return_value = _mock_search_result([_mock_doc(score=1.0)])

        conditions = [FilterCondition(
            column=TableField.CONTENT.value,
            op=FilterOperator.EQUAL,
            value="query",
        )]
        df = handler.select("my_index", conditions=conditions)
        self.assertAlmostEqual(df[TableField.DISTANCE.value].iloc[0], 0.0)

    @patch("mindsdb.integrations.handlers.moss_handler.moss_handler._run")
    def test_select_get_all_docs(self, mock_run):
        handler = _make_handler()
        handler._client = MagicMock()
        handler.is_connected = True
        handler._loaded_indexes.add("my_index")

        docs = [_mock_doc("doc-1"), _mock_doc("doc-2")]
        mock_run.return_value = docs

        df = handler.select("my_index")

        handler._client.get_docs.assert_called_once()
        self.assertEqual(len(df), 2)

    @patch("mindsdb.integrations.handlers.moss_handler.moss_handler._run")
    def test_select_by_id(self, mock_run):
        handler = _make_handler()
        handler._client = MagicMock()
        handler.is_connected = True
        handler._loaded_indexes.add("my_index")

        mock_run.return_value = [_mock_doc("doc-1")]

        conditions = [FilterCondition(
            column=TableField.ID.value,
            op=FilterOperator.EQUAL,
            value="doc-1",
        )]
        handler.select("my_index", conditions=conditions)

        call_args = handler._client.get_docs.call_args
        get_opts = call_args[0][1]
        self.assertIn("doc-1", get_opts.doc_ids)

    @patch("mindsdb.integrations.handlers.moss_handler.moss_handler._run")
    def test_select_uses_alpha(self, mock_run):
        handler = _make_handler()
        handler._client = MagicMock()
        handler.is_connected = True
        handler._loaded_indexes.add("my_index")
        handler._alpha = 0.5

        mock_run.return_value = _mock_search_result([])

        conditions = [FilterCondition(
            column=TableField.CONTENT.value,
            op=FilterOperator.EQUAL,
            value="query",
        )]
        handler.select("my_index", conditions=conditions)

        call_args = handler._client.query.call_args
        query_opts = call_args[0][2]
        self.assertEqual(query_opts.alpha, 0.5)


class TestMossHandlerDelete(unittest.TestCase):
    @patch("mindsdb.integrations.handlers.moss_handler.moss_handler._run")
    def test_delete_by_id(self, mock_run):
        handler = _make_handler()
        handler._client = MagicMock()
        handler.is_connected = True

        ready = _mock_index(status="Ready")
        mock_run.side_effect = [None, ready]

        conditions = [FilterCondition(
            column=TableField.ID.value,
            op=FilterOperator.EQUAL,
            value="doc-1",
        )]
        handler.delete("my_index", conditions=conditions)

        handler._client.delete_docs.assert_called_once_with("my_index", ["doc-1"])

    def test_delete_without_conditions_raises(self):
        handler = _make_handler()
        handler._client = MagicMock()
        handler.is_connected = True
        with self.assertRaises(Exception):
            handler.delete("my_index", conditions=[])

    def test_delete_without_id_filter_raises(self):
        handler = _make_handler()
        handler._client = MagicMock()
        handler.is_connected = True
        conditions = [FilterCondition(
            column="metadata.category",
            op=FilterOperator.EQUAL,
            value="docs",
        )]
        with self.assertRaises(Exception):
            handler.delete("my_index", conditions=conditions)


class TestMossHandlerTableOps(unittest.TestCase):
    @patch("mindsdb.integrations.handlers.moss_handler.moss_handler._run")
    def test_get_tables(self, mock_run):
        handler = _make_handler()
        handler._client = MagicMock()
        handler.is_connected = True

        mock_run.return_value = [_mock_index("idx-a"), _mock_index("idx-b")]

        res = handler.get_tables()
        self.assertEqual(list(res.data_frame["table_name"]), ["idx-a", "idx-b"])

    @patch("mindsdb.integrations.handlers.moss_handler.moss_handler._run")
    def test_drop_table(self, mock_run):
        handler = _make_handler()
        handler._client = MagicMock()
        handler.is_connected = True
        handler._loaded_indexes.add("my_index")

        mock_run.return_value = True
        handler.drop_table("my_index")

        handler._client.delete_index.assert_called_once_with("my_index")
        self.assertNotIn("my_index", handler._loaded_indexes)

    @patch("mindsdb.integrations.handlers.moss_handler.moss_handler._run")
    def test_drop_table_if_exists_swallows_error(self, mock_run):
        handler = _make_handler()
        handler._client = MagicMock()
        handler.is_connected = True
        mock_run.side_effect = Exception("not found")

        handler.drop_table("my_index", if_exists=True)  # should not raise

    def test_create_table_is_noop(self):
        handler = _make_handler()
        handler._client = MagicMock()
        handler.is_connected = True
        handler.create_table("my_index")  # should not raise or call anything
        handler._client.assert_not_called()


if __name__ == "__main__":
    unittest.main()
