import json
import os
import tempfile
import time
import unittest
from unittest.mock import patch

# Ensure MindsDB config points to a writable temp location in isolated test runs.
if "MINDSDB_CONFIG_PATH" not in os.environ:
    _storage_dir = tempfile.mkdtemp(prefix="mindsdb_cache_test_")
    _cfg_file = os.path.join(_storage_dir, "config.json")
    with open(_cfg_file, "w") as _fd:
        json.dump({"storage_db": f"sqlite:///{os.path.join(_storage_dir, 'mindsdb.db')}"}, _fd)
    os.environ["MINDSDB_STORAGE_DIR"] = _storage_dir
    os.environ["MINDSDB_CONFIG_PATH"] = _cfg_file

from mindsdb.interfaces.database.integrations import HandlersCache
from mindsdb.utilities.context import context as ctx


class DummyHandler:
    def __init__(self):
        self.disconnect_calls = 0

    def disconnect(self):
        self.disconnect_calls += 1


class TestHandlersCache(unittest.TestCase):
    def setUp(self):
        self._previous_context = ctx.dump()
        ctx.company_id = 42
        self.cache = HandlersCache()

    def tearDown(self):
        self.cache._stop_clean()
        ctx.load(self._previous_context)

    def test_delete_removes_all_keys_for_name_and_company(self):
        handler_a = DummyHandler()
        handler_b = DummyHandler()
        handler_other_company = DummyHandler()
        handler_other_name = DummyHandler()

        self.cache.handlers = {
            ("ga_conn", 42, 1001): {"handler": handler_a, "expired_at": time.time() + 60},
            ("ga_conn", 42, 1002): {"handler": handler_b, "expired_at": time.time() + 60},
            ("ga_conn", 7, 1003): {"handler": handler_other_company, "expired_at": time.time() + 60},
            ("other_conn", 42, 1004): {"handler": handler_other_name, "expired_at": time.time() + 60},
        }

        self.cache.delete("ga_conn")

        self.assertNotIn(("ga_conn", 42, 1001), self.cache.handlers)
        self.assertNotIn(("ga_conn", 42, 1002), self.cache.handlers)
        self.assertIn(("ga_conn", 7, 1003), self.cache.handlers)
        self.assertIn(("other_conn", 42, 1004), self.cache.handlers)
        self.assertEqual(handler_a.disconnect_calls, 1)
        self.assertEqual(handler_b.disconnect_calls, 1)
        self.assertEqual(handler_other_company.disconnect_calls, 0)
        self.assertEqual(handler_other_name.disconnect_calls, 0)

    def test_cleaner_disconnects_cached_handler_entry(self):
        handler = DummyHandler()
        self.cache.handlers = {
            ("ga_conn", 42, 1001): {"handler": handler, "expired_at": time.time() - 1},
        }

        with patch.object(self.cache._stop_event, "wait", side_effect=[False, True]):
            with patch("mindsdb.interfaces.database.integrations.sys.getrefcount", return_value=2):
                self.cache._clean()

        self.assertEqual(handler.disconnect_calls, 1)
        self.assertEqual(self.cache.handlers, {})
