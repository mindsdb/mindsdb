import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import requests

from mindsdb.integrations.utilities.community_handler_fetcher import (
    _fetch_tree_recursive,
    _resolve_tree_sha,
    fetch_handler,
)

REPO = "mindsdb/mindsdb-community-handlers"
BRANCH = "main"
PATH_PREFIX = "community_handlers"
HANDLER = "elasticsearch_handler"
REMOTE_PREFIX = f"{PATH_PREFIX}/{HANDLER}"
TREE_SHA = "abc123deadbeef"


def _make_response(status_code=200, json_data=None, raise_for_status=None):
    """Helper: build a MagicMock that looks like a requests.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data if json_data is not None else {}
    resp.text = ""
    if raise_for_status is not None:
        resp.raise_for_status.side_effect = raise_for_status
    else:
        resp.raise_for_status.return_value = None
    return resp


def _make_get_side_effect(contents_resp, trees_resp, raw_resp=None):
    """Return a side_effect callable that dispatches mocked responses by URL."""

    def _get(url, **kwargs):
        if "git/trees" in url:
            return trees_resp
        if "raw.githubusercontent.com" in url:
            return raw_resp if raw_resp is not None else _make_response(200, b"")
        # Contents API (resolve SHA or other)
        return contents_resp

    return _get


class TestResolveTreSha(unittest.TestCase):
    """Unit tests for _resolve_tree_sha()."""

    @patch("mindsdb.integrations.utilities.community_handler_fetcher.requests.get")
    def test_returns_sha_when_found(self, mock_get):
        parent_listing = [
            {"name": "other_handler", "type": "dir", "sha": "000"},
            {"name": HANDLER, "type": "dir", "sha": TREE_SHA},
        ]
        mock_get.return_value = _make_response(200, parent_listing)

        result = _resolve_tree_sha(REPO, BRANCH, REMOTE_PREFIX, {})

        self.assertEqual(result, TREE_SHA)
        mock_get.assert_called_once()

    @patch("mindsdb.integrations.utilities.community_handler_fetcher.requests.get")
    def test_returns_none_on_404(self, mock_get):
        mock_get.return_value = _make_response(404)

        result = _resolve_tree_sha(REPO, BRANCH, REMOTE_PREFIX, {})

        self.assertIsNone(result)

    @patch("mindsdb.integrations.utilities.community_handler_fetcher.requests.get")
    def test_returns_none_when_dir_not_in_listing(self, mock_get):
        parent_listing = [{"name": "other_handler", "type": "dir", "sha": "000"}]
        mock_get.return_value = _make_response(200, parent_listing)

        result = _resolve_tree_sha(REPO, BRANCH, REMOTE_PREFIX, {})

        self.assertIsNone(result)

    @patch("mindsdb.integrations.utilities.community_handler_fetcher.requests.get")
    def test_raises_on_non_200_non_404(self, mock_get):
        mock_get.return_value = _make_response(503)

        with self.assertRaises(RuntimeError):
            _resolve_tree_sha(REPO, BRANCH, REMOTE_PREFIX, {})

    @patch("mindsdb.integrations.utilities.community_handler_fetcher.requests.get")
    def test_raises_on_network_error(self, mock_get):
        mock_get.side_effect = requests.RequestException("timeout")

        with self.assertRaises(RuntimeError):
            _resolve_tree_sha(REPO, BRANCH, REMOTE_PREFIX, {})


class TestFetchTreeRecursive(unittest.TestCase):
    """Unit tests for _fetch_tree_recursive()."""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _trees_response(self, entries, truncated=False):
        return _make_response(200, {"sha": TREE_SHA, "tree": entries, "truncated": truncated})

    @patch("mindsdb.integrations.utilities.community_handler_fetcher.requests.get")
    def test_downloads_flat_and_nested_files(self, mock_get):
        tree_entries = [
            {"path": "__init__.py", "type": "blob", "sha": "s1", "size": 10},
            {"path": "elasticsearch_handler.py", "type": "blob", "sha": "s2", "size": 500},
            {"path": "tests", "type": "tree", "sha": "s3"},
            {"path": "tests/__init__.py", "type": "blob", "sha": "s4", "size": 0},
            {"path": "tests/test_elasticsearch_handler.py", "type": "blob", "sha": "s5", "size": 1107},
        ]
        trees_resp = self._trees_response(tree_entries)
        raw_resp = _make_response(200)
        raw_resp.content = b"# file content"

        def _get(url, **kwargs):
            if "git/trees" in url:
                return trees_resp
            return raw_resp

        mock_get.side_effect = _get

        count = _fetch_tree_recursive(REPO, BRANCH, TREE_SHA, REMOTE_PREFIX, self.tmp, {})

        self.assertEqual(count, 4)
        self.assertTrue((self.tmp / "__init__.py").exists())
        self.assertTrue((self.tmp / "elasticsearch_handler.py").exists())
        self.assertTrue((self.tmp / "tests" / "__init__.py").exists())
        self.assertTrue((self.tmp / "tests" / "test_elasticsearch_handler.py").exists())

    @patch("mindsdb.integrations.utilities.community_handler_fetcher.requests.get")
    def test_max_depth_enforcement(self, mock_get):
        # depth 4 means path has 4 slashes → 5 components → should be skipped
        deep_path = "a/b/c/d/e.py"
        self.assertEqual(deep_path.count("/"), 4)  # 4 >= max_depth=4 → skipped

        tree_entries = [
            {"path": "__init__.py", "type": "blob", "sha": "s1", "size": 0},
            {"path": deep_path, "type": "blob", "sha": "s2", "size": 99},
        ]
        trees_resp = self._trees_response(tree_entries)
        raw_resp = _make_response(200)
        raw_resp.content = b""

        def _get(url, **kwargs):
            if "git/trees" in url:
                return trees_resp
            return raw_resp

        mock_get.side_effect = _get

        count = _fetch_tree_recursive(REPO, BRANCH, TREE_SHA, REMOTE_PREFIX, self.tmp, {}, max_depth=4)

        self.assertEqual(count, 1)
        self.assertTrue((self.tmp / "__init__.py").exists())
        self.assertFalse((self.tmp / deep_path).exists())

    @patch("mindsdb.integrations.utilities.community_handler_fetcher.requests.get")
    def test_truncated_tree_logs_warning(self, mock_get):
        trees_resp = self._trees_response([], truncated=True)
        mock_get.return_value = trees_resp

        with self.assertLogs("mindsdb.integrations.utilities.community_handler_fetcher", level="WARNING") as cm:
            _fetch_tree_recursive(REPO, BRANCH, TREE_SHA, REMOTE_PREFIX, self.tmp, {})

        self.assertTrue(any("truncated" in line for line in cm.output))

    @patch("mindsdb.integrations.utilities.community_handler_fetcher.requests.get")
    def test_raises_on_file_download_failure(self, mock_get):
        tree_entries = [{"path": "__init__.py", "type": "blob", "sha": "s1", "size": 10}]
        trees_resp = self._trees_response(tree_entries)
        raw_resp = _make_response(500)
        raw_resp.raise_for_status.side_effect = requests.HTTPError("500 Server Error")

        def _get(url, **kwargs):
            if "git/trees" in url:
                return trees_resp
            return raw_resp

        mock_get.side_effect = _get

        with self.assertRaises(RuntimeError):
            _fetch_tree_recursive(REPO, BRANCH, TREE_SHA, REMOTE_PREFIX, self.tmp, {})


class TestFetchHandler(unittest.TestCase):
    """Integration-style unit tests for fetch_handler()."""

    def setUp(self):
        self.storage = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.storage, ignore_errors=True)

    def _parent_listing(self):
        return [{"name": HANDLER, "type": "dir", "sha": TREE_SHA}]

    def _tree_entries(self):
        return [
            {"path": "__init__.py", "type": "blob", "sha": "s1", "size": 10},
            {"path": "tests/__init__.py", "type": "blob", "sha": "s2", "size": 0},
            {"path": "tests/test_elasticsearch_handler.py", "type": "blob", "sha": "s3", "size": 1107},
        ]

    @patch("mindsdb.integrations.utilities.community_handler_fetcher.requests.get")
    def test_handler_with_subdirectories(self, mock_get):
        contents_resp = _make_response(200, self._parent_listing())
        trees_resp = _make_response(200, {"sha": TREE_SHA, "tree": self._tree_entries(), "truncated": False})
        raw_resp = _make_response(200)
        raw_resp.content = b"# content"

        mock_get.side_effect = _make_get_side_effect(contents_resp, trees_resp, raw_resp)

        result = fetch_handler(HANDLER, self.storage)

        dest = self.storage / HANDLER
        self.assertEqual(result, dest)
        self.assertTrue((dest / "__init__.py").exists())
        self.assertTrue((dest / "tests" / "__init__.py").exists())
        self.assertTrue((dest / "tests" / "test_elasticsearch_handler.py").exists())

    @patch("mindsdb.integrations.utilities.community_handler_fetcher.requests.get")
    def test_404_handler_not_found(self, mock_get):
        mock_get.return_value = _make_response(404)

        result = fetch_handler(HANDLER, self.storage)

        self.assertIsNone(result)
        # tmp dir must not be left behind
        self.assertFalse((self.storage / f".tmp_{HANDLER}").exists())

    @patch("mindsdb.integrations.utilities.community_handler_fetcher.requests.get")
    def test_atomic_rename_cleanup_on_failure(self, mock_get):
        contents_resp = _make_response(200, self._parent_listing())
        trees_resp = _make_response(200, {"sha": TREE_SHA, "tree": self._tree_entries(), "truncated": False})
        # Simulate a download failure for raw files
        raw_resp = _make_response(500)
        raw_resp.raise_for_status.side_effect = requests.HTTPError("500")

        mock_get.side_effect = _make_get_side_effect(contents_resp, trees_resp, raw_resp)

        with self.assertRaises(RuntimeError):
            fetch_handler(HANDLER, self.storage)

        # tmp dir must be cleaned up after the exception
        self.assertFalse((self.storage / f".tmp_{HANDLER}").exists())
        # dest dir must not exist either
        self.assertFalse((self.storage / HANDLER).exists())

    def test_existing_handler_skips_fetch(self):
        dest = self.storage / HANDLER
        dest.mkdir(parents=True)
        (dest / "__init__.py").write_text("# existing")

        with patch("mindsdb.integrations.utilities.community_handler_fetcher.requests.get") as mock_get:
            result = fetch_handler(HANDLER, self.storage)

        self.assertEqual(result, dest)
        mock_get.assert_not_called()

    @patch("mindsdb.integrations.utilities.community_handler_fetcher.requests.get")
    def test_max_depth_files_not_written(self, mock_get):
        deep_path = "a/b/c/d/deep.py"
        tree_entries = [
            {"path": "__init__.py", "type": "blob", "sha": "s1", "size": 0},
            {"path": deep_path, "type": "blob", "sha": "s2", "size": 99},
        ]
        contents_resp = _make_response(200, self._parent_listing())
        trees_resp = _make_response(200, {"sha": TREE_SHA, "tree": tree_entries, "truncated": False})
        raw_resp = _make_response(200)
        raw_resp.content = b""

        mock_get.side_effect = _make_get_side_effect(contents_resp, trees_resp, raw_resp)

        fetch_handler(HANDLER, self.storage)

        dest = self.storage / HANDLER
        self.assertTrue((dest / "__init__.py").exists())
        self.assertFalse((dest / deep_path).exists())

    @patch("mindsdb.integrations.utilities.community_handler_fetcher.requests.get")
    def test_truncated_tree_warning_propagates(self, mock_get):
        contents_resp = _make_response(200, self._parent_listing())
        trees_resp = _make_response(200, {"sha": TREE_SHA, "tree": [], "truncated": True})

        mock_get.side_effect = _make_get_side_effect(contents_resp, trees_resp)

        with self.assertLogs("mindsdb.integrations.utilities.community_handler_fetcher", level="WARNING") as cm:
            fetch_handler(HANDLER, self.storage)

        self.assertTrue(any("truncated" in line for line in cm.output))


if __name__ == "__main__":
    unittest.main()
