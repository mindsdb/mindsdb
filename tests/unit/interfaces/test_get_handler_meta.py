"""
Unit tests for IntegrationController.get_handler_meta() focusing on the
handler_folder=None crash fix for community handler stubs.

Covered scenarios:
  1. Community stub (path=None), no handler_folder passed → folder derived from stub metadata.
  2. Community stub (path=None), explicit handler_folder passed → explicit folder used as-is.
  3. Non-community (built-in) handler with path set → fetch path never triggered.
  4. Community stub whose "import.folder" is also None (malformed entry) → graceful None return.
"""

import threading
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch


def _make_controller():
    """
    Return an IntegrationController instance with _load_handler_modules skipped
    so no real filesystem / network access happens during construction.
    """
    from mindsdb.interfaces.database.integrations import IntegrationController

    with patch.object(IntegrationController, "_load_handler_modules"):
        ctrl = IntegrationController()

    # Minimal attributes that other methods rely on.
    ctrl.handler_modules = {}
    ctrl.handlers_import_status = {}
    ctrl.handlers_cache = MagicMock()
    ctrl._import_lock = threading.Lock()
    ctrl._community_handlers_dir = None
    return ctrl


def _community_stub(handler_name: str, folder: str | None = None):
    """Build a community handler stub as created by _load_handler_modules."""
    from mindsdb.integrations.libs.const import HANDLER_SUPPORT_LEVEL

    return {
        "path": None,
        "import": {
            "success": None,
            "error_message": None,
            "folder": folder if folder is not None else f"{handler_name}_handler",
            "dependencies": [],
        },
        "name": handler_name,
        "title": handler_name.capitalize(),
        "description": "",
        "permanent": False,
        "connection_args": None,
        "class_type": None,
        "type": None,
        "support_level": HANDLER_SUPPORT_LEVEL.COMMUNITY,
    }


def _builtin_stub(handler_name: str, handler_path: Path):
    """Build a built-in handler stub as created by _register_handler_dir."""
    return {
        "path": handler_path,
        "import": {
            "success": True,
            "error_message": None,
            "folder": handler_path.name,
            "dependencies": [],
        },
        "name": handler_name,
        "permanent": False,
        "connection_args": None,
        "class_type": None,
        "type": None,
        "support_level": None,
        "community": False,
    }


class TestGetHandlerMetaCommunityFolderFallback(unittest.TestCase):
    """get_handler_meta() derives handler_folder from stub metadata when None."""

    def setUp(self):
        self.ctrl = _make_controller()

    def test_community_stub_folder_derived_from_metadata(self):
        """
        When handler_folder is not supplied, get_handler_meta() must read
        "import.folder" from the stub and pass it to _fetch_community_handler.
        """
        stub = _community_stub("github", folder="github_handler")
        self.ctrl.handlers_import_status["github"] = stub

        fetched_meta = {**stub, "path": Path("/tmp/github_handler")}
        fetched_meta["import"] = {**stub["import"], "success": True}

        with patch.object(self.ctrl, "_fetch_community_handler", return_value=fetched_meta) as mock_fetch:
            result = self.ctrl.get_handler_meta("github")  # no handler_folder

        mock_fetch.assert_called_once_with("github", "github_handler")
        self.assertIsNotNone(result)

    def test_community_stub_explicit_folder_not_overridden(self):
        """
        When handler_folder is explicitly provided, it must be forwarded as-is
        and the stub metadata must not override it.
        """
        stub = _community_stub("github", folder="github_handler")
        self.ctrl.handlers_import_status["github"] = stub

        fetched_meta = {**stub, "path": Path("/tmp/custom_dir")}
        fetched_meta["import"] = {**stub["import"], "success": True}

        with patch.object(self.ctrl, "_fetch_community_handler", return_value=fetched_meta) as mock_fetch:
            result = self.ctrl.get_handler_meta("github", handler_folder="custom_dir")

        mock_fetch.assert_called_once_with("github", "custom_dir")
        self.assertIsNotNone(result)

    def test_builtin_handler_fetch_path_not_triggered(self):
        """
        A built-in handler with a real path must not trigger the community fetch
        path regardless of the handler_folder argument.
        """
        stub = _builtin_stub("mysql", Path("/opt/mindsdb/handlers/mysql_handler"))
        self.ctrl.handlers_import_status["mysql"] = stub

        with (
            patch.object(self.ctrl, "_fetch_community_handler") as mock_fetch,
            patch.object(self.ctrl, "import_handler", return_value=stub),
        ):
            result = self.ctrl.get_handler_meta("mysql")

        mock_fetch.assert_not_called()
        self.assertIsNotNone(result)

    def test_community_stub_missing_folder_returns_none_gracefully(self):
        """
        If the stub's "import.folder" is also None (malformed index entry),
        the guard in get_handler_meta() must return None immediately — before
        _fetch_community_handler is ever called — to avoid a TypeError from
        fetch_handler(None, storage_dir).
        """
        stub = _community_stub("broken")
        stub["import"]["folder"] = None  # simulate malformed entry
        self.ctrl.handlers_import_status["broken"] = stub

        with patch.object(self.ctrl, "_fetch_community_handler") as mock_fetch:
            result = self.ctrl.get_handler_meta("broken")  # no handler_folder

        mock_fetch.assert_not_called()  # guard exits before reaching _fetch_community_handler
        self.assertIsNone(result)

    def test_unknown_handler_returns_none(self):
        """get_handler_meta() for a completely unknown handler name returns None."""
        result = self.ctrl.get_handler_meta("does_not_exist")
        self.assertIsNone(result)


class TestGetHandlersImportStatus(unittest.TestCase):
    """get_handlers_import_status() must not fetch/import community stubs."""

    def setUp(self):
        self.ctrl = _make_controller()

    def test_community_stub_not_fetched_during_listing(self):
        """
        Community stubs (support_level="community", path=None) must not trigger
        _fetch_community_handler() or import_handler() during listing.
        """
        stub = _community_stub("github", folder="github_handler")
        self.ctrl.handlers_import_status["github"] = stub

        with (
            patch.object(self.ctrl, "_fetch_community_handler") as mock_fetch,
            patch.object(self.ctrl, "import_handler") as mock_import,
        ):
            self.ctrl.get_handlers_import_status()

        mock_fetch.assert_not_called()
        mock_import.assert_not_called()

    def test_community_stub_metadata_returned_in_listing(self):
        """
        Stub metadata must be present in the result so the UI can render the
        handler entry without a fetch having occurred.
        """
        stub = _community_stub("github", folder="github_handler")
        self.ctrl.handlers_import_status["github"] = stub

        with (
            patch.object(self.ctrl, "_fetch_community_handler"),
            patch.object(self.ctrl, "import_handler"),
        ):
            result = self.ctrl.get_handlers_import_status()

        self.assertIn("github", result)
        meta = result["github"]
        self.assertEqual(meta["name"], "github")
        self.assertEqual(meta["support_level"], "community")
        self.assertIsNone(meta["path"])
        self.assertIsNotNone(meta["import"])

    def test_non_community_handler_uses_get_handler_meta(self):
        """
        Built-in handlers (path != None) must still go through get_handler_meta()
        so that lazy import is triggered if needed.
        """
        stub = _builtin_stub("mysql", Path("/opt/mindsdb/handlers/mysql_handler"))
        self.ctrl.handlers_import_status["mysql"] = stub

        with patch.object(self.ctrl, "get_handler_meta", return_value=stub) as mock_meta:
            self.ctrl.get_handlers_import_status()

        mock_meta.assert_called_once_with("mysql", stub["import"]["folder"])

    def test_fetched_community_handler_uses_get_handler_meta(self):
        """
        A community handler that has already been fetched (path != None) must
        also go through get_handler_meta() — the early-return guard only applies
        when path is None.
        """
        stub = _community_stub("github", folder="github_handler")
        stub["path"] = Path("/tmp/community_handlers/github_handler")
        stub["import"]["success"] = True
        self.ctrl.handlers_import_status["github"] = stub

        with patch.object(self.ctrl, "get_handler_meta", return_value=stub) as mock_meta:
            self.ctrl.get_handlers_import_status()

        mock_meta.assert_called_once_with("github", "github_handler")


if __name__ == "__main__":
    unittest.main()
