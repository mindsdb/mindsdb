import tempfile
from http import HTTPStatus
from pathlib import Path
from unittest.mock import patch


def test_icon_builtin_handler(client):
    """
    A built-in handler with a registered icon and a valid local path must
    return the icon file (HTTP 200).
    """
    with tempfile.TemporaryDirectory() as tmp:
        icon_file = Path(tmp) / "icon.svg"
        icon_file.write_text("<svg/>")

        meta = {
            "path": Path(tmp),
            "icon": {"name": "icon.svg", "type": "svg", "data": "<svg/>"},
            "import": {"success": True, "error_message": None},
        }

        with patch.object(
            client.application.integration_controller,
            "get_handlers_metadata",
            return_value={"mysql": meta},
        ):
            response = client.get("/api/handlers/mysql/icon", follow_redirects=True)

        status_code = response.status_code
        response.close()

    assert status_code == HTTPStatus.OK


def test_icon_community_stub_no_path(client):
    """
    An unfetched community handler stub (path=None, no 'icon' key) must
    return HTTP 404 cleanly — no exception should propagate.
    """
    meta = {
        "path": None,
        "import": {
            "success": None,
            "error_message": None,
            "folder": "github_handler",
        },
        "name": "github",
        "support_level": "community",
    }

    with patch.object(
        client.application.integration_controller,
        "get_handlers_metadata",
        return_value={"github": meta},
    ):
        response = client.get("/api/handlers/github/icon", follow_redirects=True)

    assert response.status_code == HTTPStatus.NOT_FOUND


def test_icon_unknown_handler(client):
    """
    A request for an icon of an unknown handler must return HTTP 404.
    """
    with patch.object(
        client.application.integration_controller,
        "get_handlers_metadata",
        return_value={},
    ):
        response = client.get("/api/handlers/does_not_exist/icon", follow_redirects=True)

    assert response.status_code == HTTPStatus.NOT_FOUND


def test_handlers_list_skips_none_meta(client):
    """
    The listing endpoint must not crash when get_handlers_import_status()
    returns None for a handler (e.g. an unfetched community handler that
    failed to load). The None entry is silently skipped and the remaining
    handlers are returned normally.
    """
    mysql_meta = {
        "path": None,
        "import": {"success": True, "error_message": None, "folder": "mysql_handler"},
        "name": "mysql",
        "type": "data",
        "title": "MySQL",
        "description": "MySQL handler",
        "permanent": False,
        "connection_args": None,
        "class_type": "sql",
        "support_level": "community",
        "icon": None,
    }

    with patch.object(
        client.application.integration_controller,
        "get_handlers_import_status",
        return_value={"broken_community": None, "mysql": mysql_meta},
    ):
        response = client.get("/api/handlers/", follow_redirects=True)

    assert response.status_code == HTTPStatus.OK
    names = [h["name"] for h in response.get_json()]
    assert "mysql" in names
    assert "broken_community" not in names
