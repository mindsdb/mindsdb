import os
import sys
from http import HTTPStatus
from pathlib import Path
import tempfile
import shutil

import pytest
from flask.testing import FlaskClient
from flask.app import Flask

from mindsdb.api.http.initialize import initialize_app
from mindsdb.migrations import migrate
from mindsdb.interfaces.storage import db
from mindsdb.utilities.config import config
from mindsdb.integrations.libs.process_cache import process_cache


@pytest.fixture(scope="module", autouse=True)
def app():
    """Provide a temporary app instance and ensure cleanup even on setup issues."""
    old_minds_db_con = os.environ.get("MINDSDB_DB_CON")
    old_config_path = os.environ.get("MINDSDB_CONFIG_PATH")
    temp_dir_ctx = None
    try:
        # Use a workspace-local temp dir to avoid platform /tmp permission quirks.
        temp_root = Path.cwd() / "var" / "tmp_http_app"
        temp_root.mkdir(parents=True, exist_ok=True)
        temp_dir = Path(tempfile.mkdtemp(prefix="test_tmp_", dir=temp_root)).resolve()
        temp_dir_ctx = temp_dir  # track for cleanup
        os.environ["MINDSDB_STORAGE_DIR"] = str(temp_dir)
        db_file = (temp_dir / "mindsdb.sqlite3.db").resolve()
        db_file.parent.mkdir(parents=True, exist_ok=True)
        db_file.touch(mode=0o666, exist_ok=True)
        db_path = "sqlite:///" + str(db_file)
        # Need to change env variable for migrate module, since it calls db.init().
        os.environ["MINDSDB_DB_CON"] = db_path
        # Ensure we don't inherit a stale config path from executor tests.
        os.environ.pop("MINDSDB_CONFIG_PATH", None)
        # Alembic/migrations expect the directory/file to exist and be accessible.
        os.chmod(db_file, 0o666)
        config.prepare_env_config()
        config.merge_configs()
        config["gui"]["open_on_start"] = False
        config["gui"]["autoupdate"] = False
        db.init()
        migrate.migrate_to_head()
        app = initialize_app()
        app._mindsdb_temp_dir = temp_dir
        yield app
    finally:
        process_cache.shutdown()
        if old_minds_db_con is not None:
            os.environ["MINDSDB_DB_CON"] = old_minds_db_con
        else:
            os.environ.pop("MINDSDB_DB_CON", None)
        if old_config_path is not None:
            os.environ["MINDSDB_CONFIG_PATH"] = old_config_path
        else:
            os.environ.pop("MINDSDB_CONFIG_PATH", None)
        try:
            if temp_dir_ctx is not None:
                shutil.rmtree(temp_dir_ctx, ignore_errors=True)
        except PermissionError:
            # On Windows temp dirs may fail to clean up; leave them in place.
            pass


@pytest.fixture(scope="module")
def client(app: Flask) -> FlaskClient:
    return app.test_client()


def create_dummy_db(client: FlaskClient, db_name: str):
    temp_dir = client.application._mindsdb_temp_dir
    dummy_data_db_path = os.path.join(temp_dir, "_dummy_data_db")
    response = client.post(
        "/api/sql/query",
        json={
            "query": f'''
                create database {db_name}
                with ENGINE = "dummy_data"
                PARAMETERS = {{"db_path": "{dummy_data_db_path}"}}'''
        },
    )
    assert response.status_code == HTTPStatus.OK
    assert response.json["type"] == "ok"


def create_demo_db(client: FlaskClient):
    example_db_data = {
        "database": {
            "name": "example_db",
            "engine": "postgres",
            "parameters": {
                "user": "demo_user",
                "password": "demo_password",
                "host": "samples.mindsdb.com",
                "port": "5432",
                "database": "demo",
                "schema": "demo_data",
            },
        }
    }
    response = client.post("/api/databases", json=example_db_data, follow_redirects=True)
    assert "201" in response.status


def create_dummy_ml(client: FlaskClient):
    from mindsdb.interfaces.database.integrations import integration_controller

    test_handler_path = Path(__file__).parents[2]
    sys.path.append(str(test_handler_path))

    handler_dir = Path(test_handler_path) / "dummy_ml_handler"

    handler_meta = {
        "import": {
            "success": None,
            "error_message": None,
            "folder": handler_dir.name,
            "dependencies": [],
        },
        "path": handler_dir,
        "name": "dummy_ml",
        "permanent": False,
    }
    integration_controller.handlers_import_status["dummy_ml"] = handler_meta
    integration_controller.import_handler("dummy_ml", "")

    if not integration_controller.get_handler_meta("dummy_ml")["import"]["success"]:
        error = integration_controller.handlers_import_status["dummy_ml"]["import"]["error_message"]
        raise Exception(f"Can not import: {str(handler_dir)}: {error}")

    response = client.post(
        "/api/sql/query",
        json={
            "query": """
                create ml_engine dummy_ml
                from dummy_ml
            """
        },
    )
    assert response.status_code == HTTPStatus.OK
    assert response.json["type"] == "ok"
