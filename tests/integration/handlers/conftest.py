import pytest
import json
import logging
import time
import mindsdb_sdk
from pathlib import Path
from dotenv import load_dotenv
from typing import Generator, Any
from functools import wraps
from tests.integration.handlers.utils import config
from tests.integration.handlers.utils.helpers import get_handlers_info, build_parameters_clause


project_root = Path(__file__).parent.parent.parent.parent


def pytest_addoption(parser):
    """
    Adds command-line options specific to DSI tests.
    """
    parser.addoption("--run-dsi-tests", action="store_true", default=False, help="run DSI integration tests")


def pytest_configure(config):
    """
    Registers the custom 'dsi' mark and loads the .env file.
    """
    config.addinivalue_line("markers", "dsi: mark test as part of the DSI framework")
    if config.getoption("--run-dsi-tests"):
        logging.info("--- DSI: Loading environment variables ---")
        if not load_dotenv(override=True):
            logging.warning("DSI: Could not find .env file. Using system variables.")
        else:
            logging.info("DSI: Successfully loaded environment variables from .env file.")


@pytest.fixture(scope="session")
def query_log_data():
    """A session-scoped dictionary to store all query logs."""
    return {}


def pytest_collection_modifyitems(config, items):
    """
    Modifies DSI test items after collection to skip them if the flag is not provided.
    """
    if not config.getoption("--run-dsi-tests"):
        skip_dsi = pytest.mark.skip(reason="need --run-dsi-tests option to run")
        for item in items:
            if "dsi" in item.keywords:
                item.add_marker(skip_dsi)


@pytest.fixture(scope="session")
def mindsdb_server(query_log_data) -> Generator[Any, None, None]:
    """
    Establishes a connection to the MindsDB SDK and patches query objects
    to automatically log when .fetch() is called.
    """
    logging.info("--- DSI: Attempting to connect to SDK ---")
    url = f"{config.MINDSDB_PROTOCOL}://{config.MINDSDB_HOST}:{config.MINDSDB_PORT}"

    try:
        if config.MINDSDB_USER and config.MINDSDB_PASSWORD:
            server = mindsdb_sdk.connect(url=url, login=config.MINDSDB_USER, password=config.MINDSDB_PASSWORD)
        else:
            server = mindsdb_sdk.connect(url)
        logging.info("DSI: Successfully connected to MindsDB via SDK.")
    except ConnectionError as e:
        pytest.fail(f"DSI: Failed to connect to MindsDB. Error: {e}", pytrace=False)

    original_query_method = server.query

    def patched_query_constructor(sql_query: str):
        query_object = original_query_method(sql_query)
        original_fetch = query_object.fetch

        @wraps(original_fetch)
        def logged_fetch(*args, **kwargs):
            handler_name = "dsi_test"
            start_time = time.time()
            actual_response = None
            error = None
            try:
                response_df = original_fetch(*args, **kwargs)
                if response_df is not None:
                    actual_response = response_df.to_json(orient="records") if not response_df.empty else "[]"
                return response_df
            except RuntimeError as e:
                error = str(e)
                raise
            finally:
                duration = time.time() - start_time
                if handler_name not in query_log_data:
                    query_log_data[handler_name] = []
                query_log_data[handler_name].append(
                    {
                        "query": sql_query,
                        "duration": round(duration, 4),
                        "actual_response": actual_response,
                        "error": error,
                    }
                )

        query_object.fetch = logged_fetch
        return query_object

    server.query = patched_query_constructor
    yield server

    # Teardown: write the log file.
    reports_dir = project_root / "reports"
    reports_dir.mkdir(exist_ok=True)
    log_filepath = reports_dir / "all_handlers_query_log.json"
    with open(log_filepath, "w") as f:
        json.dump(query_log_data, f, indent=4)
    logging.info(f"DSI: Full query log saved to {log_filepath}")


@pytest.fixture(scope="session")
def session_databases(mindsdb_server):
    """Creates and tears down databases for each handler for the test session."""
    created_dbs = {}
    all_handlers, _ = get_handlers_info(mindsdb_server)

    for handler_info in all_handlers:
        handler_name = handler_info["name"]
        db_name = f"test_session_{handler_name}"
        params_clause, skip_reason = build_parameters_clause(handler_name, handler_info["connection_args"])

        if skip_reason:
            logging.warning(f"DSI: Skipping database creation for {handler_name}: {skip_reason}")
            continue

        try:
            mindsdb_server.query(f"DROP DATABASE IF EXISTS {db_name};").fetch()
            create_query = f"CREATE DATABASE {db_name} WITH ENGINE = '{handler_name}', PARAMETERS = {params_clause};"
            mindsdb_server.query(create_query).fetch()
            created_dbs[handler_name] = db_name
            logging.info(f"DSI: Successfully created database '{db_name}' for handler '{handler_name}'.")
        except RuntimeError as e:
            logging.exception(f"DSI: Failed to create database for {handler_name}: {e}")
            raise

    yield created_dbs

    logging.info("--- DSI: Tearing down session databases ---")
    for db_name in created_dbs.values():
        try:
            mindsdb_server.query(f"DROP DATABASE IF EXISTS {db_name};").fetch()
            logging.info(f"DSI: Successfully dropped database {db_name}.")
        except RuntimeError as e:
            logging.exception(f"DSI: Failed to drop database {db_name}: {e}")
