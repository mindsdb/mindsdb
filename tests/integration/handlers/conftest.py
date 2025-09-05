import pytest
import sys
import json
import logging
import time
import mindsdb_sdk
from pathlib import Path
from dotenv import load_dotenv
from typing import Generator, Any

from tests.integration.handlers.utils import config
from tests.integration.handlers.utils.helpers import get_handlers_info, build_parameters_clause

# Define project_root relative to this conftest's location for logging.
project_root = Path(__file__).parent.parent.parent.parent


# --- Pytest Hooks for DSI ---

def pytest_configure(config):
    config.addinivalue_line("markers", "dsi: mark test as part of the DSI framework")
    if config.getoption("--run-dsi-tests"):
        logging.info("--- DSI: Loading environment variables ---")
        if not load_dotenv(override=True):
            logging.warning("DSI: Could not find .env file. Using system variables.")
        else:
            logging.info("DSI: Successfully loaded environment variables from .env file.")


# --- DSI Framework Fixtures ---

@pytest.fixture(scope="session")
def query_log_data():
    """A session-scoped dictionary to store all query logs."""
    return {}


@pytest.fixture(scope="session")
def mindsdb_server(query_log_data) -> Generator[Any, None, None]:
    """
    Establishes a connection to the MindsDB SDK and patches the `query` method
    to automatically log all queries.
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

    # --- ENHANCED MONKEY-PATCHING LOGIC ---
    original_query_method = server.query
    # Get the class of the object returned by server.query()
    original_query_class = type(original_query_method("SELECT 1"))

    # Create a new class that inherits from the original Query class
    class PatchedQuery(original_query_class):
            def fetch(self, *args, **kwargs):
                handler_name = "dsi_test"
                start_time = time.time()
                actual_response = None
                error = None
                try:
                    # Call the original fetch method
                    response_df = super().fetch(*args, **kwargs)
                    if response_df is not None:
                        actual_response = response_df.to_json(orient='records') if not response_df.empty else '[]'
                    return response_df
                # Catch only the specific error the SDK raises for query failures.
                except RuntimeError as e:
                    error = str(e)
                    raise # Re-raise to ensure the test handles the failure.
                finally:
                    duration = time.time() - start_time
                    if handler_name not in query_log_data:
                        query_log_data[handler_name] = []
                    query_log_data[handler_name].append({
                        'query': self.sql, 'duration': round(duration, 4),
                        'actual_response': actual_response, 'error': error
                    })


    # This new function will create a query object and then change its class to our patched version
    def patched_query_constructor(sql_query: str):
        query_object = original_query_method(sql_query)
        query_object.__class__ = PatchedQuery
        return query_object

    # Replace the original server.query with our new constructor
    server.query = patched_query_constructor
    # --- END OF PATCHING LOGIC ---

    yield server

    # Teardown: write the log file.
    reports_dir = project_root / 'reports'
    reports_dir.mkdir(exist_ok=True)
    log_filepath = reports_dir / 'all_handlers_query_log.json'
    with open(log_filepath, 'w') as f:
        json.dump(query_log_data, f, indent=4)
    logging.info(f"DSI: Full query log saved to {log_filepath}")


@pytest.fixture(scope="session")
def session_databases(mindsdb_server):
    """Creates and tears down databases for each handler for the test session."""
    created_dbs = {}
    all_handlers, _ = get_handlers_info(mindsdb_server)

    for handler_info in all_handlers:
        handler_name = handler_info['name']
        db_name = f"test_session_{handler_name}"
        params_clause, skip_reason = build_parameters_clause(handler_name, handler_info['connection_args'])

        if skip_reason:
            logging.warning(f"DSI: Skipping database creation for {handler_name}: {skip_reason}")
            continue

        try:
            mindsdb_server.query(f"DROP DATABASE IF EXISTS {db_name};").fetch()
            create_query = f"CREATE DATABASE {db_name} WITH ENGINE = '{handler_name}', PARAMETERS = {params_clause};"
            mindsdb_server.query(create_query).fetch()
            created_dbs[handler_name] = db_name
            logging.info(f"DSI: Successfully created database '{db_name}' for handler '{handler_name}'.")

        # Catch only the specific error the SDK raises for query failures.
        except RuntimeError as e:
            logging.error(f"DSI: Failed to create database for {handler_name}: {e}")
            raise  # Re-raise to ensure the test fails as expected.

    yield created_dbs

    # Teardown: drop all created databases.
    logging.info("--- DSI: Tearing down session databases ---")
    for db_name in created_dbs.values():
        try:
            mindsdb_server.query(f"DROP DATABASE IF EXISTS {db_name};").fetch()
            logging.info(f"DSI: Successfully dropped database {db_name}.")
        except RuntimeError as e:
            logging.error(f"DSI: Failed to drop database {db_name}: {e}")