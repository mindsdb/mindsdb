import pytest
import os
import sys
import json
import time
import logging
from pathlib import Path
from dotenv import load_dotenv
from typing import Generator, Any, Tuple, List, Dict

# DSI: Add project root to path for script imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


# --- Modified: Existing and New Pytest Hooks ---

def pytest_addoption(parser):
    # Existing option for MindsDB's test suite
    parser.addoption("--runslow", action="store_true", default=False, help="run slow tests")


def pytest_configure(config):
    """
    Called by pytest before test collection.
    """
    config.addinivalue_line("markers", "slow: mark test as slow to run")
    config.addinivalue_line("markers", "dsi: mark test as part of the DSI framework")

    # --- DSI: Smartly trigger test generation based on the test path ---
    # This checks if the user is targeting the DSI test directory.
    if any('integration/handlers' in str(arg) for arg in config.args):
        # We only import the DSI framework if we need it
        from tests.scripts.generate_tests import main as generate_tests_main

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            force=True
        )

        logging.info("--- DSI: Loading environment variables ---")
        try:
            dotenv_path = project_root / '.env'
            if load_dotenv(dotenv_path=dotenv_path, override=True):
                logging.info(f"DSI: Successfully loaded environment variables from: {dotenv_path}")
            else:
                logging.warning(f"DSI: Could not find .env file at {dotenv_path}. Using system variables.")
        except Exception as e:
            logging.error(f"DSI: An error occurred while loading the .env file: {e}")

        logging.info("--- DSI: Generating integration tests ---")
        try:
            generate_tests_main()
            logging.info("--- DSI: Finished generating integration tests ---")
        except Exception as e:
            pytest.fail(f"DSI: Failed to generate tests during session start: {e}")


def pytest_collection_modifyitems(config, items):
    # The --runslow logic remains for the main test suite.
    if not config.getoption("--runslow"):
        skip_slow = pytest.mark.skip(reason="need --runslow option to run")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)


def pytest_sessionfinish(session: Any, exitstatus: int) -> None:
    """
    Called by pytest after the test session finishes to clean up generated files.
    """
    # Only clean up if the tests were generated in the first place.
    if any('integration/handlers' in str(arg) for arg in session.config.args):
        logging.info("--- DSI: Cleaning up generated test file ---")
        try:
            generated_test_file = project_root / 'tests' / 'integration' / 'handlers' / 'test_generated_integrations.py'
            if generated_test_file.exists():
                os.remove(generated_test_file)
                logging.info(f"DSI: Successfully removed {generated_test_file}")
        except Exception as e:
            logging.error(f"DSI: Failed to clean up generated test file: {e}")


# --- New: DSI Framework Fixtures ---

@pytest.fixture(scope="session")
def mindsdb_server(pytestconfig) -> Generator[Any, None, None]:
    if not any('integration/handlers' in str(arg) for arg in pytestconfig.args):
        yield None
        return
    
    from tests.integration.handlers.utils.helpers import connect_to_mindsdb
        
    try:
        server = connect_to_mindsdb()
        yield server
    except Exception as e:
        pytest.fail(f"DSI: Failed to connect to MindsDB via SDK: {e}")


@pytest.fixture(scope="session")
def query_logger(pytestconfig):
    if not any('integration/handlers' in str(arg) for arg in pytestconfig.args):
        yield None
        return

    full_query_log = {}
    def log_query(handler_name, query_string, duration, actual_response=None, error=None):
        if handler_name not in full_query_log:
            full_query_log[handler_name] = []
        full_query_log[handler_name].append({
            'query': query_string, 'duration': round(duration, 4),
            'actual_response': actual_response, 'error': error
        })
    yield log_query
    reports_dir = project_root / 'reports'
    reports_dir.mkdir(exist_ok=True)
    log_filepath = reports_dir / 'all_handlers_query_log.json'
    with open(log_filepath, 'w') as f:
        json.dump(full_query_log, f, indent=4)
    logging.info(f"DSI: Full query log saved to {log_filepath}")


@pytest.fixture(scope="session")
def session_databases(mindsdb_server, pytestconfig):
    if not any('integration/handlers' in str(arg) for arg in pytestconfig.args):
        yield None
        return

    from tests.integration.handlers.utils.helpers import get_handlers_info, build_parameters_clause

    created_dbs = {}
    all_handlers, _ = get_handlers_info(mindsdb_server)
    for handler_info in all_handlers:
        handler_name = handler_info['name']
        db_name = f"test_session_{handler_name}"
        params_clause, skip_reason = build_parameters_clause(handler_name, handler_info['connection_args'])
        if skip_reason:
            continue
        try:
            mindsdb_server.query(f"DROP DATABASE IF EXISTS {db_name};").fetch()
            create_query = f"CREATE DATABASE {db_name} WITH ENGINE = '{handler_name}', PARAMETERS = {params_clause};"
            mindsdb_server.query(create_query).fetch()
            created_dbs[handler_name] = db_name
            logging.info(f"DSI: Successfully created database '{db_name}' for handler '{handler_name}'.")
        except Exception as e:
            logging.error(f"DSI: Failed to create database for {handler_name}: {e}")
    yield created_dbs
    logging.info("--- DSI: Tearing down session databases ---")
    for db_name in created_dbs.values():
        try:
            mindsdb_server.query(f"DROP DATABASE IF EXISTS {db_name};").fetch()
        except Exception as e:
            logging.error(f"DSI: Failed to drop database {db_name}: {e}")