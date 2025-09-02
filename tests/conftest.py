import pytest
import os
import sys
import json
import time
import logging
from pathlib import Path
from dotenv import load_dotenv
from typing import Generator, Any

# DSI: Add project root to path for script imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


# --- Modified: Existing and New Pytest Hooks ---

def pytest_addoption(parser):
    parser.addoption("--runslow", action="store_true", default=False, help="run slow tests")
    parser.addoption("--run-dsi-tests", action="store_true", default=False, help="run DSI integration tests")


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow to run")
    config.addinivalue_line("markers", "dsi: mark test as part of the DSI framework")

    if config.getoption("--run-dsi-tests"):
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
            pytest.fail(f"DSI: A critical error occurred while loading the .env file: {e}", pytrace=False)
        
        logging.info("--- DSI: Test generation will be handled by pytest parametrization ---")


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--runslow"):
        skip_slow = pytest.mark.skip(reason="need --runslow option to run")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)

    if not config.getoption("--run-dsi-tests"):
        items[:] = [item for item in items if "dsi" not in item.keywords]


def pytest_sessionfinish(session: Any, exitstatus: int) -> None:
    if session.config.getoption("--run-dsi-tests"):
        logging.info("--- DSI: Test session finished. No generated files to clean up. ---")


# --- DSI Framework Fixtures with Improved Error Handling ---

@pytest.fixture(scope="session")
def mindsdb_server(pytestconfig) -> Generator[Any, None, None]:
    if not pytestconfig.getoption("--run-dsi-tests"):
        yield None
        return
    
    from tests.integration.handlers.utils.helpers import connect_to_mindsdb
        
    try:
        server = connect_to_mindsdb()
        yield server
    except Exception as e:
        pytest.fail(
            f"DSI: Failed to connect to MindsDB via SDK. Please ensure MindsDB is running and accessible. Error: {e}", 
            pytrace=False
        )


@pytest.fixture(scope="session")
def query_logger(pytestconfig):
    if not pytestconfig.getoption("--run-dsi-tests"):
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
    
    try:
        reports_dir = project_root / 'reports'
        reports_dir.mkdir(exist_ok=True)
        log_filepath = reports_dir / 'all_handlers_query_log.json'
        with open(log_filepath, 'w') as f:
            json.dump(full_query_log, f, indent=4)
        logging.info(f"DSI: Full query log saved to {log_filepath}")
    except Exception as e:
        logging.error(f"DSI: Failed to save the query log file: {e}")


@pytest.fixture(scope="session")
def session_databases(mindsdb_server, pytestconfig):
    if not pytestconfig.getoption("--run-dsi-tests") or mindsdb_server is None:
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
            logging.warning(f"DSI: Skipping database creation for {handler_name}: {skip_reason}")
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
            logging.info(f"DSI: Successfully dropped database {db_name}.")
        except Exception as e:
            logging.error(f"DSI: Failed to drop database {db_name}: {e}")