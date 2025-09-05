import pytest
import logging
import json
import time
from pathlib import Path
from typing import Dict, Any, List

# Import helpers from the central helpers.py file
from tests.integration.handlers.utils.helpers import get_handlers_info, build_parameters_clause, connect_to_mindsdb

# --- Test Case Generation Logic ---

def generate_test_cases_for_parametrization() -> List[Dict[str, Any]]:
    """
    Generates test case definitions for all handlers to be used by pytest.parametrize.
    Connects to MindsDB temporarily just for test case discovery.
    """
    try:
        # We need a temporary server connection to discover handlers and tables.
        # The actual tests will use the 'mindsdb_server' fixture from conftest.
        server = connect_to_mindsdb()
    except ConnectionError as e:
        # If we can't connect, we can't generate tests.
        return [{
            'handler_name': 'mindsdb_connection_check',
            'test_type': 'setup_failure',
            'reason': f"Failed to connect to MindsDB to generate tests. Please ensure the server is running. Error: {e}"
        }]

    handlers_to_test, uninstalled_handlers = get_handlers_info(server)
    test_cases = []

    for handler in handlers_to_test:
        handler_name = handler['name']
        config_path = Path(__file__).parent / 'configs' / f'{handler_name}.json'
        params_clause, skip_reason = build_parameters_clause(handler_name, handler['connection_args'])

        if skip_reason:
            test_cases.append({
                'handler_name': handler_name,
                'test_type': 'skipped_test',
                'reason': skip_reason
            })
            continue

        temp_db_name = f"test_discover_{handler_name}"
        try:
            server.query(f"DROP DATABASE IF EXISTS {temp_db_name};").fetch()
            create_query = f"CREATE DATABASE {temp_db_name} WITH ENGINE = '{handler_name}', PARAMETERS = {params_clause};"
            server.query(create_query).fetch()

            tables_df = server.query(f"SHOW TABLES FROM {temp_db_name};").fetch()
            table_name_column = tables_df.columns[0]
            for table_name in tables_df[table_name_column].head(2):
                test_cases.append({
                    'handler_name': handler_name,
                    'query_template': f"SELECT * FROM {{db_name}}.{table_name} LIMIT 1;",
                    'test_type': 'autodiscovery_schema'
                })
        except Exception as e:
            error_message = f"Failed to discover tables for handler '{handler_name}': {e}"
            logging.error(f"DSI: {error_message}")
            test_cases.append({
                'handler_name': handler_name,
                'test_type': 'skipped_test',
                'reason': error_message
            })
            continue
        finally:
            server.query(f"DROP DATABASE IF EXISTS {temp_db_name};").fetch()

        if config_path.is_file():
            with open(config_path, 'r') as f:
                test_config = json.load(f)
            if 'queries' in test_config:
                for query_name, query_details in test_config['queries'].items():
                    test_cases.append({
                        'handler_name': handler_name,
                        'query_template': query_details['query'],
                        'test_type': 'custom',
                        'details': query_details
                    })
            if 'negative_tests' in test_config:
                for neg_test in test_config['negative_tests']:
                    test_cases.append({
                        'handler_name': handler_name,
                        'query_template': neg_test['query'],
                        'test_type': 'negative',
                        'details': neg_test
                    })

    for handler_name in uninstalled_handlers:
        test_cases.append({
            'handler_name': handler_name,
            'test_type': 'skipped_test',
            'reason': f"Handler {handler_name} not installed."
        })

    return test_cases


# --- Main Parametrized Test Function ---

def idfn(test_case):
    return f"{test_case.get('handler_name', 'unknown')}-{test_case.get('test_type', 'unknown')}"

@pytest.mark.dsi
@pytest.mark.parametrize("test_case", generate_test_cases_for_parametrization(), ids=idfn)
def test_handler_integrations(mindsdb_server, session_databases, test_case):
    """
    This single test function runs all DSI tests based on the parametrized test_case.
    Query logging is handled automatically by the patched mindsdb_server fixture.
    """
    handler_name = test_case['handler_name']
    test_type = test_case['test_type']

    if test_type == 'setup_failure':
        pytest.fail(test_case['reason'], pytrace=False)

    if test_type == 'skipped_test':
        pytest.skip(test_case.get('reason', 'Unknown reason'))

    if handler_name not in session_databases:
        pytest.skip(f"Database for handler '{handler_name}' was not set up successfully.")

    db_name = session_databases[handler_name]
    query_template = test_case.get('query_template', '')
    query = query_template.format(db_name=db_name)
    logging.info(f"Running test for handler '{handler_name}': {query}")

    if test_type == 'negative':
        details = test_case['details']
        with pytest.raises(Exception) as excinfo:
            mindsdb_server.query(query).fetch()

        error_str = str(excinfo.value)
        assert details['expected_error'] in error_str, f'Negative test failed. Expected error substring "{details["expected_error"]}" not found in actual error: {error_str}'
    else:  # Positive tests
        select_df = mindsdb_server.query(query).fetch()
        assert select_df is not None, f"Query '{query}' returned None instead of a DataFrame."

        if test_type == 'autodiscovery_schema':
            assert not select_df.columns.empty, f"Schema discovery failed for '{query}'. No columns were found."

        if test_type == 'custom':
            details = test_case['details']
            assert not select_df.empty, f"Custom query '{query}' returned no results."

            expected_columns = set(details['expected_columns'])
            actual_columns = set(select_df.columns)
            missing_columns = expected_columns - actual_columns
            assert not missing_columns, f'Custom query failed for {query}. Missing expected columns: {missing_columns}'

            if 'exact_rows' in details:
                assert len(select_df) == details['exact_rows']
            else:
                assert len(select_df) >= details.get('min_rows', 1)