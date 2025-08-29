import pytest
import logging
import json
from pathlib import Path
from typing import Dict, Any, List

# Import helpers from the central helpers.py file
from tests.integration.handlers.utils.helpers import get_handlers_info, build_parameters_clause

def generate_test_cases(mindsdb_server: Any) -> List[Dict[str, Any]]:
    """
    Generates test case definitions for all handlers.
    """
    handlers_to_test, uninstalled_handlers = get_handlers_info(mindsdb_server)
    test_cases = []

    for handler in handlers_to_test:
        handler_name = handler['name']
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
            mindsdb_server.query(f"DROP DATABASE IF EXISTS {temp_db_name};").fetch()
            create_query = f"CREATE DATABASE {temp_db_name} WITH ENGINE = '{handler_name}', PARAMETERS = {params_clause};"
            mindsdb_server.query(create_query).fetch()
            
            tables_df = mindsdb_server.query(f"SHOW TABLES FROM {temp_db_name};").fetch()
            table_name_column = tables_df.columns[0]
            for table_name in tables_df[table_name_column].head(2):
                test_cases.append({
                    'handler_name': handler_name,
                    'query_template': f"SELECT * FROM {{db_name}}.{table_name} LIMIT 1;",
                    'test_type': 'autodiscovery_schema'
                })
                test_cases.append({
                    'handler_name': handler_name,
                    'query_template': f"SELECT * FROM {{db_name}}.{table_name} LIMIT 5;",
                    'test_type': 'autodiscovery_select'
                })
        except Exception as e:
            error_message = f"Failed to discover tables for handler '{handler_name}': {e}"
            logging.error(f"DSI: {error_message}")
            test_cases.append({
                'handler_name': handler_name,
                'test_type': 'skipped_test',
                'reason': error_message
            })
            continue # Continue to the next handler
        finally:
            mindsdb_server.query(f"DROP DATABASE IF EXISTS {temp_db_name};").fetch()

        config_path = Path(__file__).parent / 'configs' / f'{handler_name}.json'
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