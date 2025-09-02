import logging
import json
from typing import Dict, Any, List, Tuple
import mindsdb_sdk

from tests.integration.handlers.utils import config

def connect_to_mindsdb():
    """
    Reusable function to connect to MindsDB SDK, handling both
    authenticated and unauthenticated scenarios based on .env config.
    """
    logging.info("--- DSI: Attempting to connect to SDK ---")
    try:
        if config.MINDSDB_USER and config.MINDSDB_PASSWORD:
            url = f"{config.MINDSDB_PROTOCOL}://{config.MINDSDB_HOST}:{config.MINDSDB_PORT}"
            logging.info(f"DSI: Connecting SDK with credentials to server at: {url}")
            server = mindsdb_sdk.connect(
                url=url,
                login=config.MINDSDB_USER,
                password=config.MINDSDB_PASSWORD
            )
        else:
            url = f"{config.MINDSDB_PROTOCOL}://{config.MINDSDB_HOST}:{config.MINDSDB_PORT}"
            logging.info(f"DSI: Connecting SDK to server at: {url} (no auth)...")
            server = mindsdb_sdk.connect(url)
            
        logging.info("DSI: Successfully connected to MindsDB via SDK.")
        return server
    except Exception as e:
        logging.error(f"DSI: Failed to connect to MindsDB via SDK: {e}", exc_info=True)
        raise e

def get_handlers_info(mindsdb_server: Any) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Discovers connection arguments for specified handlers and identifies which are not installed.
    """
    try:
        installed_handlers_df = mindsdb_server.query("SELECT NAME, IMPORT_SUCCESS FROM information_schema.handlers WHERE type = 'data'").fetch()
        if installed_handlers_df.empty:
            logging.warning("DSI: Did not discover any installed data handlers on the MindsDB server.")
            installed_handlers = set()
        else:
            installed_handlers = set(installed_handlers_df[installed_handlers_df['IMPORT_SUCCESS']]['NAME'].str.lower())
        
        # Corrected: HANDERS_TO_TEST -> HANDLERS_TO_TEST
        target_handlers_str = config.HANDLERS_TO_TEST
        target_handlers_list = [h.strip().lower() for h in target_handlers_str.split(',') if h.strip()]
        
        uninstalled_handlers = [h for h in target_handlers_list if h not in installed_handlers]
        handlers_to_test = [h for h in target_handlers_list if h in installed_handlers]

        if not handlers_to_test:
            return [], uninstalled_handlers

        in_clause = " AND LOWER(NAME) IN (" + ", ".join(f"'{h}'" for h in handlers_to_test) + ")"
        query = "SELECT NAME, CONNECTION_ARGS FROM information_schema.handlers WHERE type = 'data'" + in_clause
        
        result_df = mindsdb_server.query(query).fetch()

        handlers = []
        if not result_df.empty:
            for _, row in result_df.iterrows():
                handlers.append({
                    "name": row['NAME'],
                    "connection_args": json.loads(row['CONNECTION_ARGS']) if row['CONNECTION_ARGS'] else {}
                })
        return handlers, uninstalled_handlers
    except Exception as e:
        raise Exception(f"Failed to fetch handler information from MindsDB: {e}")

def build_parameters_clause(handler_name: str, connection_args: Dict[str, Any]) -> Tuple[str, str]:
    """
    Builds the PARAMETERS clause for a CREATE DATABASE query using credentials from config.
    """
    creds_variable_name = f"{handler_name.upper()}_CREDS"
    creds = getattr(config, creds_variable_name, None)
    
    if creds is None:
        return None, f"No credential variable named '{creds_variable_name}' found in config.py"

    params_dict = {}
    missing_creds = []
    
    all_possible_keys = set(connection_args.keys()) | set(creds.keys())

    for key in all_possible_keys:
        details = connection_args.get(key, {})
        is_required = details.get('required', False)
        value = creds.get(key)

        if is_required and value is None:
            missing_creds.append(key.upper())
            continue
        
        if value is not None:
            if handler_name.lower() == 'bigquery' and key == 'service_account_json':
                try:
                    params_dict[key] = json.loads(value)
                except (json.JSONDecodeError, TypeError):
                    return None, "The BIGQUERY_SERVICE_ACCOUNT_JSON is not a valid JSON string."
            else:
                params_dict[key] = value

    if missing_creds:
        return None, f"Missing required .env variables for {handler_name}: {', '.join(missing_creds)}"

    return json.dumps(params_dict), None