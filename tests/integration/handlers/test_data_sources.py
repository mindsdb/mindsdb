import pytest
import logging
import json
import os
from pathlib import Path
from typing import Dict, Any, List

# --- Test Case Generation (I/O-Free, Filtered, and Deterministic) ---

def get_handlers_to_test() -> List[str]:
    """
    Determines which handlers to test.
    1. If the HANDLERS_TO_TEST environment variable is set, it uses that list.
    2. Otherwise, it discovers handlers by looking for non-example config files.
    """
    env_handlers = os.environ.get("HANDLERS_TO_TEST")
    if env_handlers:
        return [h.strip() for h in env_handlers.split(',')]

    # Fallback to discovering from config files, excluding examples
    configs_path = Path(__file__).parent / "configs"
    return sorted([p.stem for p in configs_path.glob("*.json") if not p.name.startswith("example.")])


def generate_test_cases_for_parametrization() -> List[Dict[str, Any]]:
    """
    Generates a deterministic list of test cases for the specified handlers.
    This function is I/O-free and safe for pytest-xdist collection.
    """
    test_cases = []
    configs_path = Path(__file__).parent / "configs"
    handlers_to_run = get_handlers_to_test()

    for name in handlers_to_run:
        config_path = configs_path / f"{name}.json"
        
        # Add a placeholder for autodiscovery for every handler
        test_cases.append({"handler_name": name, "test_type": "autodiscovery"})

        # Generate specific tests from the handler's config file
        if config_path.is_file():
            with open(config_path, "r") as f:
                test_config = json.load(f)
            
            if "queries" in test_config:
                for query_name in sorted(test_config["queries"].keys()):
                    test_cases.append({"handler_name": name, "test_type": "custom", "query_name": query_name})

            if "negative_tests" in test_config:
                for i in range(len(test_config["negative_tests"])):
                    test_cases.append({"handler_name": name, "test_type": "negative", "test_index": i})
    
    return test_cases

# --- Main Parametrized Test Function ---

def idfn(test_case):
    """Generates a unique and descriptive ID for each test case."""
    name = test_case["handler_name"]
    ttype = test_case["test_type"]
    if ttype == "custom":
        return f"{name}-{ttype}-{test_case['query_name']}"
    if ttype == "negative":
        return f"{name}-{ttype}-negative_{test_case['test_index']}"
    return f"{name}-{ttype}"

@pytest.mark.dsi
@pytest.mark.parametrize("test_case", generate_test_cases_for_parametrization(), ids=idfn)
def test_handler_integrations(mindsdb_server, session_databases, test_case):
    """
    This test function is parametrized with individual test cases, restoring the
    original test reporting structure in a way that is safe for parallel execution.
    """
    handler_name = test_case["handler_name"]
    test_type = test_case["test_type"]

    if handler_name not in session_databases:
        pytest.skip(f"Database for handler '{handler_name}' was not set up successfully.")

    db_name = session_databases[handler_name]
    config_path = Path(__file__).parent / "configs" / f"{handler_name}.json"

    # --- Test Execution Logic ---

    if test_type == "autodiscovery":
        tables_df = mindsdb_server.query(f"SHOW TABLES FROM {db_name};").fetch()
        assert not tables_df.empty, "Autodiscovery failed: SHOW TABLES returned no results."

    elif test_type == "custom":
        with open(config_path, "r") as f:
            test_config = json.load(f)
        query_name = test_case["query_name"]
        query_details = test_config["queries"][query_name]
        query = query_details["query"].format(db_name=db_name)
        
        logging.info(f"Running custom query '{query_name}': {query}")
        select_df = mindsdb_server.query(query).fetch()
        assert not select_df.empty, f"Custom query '{query_name}' returned no results."

    elif test_type == "negative":
        with open(config_path, "r") as f:
            test_config = json.load(f)
        test_index = test_case["test_index"]
        neg_test = test_config["negative_tests"][test_index]
        query = neg_test["query"].format(db_name=db_name)

        logging.info(f"Running negative test #{test_index}: {query}")
        with pytest.raises(RuntimeError) as excinfo:
            mindsdb_server.query(query).fetch()
        
        error_str = str(excinfo.value)
        expected_error = neg_test["expected_error"]
        assert expected_error in error_str, (
            f'Negative test failed. Expected error substring "{expected_error}" not found in actual error: {error_str}'
        )