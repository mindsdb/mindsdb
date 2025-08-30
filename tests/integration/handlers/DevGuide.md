# 👨‍💻 DSI Testing Framework: A Developer's Guide

Welcome to the MindsDB Data Source Integration (DSI) Testing Framework. This guide will walk you through the architecture, components, and workflows of the framework so you can confidently extend and maintain it.

## 1. Architectural Overview

The framework is designed to be **configuration-driven** and **highly automated**. It operates on a "generate then test" model to avoid common `pytest` collection issues and to remain completely isolated from the main MindsDB test suite.

### High-Level Workflow:

1.  **Configuration**: A developer defines which handlers to test and provides credentials in the `.env` file and `tests/integration/handlers/utils/config.py`. Custom tests can be defined in JSON files.
2.  **Test Generation**: When `pytest` is run on the `tests/integration/handlers/` directory, a hook in `conftest.py` triggers a generation script. This script connects to MindsDB, discovers tables for the configured handlers, and creates a static test file: `test_generated_integrations.py`.
3.  **Test Execution**: `pytest` then discovers and runs the tests in this newly generated file.
4.  **Logging & Reporting**: As the tests run, a detailed JSON log of every query is created. After the session, the generated test file is automatically deleted.
5.  **Data Ingestion**: The JSON reports can be ingested into a PostgreSQL database for analysis.

## 2. Core Components

Understanding the role of each key file is essential to working with the framework.

### `tests/conftest.py`

* **Purpose**: This is the main entry point for `pytest`. It contains all the core fixtures and hooks that manage the test session.
* **Key Fixtures**:
    * `mindsdb_server`: Establishes the connection to the MindsDB server.
    * `session_databases`: Creates and tears down the necessary databases for each test session.
    * `query_logger`: Collects the detailed query logs and writes them to a JSON file at the end of the session.
* **Key Hooks**:
    * `pytest_configure`: Triggers the test generation script *before* test collection if the DSI test directory is targeted.
    * `pytest_sessionfinish`: Cleans up the generated test file after the session is complete.

### `tests/scripts/generate_tests.py`

* **Purpose**: This is the engine of the framework. It's responsible for all the test discovery and generation logic.
* **Workflow**: It connects to MindsDB, reads the configurations, discovers tables for each handler, and then writes the corresponding test functions to the `test_generated_integrations.py` file. It also generates tests that explicitly fail for any misconfigured or uninstalled handlers.

### `tests/integration/handlers/utils/helpers.py`

* **Purpose**: This file contains helper functions shared between the test generation script and `conftest.py` to avoid circular import errors.
* **Key Functions**:
    * `connect_to_mindsdb`: A centralized function to connect to the MindsDB SDK, handling both authenticated and unauthenticated scenarios.
    * `get_handlers_info`: Fetches the list of installed handlers from MindsDB.
    * `build_parameters_clause`: Constructs the `PARAMETERS` clause for the `CREATE DATABASE` query.

## 3. How to Extend the Framework

Here are the two most common tasks you'll encounter as a developer.

### Adding a New Data Handler

Adding a new handler is a configuration-only change and requires no new code.

1.  **Add Credentials to `.env`**: Add the necessary environment variables for the new handler.
2.  **Update `config.py`**: Add a new `HANDLERNAME_CREDS` dictionary in `tests/integration/handlers/utils/config.py` to load the new environment variables.
3.  **Update `HANDLERS_TO_TEST`**: Add the name of the new handler to the `HANDLERS_TO_TEST` variable in your `.env` file.

The framework will now automatically include the new handler in the test generation process.

### Adding Custom Tests

If you need to test a specific query or behavior that isn't covered by the autodiscovery tests, you can add a custom test.

1.  **Create a JSON File**: In the `tests/integration/handlers/configs` directory, create a new JSON file named after the handler (e.g., `postgres.json`).
2.  **Define Your Queries**: In this file, you can define custom queries, including the expected columns and the minimum number of rows, as well as negative tests with the expected error messages.

    ```json
    {
      "queries": {
        "select_with_join": {
          "query": "SELECT a.col1, b.col2 FROM {db_name}.table_a a JOIN {db_name}.table_b b ON a.id = b.id",
          "expected_columns": ["col1", "col2"],
          "min_rows": 1
        }
      },
      "negative_tests": [
        {
          "query": "SELECT * FROM {db_name}.non_existent_table;",
          "expected_error": "does not exist"
        }
      ]
    }
    ```

The test generation script will automatically find this file and create the corresponding tests.

## 4. Data Ingestion & Visualization

The framework is designed to persist all test results in a PostgreSQL database, allowing for rich historical analysis.

### `tests/scripts/ingest_report.py`

This is the core script for data persistence. It reads the JSON reports generated by a `pytest` run and populates two tables in the logging database:
* **`test_results`**: Contains a high-level summary of each test run, including status, duration, and environment details.
* **`test_query_logs`**: Contains a detailed, row-by-row log of every query executed, including the query text, response, and any errors.

This script also handles the creation and migration of these tables, making the database setup automatic.

### Connecting a Visualization Tool

The choice of visualization tool is flexible. With the test data stored in a standard PostgreSQL database, a developer can connect any preferred BI or dashboarding tool (e.g., Tableau, Grafana, Power BI, or a custom web app) to the `test_results` and `test_query_logs` tables.