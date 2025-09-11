# MindsDB Data Source Integration (DSI) Testing Framework

This is a configuration-driven, automated testing framework for MindsDB data source integrations using pytest.

## Core Features

* **Dynamic Test Generation**: Automatically discovers tables for any connected data source and generates a standard set of health-check tests using pytest's parametrization.
* **Universal Handler Support**: Uses the MindsDB-native `SHOW TABLES` command to ensure autodiscovery works for both SQL databases and API-based handlers (like GitHub).
* **Configuration-Driven**: Easily add new data sources by providing credentials in a .env file without changing any test code.
* **Extensible Custom Tests**: Add complex, custom queries for any data source via simple JSON configuration files.
* **Comprehensive Logging**: utomatically generates a detailed JSON log of every query executed, including duration, response, and errors.

## Setup

1.  **Create and activate a virtual environment:**
    ```bash
    uv venv
    source .venv/bin/activate  # On Windows, use `.venv\Scripts\activate`
    ```

2.  **Install dependencies:**
    ```bash
    uv pip install -r requirements/requirements-test.txt
    ```

3.  **Configure Environment:**
    * Copy the `.env.example` to a new file named `.env`.
    * Fill in the .env file with your MindsDB server details and the credentials for any data sources you wish to test.
    * **(Optional)**: Fill in the PG_LOG_* variables if you wish to use the script to ingest results into a PostgreSQL database.

## Running Tests

To run the full test suite, execute the following command from the project's root directory:

```bash
# using make
make datasource_integration_tests
```

## Architectural Overview
The framework operates on a dynamic test collection model powered by pytest.parametrize. This is a clean and modern approach that avoids generating temporary test files.

### High-Level Workflow:
1. Configuration: A developer provides credentials and lists the target handlers in the .env file.

2. Test Collection: When pytest starts, the generate_test_cases_for_parametrization() function in test_data_sources.py runs. It connects to MindsDB, discovers tables for the configured handlers, and builds a list of test case definitions in memory.

3. Test Execution: pytest uses this in-memory list to dynamically generate and run a test for each definition.

4. Automatic Logging: A patched version of the MindsDB SDK automatically logs every query's duration, response, and errors to reports/all_handlers_query_log.json.

## Core Components
- tests/integration/handlers/conftest.py: The main entry point for the DSI framework. It contains all the core fixtures and hooks, including mindsdb_server (which connects to the SDK and patches it for automatic logging) and session_databases (which creates and tears down databases).

- tests/integration/handlers/test_data_sources.py: The engine of the framework. It contains the logic for discovering handlers, generating test case definitions, and the main test_handler_integrations function that executes the tests.

- tests/integration/handlers/utils/: Contains helpers for configuration (config.py) and building database connection parameters (helpers.py).

## Extending the Framework

### Adding a New Data Handler
This is a configuration-only change.

1. Add Credentials to .env: Add the necessary environment variables for the new handler (e.g., CLICKHOUSE_HOST).

2. Update config.py: In tests/integration/handlers/utils/config.py, add a new HANDLERNAME_CREDS dictionary to load these environment variables.

3. Update HANDLERS_TO_TEST: Add the name of the new handler to the HANDLERS_TO_TEST variable in your .env file.

The framework will now automatically include the new handler in its test discovery process.

### Adding Custom Tests
To test specific queries, you can add a custom test configuration.

1. Create a JSON File: In tests/integration/handlers/configs/, create a new JSON file named after the handler (e.g., postgres.json).

2. Define Your Queries: Define custom queries, expected columns, and negative tests.


```
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
  ```

## Reporting and Data Ingestion
The framework automatically creates a detailed query log at reports/all_handlers_query_log.json.

Additionally, you can ingest these results into a PostgreSQL database for analysis using the provided script:

```
python tests/scripts/ingest_report.py
```

### Our Priority List for New Data Sources
Here is the prioritized list of the next data sources we plan to integrate into the framework. The checkmarks indicate the data sources that are already tested.

- Postgres: ✅

- GitHub: ✅

-Databricks: ✅

- Parquet in S3 ✅

- Jira ✅ ( there is currently a bug in the jira handler)

- SQL Server ✅

- MariaDB ✅

- MySQL ✅

- Redshift ❌