import os
import logging
from dotenv import load_dotenv
from typing import Dict, Optional, Any

# When tests are run from the project root, python-dotenv finds the .env file automatically.
if load_dotenv(override=True):
    logging.info("DSI: Successfully loaded and overrode env variables from .env file.")
else:
    logging.warning("DSI: Could not find .env file. Using system variables or falling back to default credentials.")


# --- Default Public Credentials ---
# Used as a fallback if corresponding environment variables are not set.
DEFAULT_CREDS = {
    'postgres': {
        "host": "samples.mindsdb.com",
        "port": 5432,
        "database": "demo",
        "user": "demo_user",
        "password": "demo_password",
        "schema": "sample_data",
    },
    'mariadb': {
        "host": "samples.mindsdb.com",
        "port": 3307,
        "database": "test_data",
        "user": "demo_user",
        "password": "demo_password",
    },
    'mysql': {
        "host": "samples.mindsdb.com",
        "port": 3306,
        "database": "public",
        "user": "user",
        "password": "MindsDBUser123!",
    },
    'mssql': {
        "host": "samples.mindsdb.com",
        "port": 1433,
        "database": "demo",
        "user": "demo_user",
        "password": "D3mo_Passw0rd",
    }
}


def get_creds(handler_name: str, env_key: str, creds_key: str) -> Optional[Any]:
    """
    Gets a credential value, prioritizing environment variables over hardcoded defaults.
    """
    # 1. Check environment variable first.
    value = os.getenv(env_key)
    if value is not None:
        return value
    
    # 2. If no env var, try to get from the default dictionary.
    if handler_name in DEFAULT_CREDS:
        return DEFAULT_CREDS[handler_name].get(creds_key)
        
    return None

# --- MindsDB Connection Details ---
MINDSDB_PROTOCOL: str = os.getenv("MINDSDB_PROTOCOL", "http")
MINDSDB_HOST: str = os.getenv("MINDSDB_HOST", "locahost")
MINDSDB_PORT: str = os.getenv("MINDSDB_PORT", "47334")
MINDSDB_USER: Optional[str] = os.getenv("MINDSDB_USER")
MINDSDB_PASSWORD: Optional[str] = os.getenv("MINDSDB_PASSWORD")

# --- Test Execution Configuration ---
# Default to handlers that have public credentials for a better out-of-the-box experience.
HANDLERS_TO_TEST: str = os.getenv("HANDLERS_TO_TEST", "postgres,mariadb,mysql,mssql")

# --- Data Source Credentials (Convention: HANDLERNAME_CREDS) ---
POSTGRES_CREDS: Dict[str, Any] = {
    "host": get_creds('postgres', "PG_SOURCE_HOST", "host"),
    "port": int(get_creds('postgres', "PG_SOURCE_PORT", "port")),
    "database": get_creds('postgres', "PG_SOURCE_DATABASE", "database"),
    "user": get_creds('postgres', "PG_SOURCE_USER", "user"),
    "password": get_creds('postgres', "PG_SOURCE_PASSWORD", "password"),
    "schema": get_creds('postgres', "PG_SOURCE_SCHEMA", "schema"),
}

MARIADB_CREDS: Dict[str, Any] = {
    "host": get_creds('mariadb', "MARIADB_HOST", "host"),
    "port": int(get_creds('mariadb', "MARIADB_PORT", "port")),
    "database": get_creds('mariadb', "MARIADB_DATABASE", "database"),
    "user": get_creds('mariadb', "MARIADB_USER", "user"),
    "password": get_creds('mariadb', "MARIADB_PASSWORD", "password"),
}

MYSQL_CREDS: Dict[str, Any] = {
    "host": get_creds('mysql', "MYSQL_HOST", "host"),
    "port": int(get_creds('mysql', "MYSQL_PORT", "port")),
    "database": get_creds('mysql', "MYSQL_DATABASE", "database"),
    "user": get_creds('mysql', "MYSQL_USER", "user"),
    "password": get_creds('mysql', "MYSQL_PASSWORD", "password"),
}

MSSQL_CREDS: Dict[str, Any] = {
    "host": get_creds('mssql', "SQLSERVER_HOST", "host"),
    "port": int(get_creds('mssql', "SQLSERVER_PORT", "port")),
    "database": get_creds('mssql', "SQLSERVER_DATABASE", "database"),
    "user": get_creds('mssql', "SQLSERVER_USER", "user"),
    "password": get_creds('mssql', "SQLSERVER_SA_PASSWORD", "password"),
}

# --- Handlers without default credentials ---
DATABRICKS_CREDS: Dict[str, Any] = {
    "server_hostname": os.getenv("DATABRICKS_HOST"),
    "http_path": f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
    "access_token": os.getenv("DATABRICKS_TOKEN"),
    "catalog": os.getenv("DATABRICKS_CATALOG", "workspace"),
    "schema": os.getenv("DATABRICKS_SCHEMA"),
}

SNOWFLAKE_CREDS: Dict[str, Any] = {
    "host": os.getenv("SNOWFLAKE_HOST"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
}

GITHUB_CREDS: Dict[str, Any] = {"token": os.getenv("GITHUB_TOKEN"), "repository": os.getenv("GITHUB_REPOSITORY")}

BIGQUERY_CREDS: Dict[str, Any] = {
    "project_id": os.getenv("BIGQUERY_PROJECT_ID"),
    "dataset": os.getenv("BIGQUERY_DATASET"),
    "service_account_json": os.getenv("BIGQUERY_SERVICE_ACCOUNT_JSON"),
}

S3_CREDS: Dict[str, Any] = {
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "bucket": os.getenv("S3_BUCKET_NAME"),
}

# --- PostgreSQL Logging Database Credentials ---
PG_LOG_HOST: Optional[str] = os.getenv("PG_LOG_HOST")
PG_LOG_PORT: Optional[str] = os.getenv("PG_LOG_PORT", "5432")
PG_LOG_DATABASE: Optional[str] = os.getenv("PG_LOG_DATABASE")
PG_LOG_USER: Optional[str] = os.getenv("PG_LOG_USER")
PG_LOG_PASSWORD: Optional[str] = os.getenv("PG_LOG_PASSWORD")


logging.info(f"DSI: Configuration loaded for MindsDB host: {MINDSDB_HOST}")
logging.info(f"DSI: E2E tests will run for: {HANDLERS_TO_TEST}")