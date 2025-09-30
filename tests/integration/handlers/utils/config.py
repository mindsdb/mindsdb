import os
import logging
from dotenv import load_dotenv
from typing import Dict, Optional, Any

# When tests are run from the project root, python-dotenv finds the .env file automatically.
if load_dotenv(override=True):
    logging.info("DSI: Successfully loaded and overrode env variables from .env file.")
else:
    logging.warning("DSI: Could not find .env file. Using system variables or default credentials.")

# --- MindsDB Connection Details ---
MINDSDB_PROTOCOL: str = os.getenv("MINDSDB_PROTOCOL", "http")
MINDSDB_HOST: str = os.getenv("MINDSDB_HOST", "127.0.0.1")
MINDSDB_PORT: str = os.getenv("MINDSDB_PORT", "47334")
MINDSDB_USER: Optional[str] = os.getenv("MINDSDB_USER")
MINDSDB_PASSWORD: Optional[str] = os.getenv("MINDSDB_PASSWORD")

# --- Test Execution Configuration ---
# Default to handlers that have public credentials for a better out-of-the-box experience.
HANDLERS_TO_TEST: str = os.getenv("HANDLERS_TO_TEST", "postgres,mariadb,mysql,mssql")

# --- Data Source Credentials (Convention: HANDLERNAME_CREDS) ---
# Handlers with public, default credentials
POSTGRES_CREDS: Dict[str, Any] = {
    "host": os.getenv("PG_SOURCE_HOST", "samples.mindsdb.com"),
    "port": int(os.getenv("PG_SOURCE_PORT", 5432)),
    "database": os.getenv("PG_SOURCE_DATABASE", "demo"),
    "user": os.getenv("PG_SOURCE_USER", "demo_user"),
    "password": os.getenv("PG_SOURCE_PASSWORD", "demo_password"),
    "schema": os.getenv("PG_SOURCE_SCHEMA", "sample_data"),
}

MARIADB_CREDS: Dict[str, Any] = {
    "host": os.getenv("MARIADB_HOST", "samples.mindsdb.com"),
    "port": int(os.getenv("MARIADB_PORT", 3307)),
    "database": os.getenv("MARIADB_DATABASE", "test_data"),
    "user": os.getenv("MARIADB_USER", "demo_user"),
    "password": os.getenv("MARIADB_PASSWORD", "demo_password"),
}

MYSQL_CREDS: Dict[str, Any] = {
    "host": os.getenv("MYSQL_HOST", "samples.mindsdb.com"),
    "port": int(os.getenv("MYSQL_PORT", 3306)),
    "database": os.getenv("MYSQL_DATABASE", "public"),
    "user": os.getenv("MYSQL_USER", "user"),
    "password": os.getenv("MYSQL_PASSWORD", "MindsDBUser123!"),
}

MSSQL_CREDS: Dict[str, Any] = {
    "host": os.getenv("SQLSERVER_HOST", "samples.mindsdb.com"),
    "port": int(os.getenv("SQLSERVER_PORT", 1433)),
    "database": os.getenv("SQLSERVER_DATABASE", "demo"),
    "user": os.getenv("SQLSERVER_USER", "demo_user"),
    "password": os.getenv("SQLSERVER_SA_PASSWORD", "D3mo_Passw0rd"),
}

# --- Handlers that require .env configuration ---
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
