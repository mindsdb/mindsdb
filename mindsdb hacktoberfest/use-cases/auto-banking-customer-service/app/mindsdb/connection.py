"""MindsDB connection and database setup utilities.

This module handles:
1. MindsDB server connection management
2. PostgreSQL database connection setup in MindsDB
3. OpenAI ML engine creation
"""

import os
from typing import Any, Optional

import mindsdb_sdk
from dotenv import load_dotenv

from ..db import DEFAULT_DB_CONFIG

load_dotenv()

# MindsDB configuration
MINDSDB_URL = os.getenv("MINDSDB_URL", "http://127.0.0.1:47334")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

# PostgreSQL configuration from centralized DEFAULT_DB_CONFIG
PG_HOST = DEFAULT_DB_CONFIG.get("host", "localhost")
PG_PORT = DEFAULT_DB_CONFIG.get("port", 5432)
PG_DATABASE = DEFAULT_DB_CONFIG.get("database", "demo")
PG_USER = DEFAULT_DB_CONFIG.get("user", "postgresql")
PG_PASSWORD = DEFAULT_DB_CONFIG.get("password", "psqlpasswd")
PG_SCHEMA = DEFAULT_DB_CONFIG.get("schema", "demo_data")

# Global MindsDB server instance
_mindsdb_server: Optional[Any] = None


def get_mindsdb_server() -> Optional[Any]:
    """Get the current MindsDB server connection.

    Returns:
        MindsDB server instance or None if not connected
    """
    return _mindsdb_server


def set_mindsdb_server(server: Any) -> None:
    """Set the MindsDB server instance.

    Args:
        server: MindsDB server connection
    """
    global _mindsdb_server
    _mindsdb_server = server


def connect_to_mindsdb(url: Optional[str] = None, verbose: bool = True) -> Optional[Any]:
    """Connect to MindsDB server.

    Args:
        url: MindsDB URL (uses MINDSDB_URL env var if not provided)
        verbose: Whether to print connection messages

    Returns:
        MindsDB server instance or None if connection failed
    """
    target_url = url or MINDSDB_URL

    if verbose:
        print(f"Connecting to MindsDB at {target_url}...")

    try:
        server = mindsdb_sdk.connect(target_url)
        set_mindsdb_server(server)
        if verbose:
            print("✓ Successfully connected to MindsDB")
        return server
    except Exception as e:
        print(f"✗ Failed to connect to MindsDB: {e}")
        print(f"\nPlease ensure MindsDB is running at {target_url}")
        return None


def check_database_exists(server, db_name: str) -> bool:
    """Check if a database connection exists in MindsDB.

    Args:
        server: MindsDB server connection
        db_name: Name of the database to check

    Returns:
        True if database exists, False otherwise
    """
    try:
        databases = server.list_databases()
        return db_name in [db.name for db in databases]
    except Exception as e:
        print(f"⚠ Error checking database: {e}")
        return False


def check_ml_engine_exists(server, engine_name: str) -> bool:
    """Check if an ML engine exists in MindsDB.

    Args:
        server: MindsDB server connection
        engine_name: Name of the ML engine to check

    Returns:
        True if ML engine exists, False otherwise
    """
    try:
        result = server.query(
            f"SELECT * FROM information_schema.ml_engines WHERE name = '{engine_name}'"
        )
        data = result.fetch()
        return len(data) > 0
    except Exception as e:
        print(f"⚠ Error checking ML engine: {e}")
        return False


def create_postgres_connection(server, db_name: str = "banking_postgres_db") -> bool:
    """Create PostgreSQL database connection in MindsDB.

    Args:
        server: MindsDB server connection
        db_name: Name for the database connection (default: banking_postgres_db)

    Returns:
        True if connection was created successfully or already exists
    """
    if check_database_exists(server, db_name):
        print(f"✓ Database '{db_name}' already exists, skipping creation")
        return True

    print(f"\nCreating PostgreSQL database connection '{db_name}'...")

    sql = f"""
    CREATE DATABASE {db_name}
    WITH ENGINE = 'postgres',
    PARAMETERS = {{
        "host": "{PG_HOST}",
        "port": {PG_PORT},
        "database": "{PG_DATABASE}",
        "user": "{PG_USER}",
        "password": "{PG_PASSWORD}",
        "schema": "{PG_SCHEMA}"
    }};
    """

    try:
        server.query(sql)
        print(f"✓ Successfully created database connection '{db_name}'")
        return True
    except Exception as e:
        print(f"✗ Failed to create database connection: {e}")
        return False


def create_openai_engine(server, engine_name: str = "openai_engine") -> bool:
    """Create OpenAI ML engine in MindsDB.

    Args:
        server: MindsDB server connection
        engine_name: Name for the ML engine (default: openai_engine)

    Returns:
        True if engine was created successfully or already exists
    """
    if not OPENAI_API_KEY:
        print(f"\n⚠ OPENAI_API_KEY not found in environment variables")
        print(f"⚠ Skipping OpenAI engine creation")
        return False

    if check_ml_engine_exists(server, engine_name):
        print(f"✓ ML engine '{engine_name}' already exists, skipping creation")
        return True

    print(f"\nCreating OpenAI ML engine '{engine_name}'...")

    sql = f"""
    CREATE ML_ENGINE {engine_name}
    FROM openai
    USING
        openai_api_key = '{OPENAI_API_KEY}';
    """

    try:
        server.query(sql)
        print(f"✓ Successfully created ML engine '{engine_name}'")

        # Verify creation
        result = server.query(
            f"SELECT * FROM information_schema.ml_engines WHERE name = '{engine_name}'"
        )
        data = result.fetch()
        if len(data) > 0:
            print(f"✓ Verified ML engine '{engine_name}' is registered")

        return True
    except Exception as e:
        print(f"✗ Failed to create ML engine: {e}")
        return False
