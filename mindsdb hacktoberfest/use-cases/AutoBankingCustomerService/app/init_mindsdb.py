"""MindsDB initialization for AutoBankingCustomerService.

This module handles:
1. PostgreSQL database connection setup
2. OpenAI ML engine creation
3. Agent creation and registration
"""

import os
import sys

import mindsdb_sdk
from dotenv import load_dotenv

from .db import DEFAULT_DB_CONFIG
from .agents import create_agents

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


def check_database_exists(server, db_name: str) -> bool:
    """Check if a database connection exists in MindsDB."""
    try:
        databases = server.list_databases()
        return db_name in [db.name for db in databases]
    except Exception as e:
        print(f"⚠ Error checking database: {e}")
        return False


def check_ml_engine_exists(server, engine_name: str) -> bool:
    """Check if an ML engine exists in MindsDB."""
    try:
        result = server.query(
            f"SELECT * FROM information_schema.ml_engines WHERE name = '{engine_name}'"
        )
        data = result.fetch()
        return len(data) > 0
    except Exception as e:
        print(f"⚠ Error checking ML engine: {e}")
        return False


def create_postgres_connection(server) -> bool:
    """Create PostgreSQL database connection in MindsDB."""
    db_name = "banking_postgres_db"

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


def create_openai_engine(server) -> bool:
    """Create OpenAI ML engine in MindsDB."""
    engine_name = "openai_engine"

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


def init_mindsdb(verbose: bool = True) -> bool:
    """Initialize MindsDB with all required components.

    Args:
        verbose: Whether to print detailed logs

    Returns:
        True if initialization was successful
    """
    if verbose:
        print("=" * 70)
        print("MindsDB Initialization")
        print("=" * 70)
        print(f"\nConnecting to MindsDB at {MINDSDB_URL}...")

    try:
        server = mindsdb_sdk.connect(MINDSDB_URL)
        if verbose:
            print("✓ Successfully connected to MindsDB")
    except Exception as e:
        print(f"✗ Failed to connect to MindsDB: {e}")
        print(f"\nPlease ensure MindsDB is running at {MINDSDB_URL}")
        return False

    # Step 1: Create PostgreSQL connection
    success = create_postgres_connection(server)
    if not success and verbose:
        print("\n⚠ PostgreSQL connection setup failed, but continuing...")

    # Step 2: Create OpenAI engine
    success = create_openai_engine(server)
    if not success and verbose:
        print("\n⚠ OpenAI engine setup failed, but continuing...")

    # Step 3: Create all agents
    agent_results = create_agents(server)

    if verbose:
        print("\n" + "=" * 70)
        print("✓ MindsDB initialization completed!")
        print("=" * 70)
        print("\nAvailable components:")
        print("  - Database: banking_postgres_db")
        print("  - ML Engine: openai_engine")
        print("  - Agents:")
        for agent_name, success in agent_results.items():
            status = "✓" if success else "✗"
            print(f"    {status} {agent_name}")

    return True


def main(verbose: bool = True) -> bool:
    """Main entry point for standalone execution."""
    success = init_mindsdb(verbose=verbose)
    return success


if __name__ == "__main__":
    success = main(verbose=True)
    sys.exit(0 if success else 1)
