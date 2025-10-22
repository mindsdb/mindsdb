"""Database utilities for the AutoBankingCustomerService project."""

from __future__ import annotations

import os
import sys
from contextlib import contextmanager
from typing import Dict, Iterator, Optional

import psycopg2
from psycopg2.extensions import connection as PGConnection
from dotenv import load_dotenv

load_dotenv()

# Default database configuration (read from environment variables)
DEFAULT_DB_CONFIG: Dict[str, str | int] = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "demo"),
    "user": os.getenv("DB_USER", "postgresql"),
    "password": os.getenv("DB_PASSWORD", "psqlpasswd"),
    "schema": os.getenv("DB_SCHEMA", "demo_data"),
    "table": os.getenv("DB_TABLE", "conversations_summary"),
}


@contextmanager
def db_connection(db_config: Optional[Dict[str, str | int]] = None) -> Iterator[PGConnection]:
    """Context manager that yields a PostgreSQL connection."""
    config = db_config or DEFAULT_DB_CONFIG
    conn = psycopg2.connect(**config)
    try:
        yield conn
    finally:
        conn.close()


def get_db_connection(db_config: Optional[Dict[str, str | int]] = None) -> PGConnection:
    """Return a raw PostgreSQL connection (caller must close)."""
    config = db_config or DEFAULT_DB_CONFIG
    # Extract schema/table to avoid passing them to psycopg2.connect
    connection_params = {k: v for k, v in config.items() if k not in ("schema", "table")}
    return psycopg2.connect(**connection_params)


def _get_full_table_name(db_config: Dict[str, str | int]) -> str:
    """Get the fully qualified table name (schema.table)."""
    schema = db_config.get("schema", "demo_data")
    table = db_config.get("table", "conversations_summary")
    return f"{schema}.{table}"


def _get_schema(db_config: Dict[str, str | int]) -> str:
    """Get the schema name."""
    return str(db_config.get("schema", "demo_data"))


def _get_table(db_config: Dict[str, str | int]) -> str:
    """Get the table name."""
    return str(db_config.get("table", "conversations_summary"))


def ensure_table_exists(db_config: Optional[Dict[str, str | int]] = None, verbose: bool = False) -> bool:
    """Ensure the demo_data.conversations_summary table is present."""
    config = db_config or DEFAULT_DB_CONFIG

    if _check_table_exists(config, verbose=verbose):
        if verbose:
            print("✓ conversations_summary table already exists")
        _ensure_jira_columns(config, verbose=verbose)
        return True

    if verbose:
        print("⚠ conversations_summary table not found, creating...")

    created = _init_summary_table(config, verbose=verbose)
    if created:
        _ensure_jira_columns(config, verbose=verbose)
    return created


def init_postgres(db_config: Optional[Dict[str, str | int]] = None, verbose: bool = False) -> bool:
    """Initialize all PostgreSQL tables (conversations + analytics).

    This is the unified entry point for PostgreSQL initialization.
    Delegates to the centralized init_postgres module.

    Args:
        db_config: Database configuration (uses DEFAULT_DB_CONFIG if None)
        verbose: Print detailed progress messages

    Returns:
        True if all tables initialized successfully
    """
    from .init_postgres import init_postgres as _init_postgres
    return _init_postgres(db_config=db_config, verbose=verbose)


def check_table_exists(db_config: Optional[Dict[str, str | int]] = None, verbose: bool = False) -> bool:
    """Public wrapper for checking if the table exists without creating it."""
    config = db_config or DEFAULT_DB_CONFIG
    return _check_table_exists(config, verbose=verbose)


def init_summary_table(db_config: Optional[Dict[str, str | int]] = None, verbose: bool = True) -> bool:
    """Public wrapper that forces table creation."""
    config = db_config or DEFAULT_DB_CONFIG
    success = _init_summary_table(config, verbose=verbose)
    if success:
        _ensure_jira_columns(config, verbose=verbose)
    return success


def insert_conversation_with_analysis(
    conversation_id: str,
    conversation_text: str,
    summary: str,
    resolved: bool,
    db_config: Optional[Dict[str, str | int]] = None,
    jira_issue_key: Optional[str] = None,
    jira_issue_url: Optional[str] = None,
    jira_issue_error: Optional[str] = None,
    salesforce_case_id: Optional[str] = None,
    salesforce_case_url: Optional[str] = None,
    salesforce_error: Optional[str] = None,
    recommendation: Optional[str] = None,
    recommendation_error: Optional[str] = None,
) -> None:
    """Insert a single analyzed conversation into the database."""
    config = db_config or DEFAULT_DB_CONFIG
    full_table_name = _get_full_table_name(config)

    with db_connection(config) as conn:
        with conn.cursor() as cur:
            insert_sql = f"""
                INSERT INTO {full_table_name}
                (
                    conversation_id,
                    conversation_text,
                    summary,
                    resolved,
                    jira_issue_key,
                    jira_issue_url,
                    jira_issue_error,
                    salesforce_case_id,
                    salesforce_case_url,
                    salesforce_error,
                    recommendation,
                    recommendation_error,
                    created_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW());
            """
            cur.execute(
                insert_sql,
                (
                    conversation_id,
                    conversation_text,
                    summary,
                    resolved,
                    jira_issue_key,
                    jira_issue_url,
                    jira_issue_error,
                    salesforce_case_id,
                    salesforce_case_url,
                    salesforce_error,
                    recommendation,
                    recommendation_error,
                ),
            )
        conn.commit()


def _ensure_jira_columns(db_config: Dict[str, str | int], verbose: bool = False) -> None:
    full_table_name = _get_full_table_name(db_config)
    try:
        with db_connection(db_config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    ALTER TABLE {full_table_name}
                    ADD COLUMN IF NOT EXISTS jira_issue_key TEXT NULL;
                    """
                )
                cur.execute(
                    f"""
                    ALTER TABLE {full_table_name}
                    ADD COLUMN IF NOT EXISTS jira_issue_url TEXT NULL;
                    """
                )
                cur.execute(
                    f"""
                    ALTER TABLE {full_table_name}
                    ADD COLUMN IF NOT EXISTS jira_issue_error TEXT NULL;
                    """
                )
                # Salesforce columns
                cur.execute(
                    f"""
                    ALTER TABLE {full_table_name}
                    ADD COLUMN IF NOT EXISTS salesforce_case_id TEXT NULL;
                    """
                )
                cur.execute(
                    f"""
                    ALTER TABLE {full_table_name}
                    ADD COLUMN IF NOT EXISTS salesforce_case_url TEXT NULL;
                    """
                )
                cur.execute(
                    f"""
                    ALTER TABLE {full_table_name}
                    ADD COLUMN IF NOT EXISTS salesforce_error TEXT NULL;
                    """
                )
                # Recommendation columns
                cur.execute(
                    f"""
                    ALTER TABLE {full_table_name}
                    ADD COLUMN IF NOT EXISTS recommendation TEXT NULL;
                    """
                )
                cur.execute(
                    f"""
                    ALTER TABLE {full_table_name}
                    ADD COLUMN IF NOT EXISTS recommendation_error TEXT NULL;
                    """
                )
            conn.commit()
        if verbose:
            table = _get_table(db_config)
            print(f"✓ Jira, Salesforce, and Recommendation columns verified on {table}")
    except Exception as exc:  # pragma: no cover - defensive logging
        if verbose:
            print(f"✗ Unable to ensure Jira columns: {exc}")


def _check_table_exists(db_config: Dict[str, str | int], verbose: bool = False) -> bool:
    schema = _get_schema(db_config)
    table = _get_table(db_config)
    try:
        with db_connection(db_config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = %s
                        AND table_name = %s
                    );
                    """,
                    (schema, table)
                )
                exists = cur.fetchone()[0]
    except Exception as exc:  # pragma: no cover - defensive logging
        if verbose:
            print(f"✗ Error checking table: {exc}")
        return False

    if verbose:
        print(f"✓ Table check: {'EXISTS' if exists else 'NOT FOUND'}")
    return bool(exists)


def _init_summary_table(db_config: Dict[str, str | int], verbose: bool = True) -> bool:
    schema = _get_schema(db_config)
    table = _get_table(db_config)
    full_table_name = _get_full_table_name(db_config)

    if verbose:
        print("=" * 70)
        print(f"Initializing {table} table...")
        print("=" * 70)
        print(f"\nConnecting to database at {db_config['host']}:{db_config['port']}...")

    try:
        with db_connection(db_config) as conn:
            with conn.cursor() as cur:
                if verbose:
                    print("✓ Successfully connected to database")
                    print(f"\nEnsuring schema '{schema}' exists...")
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
                conn.commit()

                if verbose:
                    print("✓ Schema ready")
                    print(f"\nCreating '{table}' table...")
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {full_table_name} (
                        id SERIAL PRIMARY KEY,
                        conversation_id VARCHAR(255) NOT NULL UNIQUE,
                        conversation_text TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        summary TEXT NULL,
                        resolved BOOLEAN NULL,
                        jira_issue_key TEXT NULL,
                        jira_issue_url TEXT NULL,
                        jira_issue_error TEXT NULL,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                    """
                )
                conn.commit()

                if verbose:
                    print(f"✓ Table '{table}' created successfully")
                    print("\nCreating index on conversation_id...")
                cur.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_conversation_id
                    ON {full_table_name}(conversation_id);
                    """
                )
                conn.commit()

                if verbose:
                    print("✓ Index created successfully")
                    print("Creating index on resolved status...")
                cur.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_resolved
                    ON {full_table_name}(resolved);
                    """
                )
                conn.commit()

                if verbose:
                    print("✓ Index on resolved created successfully")
                    print("\nCreating trigger function for updated_at...")
                cur.execute(
                    """
                    CREATE OR REPLACE FUNCTION update_updated_at_column()
                    RETURNS TRIGGER AS $$
                    BEGIN
                        NEW.updated_at = CURRENT_TIMESTAMP;
                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql;
                    """
                )
                conn.commit()

                if verbose:
                    print("✓ Trigger function created successfully")
                    print("Creating trigger for updated_at...")
                # Generate a unique trigger name based on schema and table
                trigger_name = f"update_{schema}_{table}_updated_at"
                cur.execute(
                    f"""
                    DROP TRIGGER IF EXISTS {trigger_name}
                    ON {full_table_name};

                    CREATE TRIGGER {trigger_name}
                    BEFORE UPDATE ON {full_table_name}
                    FOR EACH ROW
                    EXECUTE FUNCTION update_updated_at_column();
                    """
                )
                conn.commit()

                if verbose:
                    print("✓ Trigger created successfully")
                    print("\nVerifying table structure...")
                    cur.execute(
                        """
                        SELECT column_name, data_type, is_nullable, column_default
                        FROM information_schema.columns
                        WHERE table_schema = %s
                        AND table_name = %s
                        ORDER BY ordinal_position;
                        """,
                        (schema, table)
                    )
                    columns = cur.fetchall()
                    print("\n  Table Structure:")
                    print("  " + "-" * 66)
                    print(f"  {'Column Name':<25} {'Type':<20} {'Nullable':<10} {'Default'}")
                    print("  " + "-" * 66)
                    for col_name, data_type, nullable, default in columns:
                        default_value = default if default else "None"
                        print(
                            f"  {col_name:<25} {data_type:<20} {nullable:<10} {default_value}"
                        )
                    print("  " + "-" * 66)

                cur.execute(f"SELECT COUNT(*) FROM {full_table_name};")
                count = cur.fetchone()[0]
                if verbose:
                    print(f"\n✓ Current records in table: {count}")
                    print("\n" + "=" * 70)
                    print(f"✓ {table} table initialization completed!")
                    print("=" * 70)
                    print("\nTable is ready to receive data from your backend!")
                    print("MindsDB agents can now read from and write to this table.")

                return True
    except Exception as exc:  # pragma: no cover - defensive logging
        if verbose:
            print(f"\n✗ Error during initialization: {exc}")
        return False

    return True


if __name__ == "__main__":  # pragma: no cover - CLI convenience
    success = init_summary_table(db_config=DEFAULT_DB_CONFIG, verbose=True)
    sys.exit(0 if success else 1)
