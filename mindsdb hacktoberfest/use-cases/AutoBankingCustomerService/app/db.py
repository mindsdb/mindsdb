"""Database utilities for the AutoBankingCustomerService project."""

from __future__ import annotations

import sys
from contextlib import contextmanager
from typing import Dict, Iterator, Optional

import psycopg2
from psycopg2.extensions import connection as PGConnection

DEFAULT_DB_CONFIG: Dict[str, str | int] = {
    "host": "localhost",
    "port": 5432,
    "database": "demo",
    "user": "postgresql",
    "password": "psqlpasswd",
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
    return psycopg2.connect(**config)


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
) -> None:
    """Insert a single analyzed conversation into the database."""
    config = db_config or DEFAULT_DB_CONFIG

    with db_connection(config) as conn:
        with conn.cursor() as cur:
            insert_sql = """
                INSERT INTO demo_data.conversations_summary
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
                    created_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW());
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
                ),
            )
        conn.commit()


def _ensure_jira_columns(db_config: Dict[str, str | int], verbose: bool = False) -> None:
    try:
        with db_connection(db_config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    ALTER TABLE demo_data.conversations_summary
                    ADD COLUMN IF NOT EXISTS jira_issue_key TEXT NULL;
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE demo_data.conversations_summary
                    ADD COLUMN IF NOT EXISTS jira_issue_url TEXT NULL;
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE demo_data.conversations_summary
                    ADD COLUMN IF NOT EXISTS jira_issue_error TEXT NULL;
                    """
                )
            conn.commit()
        if verbose:
            print("✓ Jira columns verified on conversations_summary")
    except Exception as exc:  # pragma: no cover - defensive logging
        if verbose:
            print(f"✗ Unable to ensure Jira columns: {exc}")


def _check_table_exists(db_config: Dict[str, str | int], verbose: bool = False) -> bool:
    try:
        with db_connection(db_config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'demo_data'
                        AND table_name = 'conversations_summary'
                    );
                    """
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
    if verbose:
        print("=" * 70)
        print("Initializing conversations_summary table...")
        print("=" * 70)
        print(f"\nConnecting to database at {db_config['host']}:{db_config['port']}...")

    try:
        with db_connection(db_config) as conn:
            with conn.cursor() as cur:
                if verbose:
                    print("✓ Successfully connected to database")
                    print("\nEnsuring schema 'demo_data' exists...")
                cur.execute("CREATE SCHEMA IF NOT EXISTS demo_data;")
                conn.commit()

                if verbose:
                    print("✓ Schema ready")
                    print("\nCreating 'conversations_summary' table...")
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS demo_data.conversations_summary (
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
                    print("✓ Table 'conversations_summary' created successfully")
                    print("\nCreating index on conversation_id...")
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_conversation_id
                    ON demo_data.conversations_summary(conversation_id);
                    """
                )
                conn.commit()

                if verbose:
                    print("✓ Index created successfully")
                    print("Creating index on resolved status...")
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_resolved
                    ON demo_data.conversations_summary(resolved);
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
                cur.execute(
                    """
                    DROP TRIGGER IF EXISTS update_conversations_summary_updated_at
                    ON demo_data.conversations_summary;

                    CREATE TRIGGER update_conversations_summary_updated_at
                    BEFORE UPDATE ON demo_data.conversations_summary
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
                        WHERE table_schema = 'demo_data'
                        AND table_name = 'conversations_summary'
                        ORDER BY ordinal_position;
                        """
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

                cur.execute("SELECT COUNT(*) FROM demo_data.conversations_summary;")
                count = cur.fetchone()[0]
                if verbose:
                    print(f"\n✓ Current records in table: {count}")
                    print("\n" + "=" * 70)
                    print("✓ conversations_summary table initialization completed!")
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
