"""Database utilities for the AutoBankingCustomerService project.

This module provides:
1. Database configuration (DEFAULT_DB_CONFIG)
2. Connection utilities (db_connection, get_db_connection)
3. Helper functions for table/schema names
4. Core business logic (insert_conversation_with_analysis)
5. PostgreSQL initialization (delegates to init_postgres module)
"""

from __future__ import annotations

import os
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


# ============================================================================
# Connection utilities
# ============================================================================

@contextmanager
def db_connection(db_config: Optional[Dict[str, str | int]] = None) -> Iterator[PGConnection]:
    """Context manager that yields a PostgreSQL connection.

    Usage:
        with db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM table")

    Args:
        db_config: Database configuration (uses DEFAULT_DB_CONFIG if None)

    Yields:
        PostgreSQL connection (automatically closed on exit)
    """
    config = db_config or DEFAULT_DB_CONFIG
    # Extract schema/table to avoid passing them to psycopg2.connect
    connection_params = {k: v for k, v in config.items() if k not in ("schema", "table")}
    conn = psycopg2.connect(**connection_params)
    try:
        yield conn
    finally:
        conn.close()


def get_db_connection(db_config: Optional[Dict[str, str | int]] = None) -> PGConnection:
    """Return a raw PostgreSQL connection (caller must close).

    Note: Prefer using db_connection() context manager instead.
    This function is kept for backward compatibility with init_postgres module.

    Args:
        db_config: Database configuration (uses DEFAULT_DB_CONFIG if None)

    Returns:
        PostgreSQL connection (caller responsible for closing)
    """
    config = db_config or DEFAULT_DB_CONFIG
    # Extract schema/table to avoid passing them to psycopg2.connect
    connection_params = {k: v for k, v in config.items() if k not in ("schema", "table")}
    return psycopg2.connect(**connection_params)


# ============================================================================
# Helper functions
# ============================================================================

def _get_schema(db_config: Dict[str, str | int]) -> str:
    """Get the schema name from config."""
    return str(db_config.get("schema", "demo_data"))


def _get_table(db_config: Dict[str, str | int]) -> str:
    """Get the table name from config."""
    return str(db_config.get("table", "conversations_summary"))


def _get_full_table_name(db_config: Dict[str, str | int]) -> str:
    """Get the fully qualified table name (schema.table)."""
    schema = _get_schema(db_config)
    table = _get_table(db_config)
    return f"{schema}.{table}"


# ============================================================================
# PostgreSQL initialization
# ============================================================================

def init_postgres(db_config: Optional[Dict[str, str | int]] = None, verbose: bool = False) -> bool:
    """Initialize all PostgreSQL tables (conversations + analytics).

    This is the unified entry point for PostgreSQL initialization.
    Delegates to the centralized init_postgres module.

    Creates:
    - conversations_summary table
    - daily_analytics table
    - weekly_trends table
    - Update triggers for all tables

    Args:
        db_config: Database configuration (uses DEFAULT_DB_CONFIG if None)
        verbose: Print detailed progress messages

    Returns:
        True if all tables initialized successfully
    """
    from .init_postgres import init_postgres as _init_postgres
    return _init_postgres(db_config=db_config, verbose=verbose)


# ============================================================================
# Core business logic
# ============================================================================

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
    """Insert a single analyzed conversation into the database.

    Args:
        conversation_id: Unique conversation identifier
        conversation_text: Full conversation transcript
        summary: AI-generated summary
        resolved: Whether the issue was resolved
        db_config: Database configuration (uses DEFAULT_DB_CONFIG if None)
        jira_issue_key: Jira ticket key (e.g., "PROJ-123")
        jira_issue_url: Jira ticket URL
        jira_issue_error: Error message if Jira creation failed
        salesforce_case_id: Salesforce case ID
        salesforce_case_url: Salesforce case URL
        salesforce_error: Error message if Salesforce creation failed
        recommendation: AI-generated recommendation for next steps
        recommendation_error: Error message if recommendation generation failed
    """
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


# ============================================================================
# CLI entry point (for standalone testing)
# ============================================================================

if __name__ == "__main__":  # pragma: no cover - CLI convenience
    import sys
    print("Initializing PostgreSQL database...")
    success = init_postgres(db_config=DEFAULT_DB_CONFIG, verbose=True)
    sys.exit(0 if success else 1)
