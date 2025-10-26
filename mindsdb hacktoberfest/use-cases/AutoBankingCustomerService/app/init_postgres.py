"""
Unified PostgreSQL table initialization.

This module provides a centralized system for initializing all PostgreSQL tables
used by the AutoBankingCustomerService application.
"""

from typing import Dict, Optional, List, Callable
from psycopg2 import sql
from .db import get_db_connection, DEFAULT_DB_CONFIG


def check_table_exists(schema: str, table: str, db_config: Dict) -> bool:
    """Check if a table exists in the specified schema."""
    try:
        conn = get_db_connection(db_config)
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
        conn.close()
        return bool(exists)
    except Exception:
        return False


def ensure_schema(schema: str, db_config: Dict, verbose: bool = False) -> bool:
    """Ensure a schema exists in the database."""
    try:
        conn = get_db_connection(db_config)
        with conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
            conn.commit()
        conn.close()
        if verbose:
            print(f"✓ Schema '{schema}' ready")
        return True
    except Exception as e:
        if verbose:
            print(f"✗ Error creating schema '{schema}': {e}")
        return False


def create_conversations_summary_table(schema: str, db_config: Dict, verbose: bool = False) -> bool:
    """Create the conversations_summary table."""
    table_name = "conversations_summary"
    full_name = f"{schema}.{table_name}"

    if check_table_exists(schema, table_name, db_config):
        if verbose:
            print(f"✓ Table '{table_name}' already exists")
        # Ensure additional columns exist
        _ensure_conversation_columns(schema, db_config, verbose)
        return True

    if verbose:
        print(f"Creating table '{table_name}'...")

    try:
        conn = get_db_connection(db_config)
        with conn.cursor() as cur:
            # Create table
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {full_name} (
                    id SERIAL PRIMARY KEY,
                    conversation_id VARCHAR(255) NOT NULL UNIQUE,
                    conversation_text TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    summary TEXT NULL,
                    resolved BOOLEAN NULL,
                    jira_issue_key TEXT NULL,
                    jira_issue_url TEXT NULL,
                    jira_issue_error TEXT NULL,
                    salesforce_case_id TEXT NULL,
                    salesforce_case_url TEXT NULL,
                    salesforce_error TEXT NULL,
                    recommendation TEXT NULL,
                    recommendation_error TEXT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create indexes
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_conversation_id
                ON {full_name}(conversation_id)
            """)
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_resolved
                ON {full_name}(resolved)
            """)
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_created_at
                ON {full_name}(created_at DESC)
            """)

            conn.commit()
        conn.close()

        if verbose:
            print(f"✓ Table '{table_name}' created successfully")

        return True
    except Exception as e:
        if verbose:
            print(f"✗ Error creating table '{table_name}': {e}")
        return False


def _ensure_conversation_columns(schema: str, db_config: Dict, verbose: bool = False) -> None:
    """Ensure all columns exist in conversations_summary table (for backward compatibility)."""
    table_name = "conversations_summary"
    full_name = f"{schema}.{table_name}"

    try:
        conn = get_db_connection(db_config)
        with conn.cursor() as cur:
            # Jira columns
            cur.execute(f"ALTER TABLE {full_name} ADD COLUMN IF NOT EXISTS jira_issue_key TEXT NULL")
            cur.execute(f"ALTER TABLE {full_name} ADD COLUMN IF NOT EXISTS jira_issue_url TEXT NULL")
            cur.execute(f"ALTER TABLE {full_name} ADD COLUMN IF NOT EXISTS jira_issue_error TEXT NULL")

            # Salesforce columns
            cur.execute(f"ALTER TABLE {full_name} ADD COLUMN IF NOT EXISTS salesforce_case_id TEXT NULL")
            cur.execute(f"ALTER TABLE {full_name} ADD COLUMN IF NOT EXISTS salesforce_case_url TEXT NULL")
            cur.execute(f"ALTER TABLE {full_name} ADD COLUMN IF NOT EXISTS salesforce_error TEXT NULL")

            # Recommendation columns
            cur.execute(f"ALTER TABLE {full_name} ADD COLUMN IF NOT EXISTS recommendation TEXT NULL")
            cur.execute(f"ALTER TABLE {full_name} ADD COLUMN IF NOT EXISTS recommendation_error TEXT NULL")

            conn.commit()
        conn.close()
    except Exception as e:
        if verbose:
            print(f"⚠ Error ensuring columns: {e}")


def create_daily_analytics_table(schema: str, db_config: Dict, verbose: bool = False) -> bool:
    """Create the daily_analytics table."""
    table_name = "daily_analytics"
    full_name = f"{schema}.{table_name}"

    if check_table_exists(schema, table_name, db_config):
        if verbose:
            print(f"✓ Table '{table_name}' already exists")
        return True

    if verbose:
        print(f"Creating table '{table_name}'...")

    try:
        conn = get_db_connection(db_config)
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {full_name} (
                    id SERIAL PRIMARY KEY,
                    analysis_date DATE NOT NULL UNIQUE,
                    total_conversations INT NOT NULL DEFAULT 0,
                    resolved_count INT NOT NULL DEFAULT 0,
                    unresolved_count INT NOT NULL DEFAULT 0,
                    resolution_rate DECIMAL(5,2),
                    top_issues JSONB,
                    key_insights TEXT,
                    recommendations TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT positive_conversations CHECK (total_conversations >= 0),
                    CONSTRAINT valid_counts CHECK (resolved_count + unresolved_count = total_conversations),
                    CONSTRAINT valid_resolution_rate CHECK (resolution_rate >= 0 AND resolution_rate <= 100)
                )
            """)

            # Create indexes
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_daily_analytics_date
                ON {full_name}(analysis_date DESC)
            """)
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_daily_top_issues
                ON {full_name} USING GIN(top_issues)
            """)

            conn.commit()
        conn.close()

        if verbose:
            print(f"✓ Table '{table_name}' created successfully")

        return True
    except Exception as e:
        if verbose:
            print(f"✗ Error creating table '{table_name}': {e}")
        return False


def create_weekly_trends_table(schema: str, db_config: Dict, verbose: bool = False) -> bool:
    """Create the weekly_trends table."""
    table_name = "weekly_trends"
    full_name = f"{schema}.{table_name}"

    if check_table_exists(schema, table_name, db_config):
        if verbose:
            print(f"✓ Table '{table_name}' already exists")
        return True

    if verbose:
        print(f"Creating table '{table_name}'...")

    try:
        conn = get_db_connection(db_config)
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {full_name} (
                    id SERIAL PRIMARY KEY,
                    week_start_date DATE NOT NULL,
                    week_end_date DATE NOT NULL,
                    total_conversations INT NOT NULL DEFAULT 0,
                    avg_resolution_rate DECIMAL(5,2),
                    trend_direction VARCHAR(20),
                    trending_issues JSONB,
                    emerging_patterns JSONB,
                    trend_summary TEXT,
                    strategic_recommendations TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT valid_week_range CHECK (week_end_date > week_start_date),
                    CONSTRAINT unique_week UNIQUE (week_start_date, week_end_date),
                    CONSTRAINT positive_weekly_conversations CHECK (total_conversations >= 0),
                    CONSTRAINT valid_weekly_resolution_rate CHECK (avg_resolution_rate >= 0 AND avg_resolution_rate <= 100)
                )
            """)

            # Create indexes
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_weekly_trends_dates
                ON {full_name}(week_start_date DESC, week_end_date DESC)
            """)
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_weekly_trending_issues
                ON {full_name} USING GIN(trending_issues)
            """)
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_weekly_patterns
                ON {full_name} USING GIN(emerging_patterns)
            """)

            conn.commit()
        conn.close()

        if verbose:
            print(f"✓ Table '{table_name}' created successfully")

        return True
    except Exception as e:
        if verbose:
            print(f"✗ Error creating table '{table_name}': {e}")
        return False


def create_update_triggers(schema: str, db_config: Dict, verbose: bool = False) -> bool:
    """Create update timestamp triggers for all tables."""
    if verbose:
        print("Creating update timestamp triggers...")

    try:
        conn = get_db_connection(db_config)
        with conn.cursor() as cur:
            # Create trigger function (if not exists)
            cur.execute(f"""
                CREATE OR REPLACE FUNCTION {schema}.update_timestamp()
                RETURNS TRIGGER AS $$
                BEGIN
                    NEW.updated_at = CURRENT_TIMESTAMP;
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql
            """)

            # Apply triggers to all tables
            tables = ["conversations_summary", "daily_analytics", "weekly_trends"]
            for table in tables:
                full_name = f"{schema}.{table}"
                trigger_name = f"update_{table}_timestamp"

                cur.execute(f"DROP TRIGGER IF EXISTS {trigger_name} ON {full_name}")
                cur.execute(f"""
                    CREATE TRIGGER {trigger_name}
                        BEFORE UPDATE ON {full_name}
                        FOR EACH ROW
                        EXECUTE FUNCTION {schema}.update_timestamp()
                """)

            conn.commit()
        conn.close()

        if verbose:
            print("✓ Update triggers created successfully")

        return True
    except Exception as e:
        if verbose:
            print(f"✗ Error creating triggers: {e}")
        return False


def init_postgres(db_config: Optional[Dict] = None, verbose: bool = False) -> bool:
    """Initialize all PostgreSQL tables.

    This is the unified entry point for PostgreSQL initialization.
    It creates all required tables in the correct order.

    Args:
        db_config: Database configuration (uses DEFAULT_DB_CONFIG if None)
        verbose: Print detailed progress messages

    Returns:
        True if all tables initialized successfully
    """
    config = db_config or DEFAULT_DB_CONFIG
    schema = config.get("schema", "demo_data")

    if verbose:
        print("=" * 70)
        print("Initializing PostgreSQL Database")
        print("=" * 70)

    success = True

    # Step 1: Ensure schema exists
    if verbose:
        print(f"\n[1/5] Ensuring schema '{schema}' exists...")
    if not ensure_schema(schema, config, verbose):
        success = False

    # Step 2: Create conversations_summary table
    if verbose:
        print(f"\n[2/5] Initializing conversations_summary table...")
    if not create_conversations_summary_table(schema, config, verbose):
        success = False

    # Step 3: Create daily_analytics table
    if verbose:
        print(f"\n[3/5] Initializing daily_analytics table...")
    if not create_daily_analytics_table(schema, config, verbose):
        success = False

    # Step 4: Create weekly_trends table
    if verbose:
        print(f"\n[4/5] Initializing weekly_trends table...")
    if not create_weekly_trends_table(schema, config, verbose):
        success = False

    # Step 5: Create update triggers
    if verbose:
        print(f"\n[5/5] Setting up update triggers...")
    if not create_update_triggers(schema, config, verbose):
        success = False

    if verbose:
        print("\n" + "=" * 70)
        if success:
            print("PostgreSQL Database Initialization Complete!")
            print("\nInitialized tables:")
            print(f"  • {schema}.conversations_summary - Customer conversation data")
            print(f"  • {schema}.daily_analytics - Daily analytics reports")
            print(f"  • {schema}.weekly_trends - Weekly trend analysis")
        else:
            print("PostgreSQL Database Initialization Completed with Warnings")
        print("=" * 70)

    return success


if __name__ == "__main__":
    # Can be run standalone for testing
    import sys
    success = init_postgres(verbose=True)
    sys.exit(0 if success else 1)
