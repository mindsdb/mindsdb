"""
Initialize conversations_summary table for backend operations
This table stores conversation summaries and classification results

Note: MindsDB triggers should be created in MindsDB, not here.
See mindsdb_setup.sql for trigger configuration.

Can be used as:
1. Standalone script: python init_summary_table.py
2. Imported module: from init_summary_table import ensure_table_exists
"""

import psycopg2
import sys

# Database connection parameters
DEFAULT_DB_CONFIG = {
    "user": "postgresql",
    "password": "psqlpasswd",
    "host": "localhost",
    "port": "5432",
    "database": "demo"
}


def check_table_exists(db_config=None, verbose=False):
    """
    Check if conversations_summary table exists.

    Args:
        db_config: Database configuration dict (uses DEFAULT_DB_CONFIG if None)
        verbose: Print debug messages

    Returns:
        bool: True if table exists, False otherwise
    """
    config = db_config or DEFAULT_DB_CONFIG

    try:
        conn = psycopg2.connect(**config)
        cur = conn.cursor()

        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'demo_data'
                AND table_name = 'conversations_summary'
            );
        """)

        exists = cur.fetchone()[0]
        cur.close()
        conn.close()

        if verbose:
            print(f"✓ Table check: {'EXISTS' if exists else 'NOT FOUND'}")

        return exists

    except Exception as e:
        if verbose:
            print(f"✗ Error checking table: {e}")
        return False


def init_summary_table(db_config=None, verbose=True):
    """
    Creates the conversations_summary table for storing conversation summaries
    and classification results from MindsDB agents.

    Args:
        db_config: Database configuration dict (uses DEFAULT_DB_CONFIG if None)
        verbose: Print progress messages

    Returns:
        bool: True if successful, False otherwise
    """
    config = db_config or DEFAULT_DB_CONFIG

    if verbose:
        print("="*70)
        print("Initializing conversations_summary table...")
        print("="*70)

    # Connect to PostgreSQL
    if verbose:
        print(f"\nConnecting to database at {config['host']}:{config['port']}...")

    try:
        conn = psycopg2.connect(**config)
        cur = conn.cursor()
        if verbose:
            print("✓ Successfully connected to database")
    except Exception as e:
        if verbose:
            print(f"✗ Failed to connect to database: {e}")
        return False

    try:
        # Create schema if not exists
        if verbose:
            print("\nEnsuring schema 'demo_data' exists...")
        cur.execute("CREATE SCHEMA IF NOT EXISTS demo_data;")
        conn.commit()
        if verbose:
            print("✓ Schema ready")

        # Create conversations_summary table
        if verbose:
            print("\nCreating 'conversations_summary' table...")
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS demo_data.conversations_summary (
            id SERIAL PRIMARY KEY,
            conversation_id VARCHAR(255) NOT NULL UNIQUE,
            conversation_text TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            summary TEXT NULL,
            resolved BOOLEAN NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cur.execute(create_table_sql)
        conn.commit()
        if verbose:
            print("✓ Table 'conversations_summary' created successfully")

        # Create index on conversation_id for faster lookups
        if verbose:
            print("\nCreating index on conversation_id...")
        create_index_sql = """
        CREATE INDEX IF NOT EXISTS idx_conversation_id
        ON demo_data.conversations_summary(conversation_id);
        """
        cur.execute(create_index_sql)
        conn.commit()
        if verbose:
            print("✓ Index created successfully")

        # Create index on resolved for filtering
        if verbose:
            print("Creating index on resolved status...")
        create_resolved_index_sql = """
        CREATE INDEX IF NOT EXISTS idx_resolved
        ON demo_data.conversations_summary(resolved);
        """
        cur.execute(create_resolved_index_sql)
        conn.commit()
        if verbose:
            print("✓ Index on resolved created successfully")

        # Create updated_at trigger function
        if verbose:
            print("\nCreating trigger function for updated_at...")
        create_trigger_function_sql = """
        CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
        cur.execute(create_trigger_function_sql)
        conn.commit()
        if verbose:
            print("✓ Trigger function created successfully")

        # Create trigger
        if verbose:
            print("Creating trigger for updated_at...")
        create_trigger_sql = """
        DROP TRIGGER IF EXISTS update_conversations_summary_updated_at
        ON demo_data.conversations_summary;

        CREATE TRIGGER update_conversations_summary_updated_at
        BEFORE UPDATE ON demo_data.conversations_summary
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
        """
        cur.execute(create_trigger_sql)
        conn.commit()
        if verbose:
            print("✓ Trigger created successfully")

        # Verify table structure
        if verbose:
            print("\nVerifying table structure...")
            cur.execute("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = 'demo_data'
                AND table_name = 'conversations_summary'
                ORDER BY ordinal_position;
            """)

            columns = cur.fetchall()
            print("\n  Table Structure:")
            print("  " + "-"*66)
            print(f"  {'Column Name':<25} {'Type':<20} {'Nullable':<10} {'Default'}")
            print("  " + "-"*66)
            for col in columns:
                col_name = col[0]
                data_type = col[1]
                nullable = col[2]
                default = col[3] if col[3] else "None"
                print(f"  {col_name:<25} {data_type:<20} {nullable:<10} {default}")
            print("  " + "-"*66)

        # Check if table is empty
        cur.execute("SELECT COUNT(*) FROM demo_data.conversations_summary;")
        count = cur.fetchone()[0]
        if verbose:
            print(f"\n✓ Current records in table: {count}")

        if verbose:
            print("\n" + "="*70)
            print("✓ conversations_summary table initialization completed!")
            print("="*70)
            print("\nTable is ready to receive data from your backend!")
            print("MindsDB agents can now read from and write to this table.")

        return True

    except Exception as e:
        if verbose:
            print(f"\n✗ Error during initialization: {e}")
        conn.rollback()
        return False
    finally:
        cur.close()
        conn.close()
        if verbose:
            print("\nDatabase connection closed.")


def ensure_table_exists(db_config=None, verbose=False):
    """
    Check if conversations_summary table exists, and create it if it doesn't.
    This is the recommended function for use in server.py startup.

    Args:
        db_config: Database configuration dict (uses DEFAULT_DB_CONFIG if None)
        verbose: Print progress messages

    Returns:
        bool: True if table exists or was created successfully
    """
    if check_table_exists(db_config, verbose=False):
        if verbose:
            print("✓ conversations_summary table already exists")
        return True
    else:
        if verbose:
            print("⚠ conversations_summary table not found, creating...")
        return init_summary_table(db_config, verbose=verbose)


if __name__ == "__main__":
    # When run as standalone script, always use verbose mode
    success = init_summary_table(verbose=True)
    sys.exit(0 if success else 1)
