"""
Banking Data Import Script
Imports banking CSV data into PostgreSQL database
Preprocesses conversations by aggregating and sorting by timestamp

Usage:
    python import_banking_data.py                    # Uses sample file (10k records)
    python import_banking_data.py --full             # Uses full file (5.5M records)
    python import_banking_data.py --file custom.csv  # Uses custom file
"""

import psycopg2
import csv
import os
import sys
from pathlib import Path

# Database connection parameters
PARAMETERS = {
    "user": "postgresql",
    "password": "psqlpasswd",
    "host": "localhost",
    "port": "5432",
    "database": "demo"
}

# Default to sample file for faster testing
DEFAULT_CSV_FILE = "banking_sample_10k.csv"
FULL_CSV_FILE = "banking_300k.csv"


def preprocess_conversations(conn, cur):
    """
    Aggregates conversation rows by conversation_id and creates a preprocessed table
    with complete conversation text sorted by timestamp.
    """
    print("\n" + "="*70)
    print("Starting conversation preprocessing...")
    print("="*70)

    # Create preprocessed table
    print("\nCreating 'banking_conversations_preprocessed' table...")
    create_preprocessed_table_sql = """
    CREATE TABLE IF NOT EXISTS demo_data.banking_conversations_preprocessed (
        conversation_id VARCHAR(255) PRIMARY KEY,
        conversation_text TEXT,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        message_count INTEGER,
        speaker_types TEXT[]
    );
    """
    cur.execute(create_preprocessed_table_sql)
    conn.commit()
    print("✓ Preprocessed table created successfully")

    # Get all unique conversation IDs
    print("\nFetching unique conversation IDs...")
    cur.execute("SELECT COUNT(DISTINCT conversation_id) FROM demo_data.banking_conversations;")
    total_conversations = cur.fetchone()[0]
    print(f"✓ Found {total_conversations:,} unique conversations to process")

    # Process conversations in batches
    print("\nAggregating conversations by ID and sorting by timestamp...")
    aggregate_sql = """
    INSERT INTO demo_data.banking_conversations_preprocessed
    (conversation_id, conversation_text, start_time, end_time, message_count, speaker_types)
    SELECT
        conversation_id,
        STRING_AGG(
            speaker || ': ' || text,
            E'\n'
            ORDER BY date_time
        ) as conversation_text,
        MIN(date_time) as start_time,
        MAX(date_time) as end_time,
        COUNT(*) as message_count,
        ARRAY_AGG(DISTINCT speaker) as speaker_types
    FROM demo_data.banking_conversations
    GROUP BY conversation_id
    ON CONFLICT (conversation_id) DO NOTHING;
    """

    cur.execute(aggregate_sql)
    conn.commit()
    rows_inserted = cur.rowcount
    print(f"✓ Processed and inserted {rows_inserted:,} conversations")

    # Verify preprocessed data
    print("\nVerifying preprocessed data...")
    cur.execute("SELECT COUNT(*) FROM demo_data.banking_conversations_preprocessed;")
    total = cur.fetchone()[0]
    print(f"✓ Total conversations in preprocessed table: {total:,}")

    # Show sample preprocessed conversation
    print("\nSample preprocessed conversation:")
    cur.execute("""
        SELECT
            conversation_id,
            LEFT(conversation_text, 300) as preview,
            start_time,
            end_time,
            message_count,
            speaker_types
        FROM demo_data.banking_conversations_preprocessed
        LIMIT 1;
    """)

    sample = cur.fetchone()
    if sample:
        print(f"\n  Conversation ID: {sample[0]}")
        print(f"  Start Time: {sample[2]}")
        print(f"  End Time: {sample[3]}")
        print(f"  Message Count: {sample[4]}")
        print(f"  Speakers: {', '.join(sample[5])}")
        print(f"\n  Conversation Preview:\n  {sample[1][:300]}...")

    # Statistics
    print("\nConversation Statistics:")
    cur.execute("""
        SELECT
            AVG(message_count)::INTEGER as avg_messages,
            MIN(message_count) as min_messages,
            MAX(message_count) as max_messages
        FROM demo_data.banking_conversations_preprocessed;
    """)
    stats = cur.fetchone()
    print(f"  • Average messages per conversation: {stats[0]}")
    print(f"  • Minimum messages: {stats[1]}")
    print(f"  • Maximum messages: {stats[2]}")

    print("\n" + "="*70)
    print("✓ Preprocessing completed successfully!")
    print("="*70)

def main():
    print("Starting banking data import process...")

    # Parse command line arguments
    csv_filename = DEFAULT_CSV_FILE
    if len(sys.argv) > 1:
        if sys.argv[1] == "--full":
            csv_filename = FULL_CSV_FILE
            print("Using FULL dataset (5.5M records) - this will take longer!")
        elif sys.argv[1] == "--file" and len(sys.argv) > 2:
            csv_filename = sys.argv[2]
        else:
            print("Usage:")
            print("  python import_banking_data.py                    # Use sample (10k records)")
            print("  python import_banking_data.py --full             # Use full dataset (5.5M records)")
            print("  python import_banking_data.py --file custom.csv  # Use custom file")
            return

    print(f"Using CSV file: {csv_filename}")

    # Connect to PostgreSQL
    print(f"\nConnecting to database at {PARAMETERS['host']}:{PARAMETERS['port']}...")
    try:
        conn = psycopg2.connect(**PARAMETERS)
        cur = conn.cursor()
        print("✓ Successfully connected to database")
    except Exception as e:
        print(f"✗ Failed to connect to database: {e}")
        return

    try:
        # Create schema
        print("\nCreating schema 'demo_data'...")
        cur.execute("CREATE SCHEMA IF NOT EXISTS demo_data;")
        conn.commit()
        print("✓ Schema created successfully")

        # Create table
        print("\nCreating table 'banking_conversations'...")
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS demo_data.banking_conversations (
            conversation_id VARCHAR(255),
            speaker VARCHAR(50),
            date_time TIMESTAMP,
            text TEXT
        );
        """
        cur.execute(create_table_sql)
        conn.commit()
        print("✓ Table created successfully")

        # Find CSV file (in same directory as script)
        csv_path = Path(__file__).parent / csv_filename
        if not csv_path.exists():
            print(f"\n✗ CSV file not found at: {csv_path}")
            print(f"Please ensure {csv_filename} is in the AutoBankingCustomerService directory")
            return

        print(f"\n✓ Found CSV file at: {csv_path}")

        # Show file info
        file_size = csv_path.stat().st_size
        if file_size > 1024 * 1024 * 1024:
            size_str = f"{file_size / (1024**3):.2f} GB"
        elif file_size > 1024 * 1024:
            size_str = f"{file_size / (1024**2):.2f} MB"
        else:
            size_str = f"{file_size / 1024:.2f} KB"
        print(f"  File size: {size_str}")

        # Import CSV data
        print("\nImporting data from CSV...")
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            count = 0
            batch_size = 10000

            for row in reader:
                insert_sql = """
                INSERT INTO demo_data.banking_conversations
                (conversation_id, speaker, date_time, text)
                VALUES (%s, %s, %s, %s)
                """
                cur.execute(insert_sql, (
                    row['conversation_id'],
                    row['speaker'],
                    row['date_time'],
                    row['text']
                ))

                count += 1
                if count % batch_size == 0:
                    conn.commit()
                    print(f"  → Imported {count:,} records...")

            # Final commit
            conn.commit()
            print(f"\n✓ Total records imported: {count:,}")

        # Verify data
        print("\nVerifying imported data...")
        cur.execute("SELECT COUNT(*) FROM demo_data.banking_conversations;")
        total = cur.fetchone()[0]
        print(f"✓ Total records in table: {total:,}")

        # Show sample data
        print("\nSample data (first 3 records):")
        cur.execute("""
            SELECT conversation_id, speaker, date_time, LEFT(text, 50) as text_preview
            FROM demo_data.banking_conversations
            LIMIT 3;
        """)
        for row in cur.fetchall():
            print(f"  • ID: {row[0][:8]}... | Speaker: {row[1]} | Time: {row[2]} | Text: {row[3]}...")

        print("\n" + "="*70)
        print("✓ Data import completed successfully!")
        print("="*70)

        # Preprocess conversations
        preprocess_conversations(conn, cur)

    except Exception as e:
        print(f"\n✗ Error during import: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()
        print("\nDatabase connection closed.")

if __name__ == "__main__":
    main()
