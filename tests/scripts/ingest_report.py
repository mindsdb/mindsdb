import sys
import os
import json
import psycopg2
import uuid
from datetime import datetime

# Add the project root to the Python path to allow for absolute imports
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from tests.integration.handlers.utils import config

# Define the location of the report files
REPORTS_DIR = os.path.join(project_root, 'reports')
REPORT_FILE = os.path.join(REPORTS_DIR, 'report.json')
QUERY_LOG_FILE = os.path.join(REPORTS_DIR, 'all_handlers_query_log.json')


def create_tables(conn):
    """Creates or updates the test_results and test_query_logs tables."""
    # --- Main results table ---
    create_results_table_query = """
    CREATE TABLE IF NOT EXISTS test_results (
        id SERIAL PRIMARY KEY,
        execution_id UUID NOT NULL,
        test_area VARCHAR(100),
        executed_at TIMESTAMP WITH TIME ZONE,
        duration_seconds FLOAT NOT NULL,
        status VARCHAR(50) NOT NULL,
        test_name VARCHAR(255) NOT NULL,
        environment_info JSONB,
        test_configuration JSONB,
        error_details JSONB,
        CONSTRAINT unique_test_run UNIQUE (test_name, executed_at)
    );
    """
    # --- Detailed query logs table ---
    create_logs_table_query = """
    CREATE TABLE IF NOT EXISTS test_query_logs (
        id SERIAL PRIMARY KEY,
        execution_id UUID NOT NULL,
        test_name VARCHAR(255) NOT NULL,
        query_text TEXT,
        expected_response JSONB,
        actual_response JSONB,
        error_details TEXT,
        duration_seconds FLOAT,
        handler_name VARCHAR(100)
    );
    """
    # --- Add the handler_name and executed_at columns if they don't exist (for backward compatibility) ---
    alter_logs_table_query = """
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_name='test_query_logs' AND column_name='handler_name') THEN
            ALTER TABLE test_query_logs ADD COLUMN handler_name VARCHAR(100);
        END IF;

        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_name='test_query_logs' AND column_name='executed_at') THEN
            ALTER TABLE test_query_logs ADD COLUMN executed_at TIMESTAMP WITH TIME ZONE;
        END IF;
    END $$;
    """

    with conn.cursor() as cur:
        cur.execute(create_results_table_query)
        cur.execute(create_logs_table_query)
        cur.execute(alter_logs_table_query)
    conn.commit()
    print("Tables 'test_results' and 'test_query_logs' are ready.")

def ingest_main_report(conn, report_data, execution_id):
    """
    Parses the main JSON report, inserts into the test_results table,
    and returns the processed test data.
    """
    if 'tests' not in report_data:
        print("No tests found in the main report.")
        return []

    insert_query = """
    INSERT INTO test_results (
        execution_id, test_area, executed_at, duration_seconds, status,
        test_name, environment_info, test_configuration, error_details
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (test_name, executed_at) DO NOTHING;
    """
    
    executed_at = datetime.fromtimestamp(report_data['created'])
    env_info_json = json.dumps(report_data.get('environment'))
    test_config_dict = {
        'mindsdb_host': config.MINDSDB_HOST,
        'handlers_tested': config.HANDLERS_TO_TEST
    }
    test_config_json = json.dumps(test_config_dict)
    
    processed_tests = []
    with conn.cursor() as cur:
        for test in report_data['tests']:
            node_parts = test['nodeid'].split('/')
            test_area = node_parts[1] if len(node_parts) > 2 else "general"
            duration = test.get('call', {}).get('duration', 0)
            
            error_details_json = None
            if test['outcome'] not in ['passed', 'skipped']:
                longrepr = test.get('longrepr') or test.get('call', {}).get('longrepr')
                if longrepr:
                    error_details_json = json.dumps(longrepr)

            cur.execute(insert_query, (
                execution_id, test_area, executed_at, duration, test['outcome'],
                test['nodeid'], env_info_json, test_config_json, error_details_json
            ))
            if cur.rowcount > 0:
                print(f"Inserted main result for: {test['nodeid']}")
            
            processed_tests.append({'nodeid': test['nodeid'], 'executed_at': executed_at})

    conn.commit()
    return processed_tests

def ingest_query_logs(conn, query_log_data, execution_id, processed_tests):
    """Parses the detailed query log and inserts into the test_query_logs table."""
    if not processed_tests:
        print("Warning: No test runs found in the main report to associate logs with. Skipping query log ingestion.")
        return 0

    test_name_for_logs = processed_tests[0]['nodeid']
    execution_timestamp = processed_tests[0]['executed_at']

    insert_log_query = """
    INSERT INTO test_query_logs (
        execution_id, test_name, handler_name, query_text, expected_response,
        actual_response, error_details, duration_seconds, executed_at
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    
    with conn.cursor() as cur:
        total_logs_ingested = 0
        for handler_name, logs in query_log_data.items():
            for log_entry in logs:
                cur.execute(insert_log_query, (
                    execution_id,
                    test_name_for_logs,
                    handler_name,
                    log_entry['query'],
                    log_entry.get('actual_response'),  # Corrected: Use actual_response for the expected_response column
                    log_entry.get('actual_response'),
                    log_entry.get('error'),
                    log_entry.get('duration'),
                    execution_timestamp
                ))
            total_logs_ingested += len(logs)
            print(f"  -> Ingested {len(logs)} query log(s) for handler: {handler_name}")
    conn.commit()
    return total_logs_ingested

def main():
    """Main function to connect to the DB and run the ingestion."""
    if not all([config.PG_LOG_HOST, config.PG_LOG_DATABASE, config.PG_LOG_USER, config.PG_LOG_PASSWORD]):
        print("PostgreSQL logging environment variables (PG_LOG_*) not set. Exiting.")
        return

    if not os.path.exists(REPORT_FILE):
        print(f"Error: Main report file not found at {REPORT_FILE}")
        return

    execution_id = uuid.uuid4()
    print(f"Starting ingestion for execution ID: {execution_id}")

    conn = None
    try:
        conn = psycopg2.connect(
            host=config.PG_LOG_HOST, database=config.PG_LOG_DATABASE,
            user=config.PG_LOG_USER, password=config.PG_LOG_PASSWORD, port=config.PG_LOG_PORT
        )
        create_tables(conn)
        
        with open(REPORT_FILE, 'r') as f:
            report_data = json.load(f)
        processed_tests = ingest_main_report(conn, report_data, str(execution_id))
        print("✅ Main report data successfully ingested.")

        if os.path.exists(QUERY_LOG_FILE):
            with open(QUERY_LOG_FILE, 'r') as f:
                query_log_data = json.load(f)
            ingest_query_logs(conn, query_log_data, str(execution_id), processed_tests)
            print("✅ Detailed query log data successfully ingested.")
        else:
            print(f"Warning: Query log file not found at {QUERY_LOG_FILE}")

    except (Exception, psycopg2.Error) as error:
        print(f"❌ Error during ingestion: {error}")
    finally:
        if conn:
            conn.close()
            print("PostgreSQL connection closed.")

if __name__ == "__main__":
    main()