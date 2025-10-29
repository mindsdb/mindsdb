"""Startup operations for the application."""
import logging
import json
from pathlib import Path
import time
from typing import Dict, Any, List
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from config import get_config
from mindsdb import get_mindsdb_client
from utils import build_query_for_kb

logger = logging.getLogger(__name__)


class StartupError(Exception):
    """Exception raised during startup operations."""
    pass


def get_postgres_connection():
    """Get a direct PostgreSQL connection.
    
    Returns:
        PostgreSQL connection object or None
    """
    try:
        config = get_config()
        params = config.get("pgvector.parameters", {})
        
        conn = psycopg2.connect(
            host=params.get("host", "127.0.0.1"),
            port=params.get("port", 5432),
            database=params.get("database", "mydb"),
            user=params.get("user", "psql"),
            password=params.get("password", "psql")
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        return None


def setup_pgvector_database() -> Dict[str, Any]:
    """Create PostgreSQL pgvector database connection in MindsDB.
    
    Returns:
        Dict with success status and message
    """
    try:
        client = get_mindsdb_client()
        config = get_config()
        
        # Load pgvector configuration
        db_name = config.get("pgvector.database_name", "my_pgvector")
        engine = config.get("pgvector.engine", "pgvector")
        params = config.get("pgvector.parameters", {})
        
        logger.info(f"Setting up PostgreSQL database: {db_name}")
        logger.info(f"  Host: {params.get('host')}:{params.get('port')}")
        logger.info(f"  Database: {params.get('database')}")
        
        # Connect to MindsDB if not already connected
        if not client.is_connected():
            if not client.connect():
                return {
                    "success": False,
                    "operation": "pgvector_database",
                    "message": "Could not connect to MindsDB"
                }
        
        # Build CREATE DATABASE query
        params_str = ", ".join([f'"{k}": "{v}"' if isinstance(v, str) else f'"{k}": {v}' 
                                for k, v in params.items()])
        
        create_db_query = f"""
CREATE DATABASE {db_name}
WITH ENGINE = '{engine}',
PARAMETERS = {{
    {params_str}
}};
"""
        
        try:
            result = client.query(create_db_query)
            res = result.fetch()
            logger.info(f"PostgreSQL database '{db_name}' created successfully")
            return {
                "success": True,
                "operation": "pgvector_database",
                "message": f"Database '{db_name}' created successfully"
            }
        except Exception as e:
            error_msg = str(e).lower()
            if "already exists" in error_msg or "duplicate" in error_msg:
                logger.info(f"Database '{db_name}' already exists (skipping)")
                return {
                    "success": True,
                    "operation": "pgvector_database",
                    "message": f"Database '{db_name}' already exists",
                    "skipped": True
                }
            else:
                logger.error(f"Failed to create database: {e}")
                return {
                    "success": False,
                    "operation": "pgvector_database",
                    "message": f"Failed to create database: {str(e)}"
                }
                
    except Exception as e:
        logger.error(f"Error in setup_pgvector_database: {e}")
        return {
            "success": False,
            "operation": "pgvector_database",
            "message": f"Error: {str(e)}"
        }


def create_postgres_table() -> Dict[str, Any]:
    """Create paper_raw table in PostgreSQL.
    
    Creates a table with columns from metadata_columns and content_columns,
    all as VARCHAR, with an auto-incrementing ID.
    
    Returns:
        Dict with success status and message
    """
    try:
        config = get_config()
        
        # Get columns from configuration
        metadata_cols = config.get("knowledge_base.metadata_columns", ["product"])
        content_cols = config.get("knowledge_base.content_columns", ["notes"])
        all_columns = metadata_cols + content_cols
        
        logger.info("Creating PostgreSQL table: paper_raw")
        logger.info(f"  Columns: {', '.join(all_columns)}")
        
        # Connect to PostgreSQL
        conn = get_postgres_connection()
        if not conn:
            return {
                "success": False,
                "operation": "create_table",
                "message": "Could not connect to PostgreSQL"
            }
        
        try:
            cursor = conn.cursor()
            
            # Build column definitions
            column_defs = ["id SERIAL PRIMARY KEY"]
            for col in all_columns:
                column_defs.append(f"{col} VARCHAR NOT NULL")
            
            columns_str = ", ".join(column_defs)
            
            # Create table
            create_table_query = f"""
CREATE TABLE IF NOT EXISTS paper_raw (
    {columns_str}
);
"""
            
            cursor.execute(create_table_query)
            
            # Check if table exists and get row count
            cursor.execute("SELECT COUNT(*) FROM paper_raw;")
            row_count = cursor.fetchone()[0]
            
            cursor.close()
            conn.close()
            
            logger.info(f"Table 'paper_raw' created successfully (current rows: {row_count})")
            
            return {
                "success": True,
                "operation": "create_table",
                "message": f"Table 'paper_raw' created (columns: {', '.join(all_columns)})",
                "row_count": row_count
            }
            
        except Exception as e:
            if conn:
                conn.close()
            logger.error(f"Failed to create table: {e}")
            return {
                "success": False,
                "operation": "create_table",
                "message": f"Failed to create table: {str(e)}"
            }
            
    except Exception as e:
        logger.error(f"Error in create_postgres_table: {e}")
        return {
            "success": False,
            "operation": "create_table",
            "message": f"Error: {str(e)}"
        }


def insert_data_to_postgres(data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Insert data into PostgreSQL paper_raw table.
    
    Args:
        data: List of dictionaries with data to insert
        
    Returns:
        Dict with success status and message
    """
    try:
        config = get_config()
        
        # Get columns from configuration
        metadata_cols = config.get("knowledge_base.metadata_columns", ["product"])
        content_cols = config.get("knowledge_base.content_columns", ["notes"])
        all_columns = metadata_cols + content_cols
        
        logger.info(f"Inserting {len(data)} records to PostgreSQL table: paper_raw")
        
        # Connect to PostgreSQL
        conn = get_postgres_connection()
        if not conn:
            return {
                "success": False,
                "operation": "insert_data",
                "message": "Could not connect to PostgreSQL"
            }
        
        try:
            cursor = conn.cursor()
            
            # Build INSERT query
            columns_str = ", ".join(all_columns)
            placeholders = ", ".join(["%s"] * len(all_columns))
            
            insert_query = f"""
INSERT INTO paper_raw ({columns_str})
VALUES ({placeholders})
ON CONFLICT DO NOTHING;
"""
            
            # Insert each record
            inserted_count = 0
            for record in data:
                # Extract values in the same order as columns
                values = [record.get(col, "") for col in all_columns]
                
                try:
                    cursor.execute(insert_query, values)
                    inserted_count += 1
                except Exception as e:
                    logger.warning(f"Failed to insert record: {e}")
            
            # Get total row count
            cursor.execute("SELECT COUNT(*) FROM paper_raw;")
            total_rows = cursor.fetchone()[0]
            
            cursor.close()
            conn.close()
            
            logger.info(f"Inserted {inserted_count}/{len(data)} records (total rows: {total_rows})")
            
            return {
                "success": True,
                "operation": "insert_data",
                "message": f"Inserted {inserted_count} records to paper_raw",
                "inserted_count": inserted_count,
                "total_rows": total_rows
            }
            
        except Exception as e:
            if conn:
                conn.close()
            logger.error(f"Failed to insert data: {e}")
            return {
                "success": False,
                "operation": "insert_data",
                "message": f"Failed to insert data: {str(e)}"
            }
            
    except Exception as e:
        logger.error(f"Error in insert_data_to_postgres: {e}")
        return {
            "success": False,
            "operation": "insert_data",
            "message": f"Error: {str(e)}"
        }


def setup_knowledge_base() -> Dict[str, Any]:
    """Create knowledge base in MindsDB.
    
    Returns:
        Dict with success status and message
    """
    try:
        client = get_mindsdb_client()
        config = get_config()

        # Connect to MindsDB if not already connected
        if not client.is_connected():
            if not client.connect():
                return {
                    "success": False,
                    "operation": "knowledge_base",
                    "message": "Could not connect to MindsDB"
                }
        
        kb_name = config.get("knowledge_base.name", "my_pg_kb")
        create_kb_query = build_query_for_kb(kb_name, 'create')
        
        try:
            result = client.query(create_kb_query)
            _ = result.fetch()
            logger.info(f"Knowledge base '{kb_name}' created successfully")
            return {
                "success": True,
                "operation": "knowledge_base",
                "message": f"Knowledge base '{kb_name}' created successfully"
            }
        except Exception as e:
            error_msg = str(e).lower()
            if "already exists" in error_msg or "duplicate" in error_msg:
                logger.info(f"Knowledge base '{kb_name}' already exists (skipping)")
                return {
                    "success": True,
                    "operation": "knowledge_base",
                    "message": f"Knowledge base '{kb_name}' already exists",
                    "skipped": True
                }
            else:
                logger.error(f"Failed to create knowledge base: {e}")
                return {
                    "success": False,
                    "operation": "knowledge_base",
                    "message": f"Failed to create knowledge base: {str(e)}"
                }
                
    except Exception as e:
        logger.error(f"Error in setup_knowledge_base: {e}")
        return {
            "success": False,
            "operation": "knowledge_base",
            "message": f"Error: {str(e)}"
        }


def create_knowledge_base_index() -> Dict[str, Any]:
    """Create knowledge base Index in MindsDB.
    
    Returns:
        Dict with success status and message
    """
    try:
        client = get_mindsdb_client()
        config = get_config()
        
        # Load knowledge base configuration
        kb_name = config.get("knowledge_base.name", "my_pg_kb")
        
        logger.info(f"Creating knowledge base index: {kb_name}")
        
        # Connect to MindsDB if not already connected
        if not client.is_connected():
            if not client.connect():
                return {
                    "success": False,
                    "operation": "knowledge_base",
                    "message": "Could not connect to MindsDB"
                }
        
        # Build CREATE KNOWLEDGE_BASE query
        
        create_kb_index_query = f"""
CREATE INDEX ON KNOWLEDGE_BASE {kb_name};
"""
        
        try:
            time.sleep(3)
            result = client.query(create_kb_index_query)
            _ = result.fetch()
            logger.info(f"Knowledge base index for '{kb_name}' created successfully")
            return {
                "success": True,
                "operation": "knowledge_base",
                "message": f"Knowledge base index for '{kb_name}' created successfully"
            }
        except Exception as e:
            error_msg = str(e).lower()
            logger.error(f"Failed to create knowledge base index: {e}")
            return {
                "success": False,
                "operation": "knowledge_base_index",
                "message": f"Failed to create knowledge base index: {str(e)}"
            }
                
    except Exception as e:
        logger.error(f"Error in create_knowledge_base_index: {e}")
        return {
            "success": False,
            "operation": "knowledge_base_index",
            "message": f"Error: {str(e)}"
        }

def insert_to_knowledge_base() -> Dict[str, Any]:
    """Insert data from PostgreSQL table into MindsDB knowledge base.
    
    Uses the SQL command: INSERT INTO <kb_name> SELECT <content>, <metadata> FROM <db_name>.paper_raw
    
    Returns:
        Dict with success status and message
    """
    try:
        client = get_mindsdb_client()
        config = get_config()
        
        # Get configuration
        kb_name = config.get("knowledge_base.name", "my_pg_kb")
        db_name = config.get("pgvector.database_name", "my_pgvector")
        metadata_cols = config.get("knowledge_base.metadata_columns", ["product"])
        content_cols = config.get("knowledge_base.content_columns", ["notes"])
        
        logger.info(f"Inserting data to knowledge base: {kb_name}")
        logger.info(f"  Source: {db_name}.paper_raw")
        logger.info(f"  Content columns: {', '.join(content_cols)}")
        logger.info(f"  Metadata columns: {', '.join(metadata_cols)}")
        
        # Connect to MindsDB if not already connected
        if not client.is_connected():
            if not client.connect():
                return {
                    "success": False,
                    "operation": "kb_insertion",
                    "message": "Could not connect to MindsDB"
                }
        
        # Build column list: content columns first, then metadata columns
        all_columns = content_cols + metadata_cols
        columns_str = ", ".join(all_columns)
        
        # Build INSERT query
        insert_kb_query = f"""
INSERT INTO {kb_name}
    SELECT {columns_str}
    FROM {db_name}.paper_raw;
"""
        
        try:
            result = client.query(insert_kb_query)
            res = result.fetch()
            logger.info(res)
            logger.info(f"Data inserted to knowledge base '{kb_name}' successfully")
            return {
                "success": True,
                "operation": "kb_insertion",
                "message": f"Data inserted to knowledge base '{kb_name}' from {db_name}.paper_raw"
            }
        except Exception as e:
            logger.error(f"Failed to insert data to knowledge base: {e}")
            return {
                "success": False,
                "operation": "kb_insertion",
                "message": f"Failed to insert data: {str(e)}"
            }
                
    except Exception as e:
        logger.error(f"Error in insert_to_knowledge_base: {e}")
        return {
            "success": False,
            "operation": "kb_insertion",
            "message": f"Error: {str(e)}"
        }


def load_sample_data() -> Dict[str, Any]:
    """Load sample data from JSON file and insert to PostgreSQL.
    
    Returns:
        Dict with success status and message
    """
    try:
        project_root = Path(__file__).parent
        data_file = project_root / "data" / "sample_data.json"
        
        logger.info(f"Loading sample data from: {data_file.name}")
        
        if not data_file.exists():
            logger.warning(f"Sample data file not found: {data_file}")
            return {
                "success": True,
                "operation": "sample_data",
                "message": "Sample data file not found (skipping)",
                "skipped": True
            }
        
        # Load JSON data
        with open(data_file, 'r') as f:
            data = json.load(f)
        
        logger.info(f"Loaded {len(data)} records from sample data file")
        
        for rec in data:
            rec["text"] = "\n".join([f"{key}: {value}" for key, value in rec.items()])

        # Insert data to PostgreSQL table
        insert_result = insert_data_to_postgres(data)

        if insert_result.get("success"):
            logger.info(f"Data inserted to PostgreSQL: {insert_result.get('message')}")
            return {
                "success": True,
                "operation": "sample_data",
                "message": f"Loaded {len(data)} records and inserted to PostgreSQL",
                "records_count": len(data),
                "inserted_count": insert_result.get("inserted_count", 0)
            }
        else:
            logger.error(f"Failed to insert data to PostgreSQL: {insert_result.get('message')}")
            return {
                "success": False,
                "operation": "sample_data",
                "message": f"Failed to insert data: {insert_result.get('message')}"
            }
        
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON format in sample data: {e}")
        return {
            "success": False,
            "operation": "sample_data",
            "message": f"Invalid JSON format: {str(e)}"
        }
    except Exception as e:
        logger.error(f"Error loading sample data: {e}")
        return {
            "success": False,
            "operation": "sample_data",
            "message": f"Error: {str(e)}"
        }


def run_startup_operations(skip_on_error: bool = True) -> Dict[str, Any]:
    """Run all startup operations.
    
    Args:
        skip_on_error: If True, continue even if some operations fail
        
    Returns:
        Dict with results of all operations
        
    Raises:
        StartupError: If skip_on_error is False and any operation fails
    """
    logger.info("=" * 60)
    logger.info("Running startup operations...")
    logger.info("=" * 60)
    
    config = get_config()
    
    # Check if startup operations are enabled
    run_startup = config.get("app.run_startup", True)
    if not run_startup:
        logger.info("Startup operations disabled in configuration")
        return {
            "enabled": False,
            "message": "Startup operations disabled in configuration"
        }
    
    operations = [
        ("PostgreSQL Database", setup_pgvector_database),
        ("PostgreSQL Table", create_postgres_table),
        ("Sample Data", load_sample_data),
        ("Knowledge Base", setup_knowledge_base),
        ("KB Data Insertion", insert_to_knowledge_base),
        ("Create KB index", create_knowledge_base_index),
    ]
    
    results = {}
    all_successful = True
    
    for name, operation_func in operations:
        logger.info(f"\nRunning: {name}")
        try:
            result = operation_func()
            results[name] = result
            
            if result.get("success"):
                if result.get("skipped"):
                    logger.info(f"  Skipped: {result.get('message')}")
                else:
                    logger.info(f"  Success: {result.get('message')}")
            else:
                logger.error(f"  Failed: {result.get('message')}")
                all_successful = False
                
                if not skip_on_error:
                    raise StartupError(f"{name} failed: {result.get('message')}")
                    
        except Exception as e:
            logger.error(f"  Error: {e}")
            results[name] = {
                "success": False,
                "operation": name.lower().replace(" ", "_"),
                "message": str(e)
            }
            all_successful = False
            
            if not skip_on_error:
                raise
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("Startup Operations Summary")
    logger.info("=" * 60)
    
    for name, result in results.items():
        if result.get("success"):
            if result.get("skipped"):
                status = "SKIPPED"
            else:
                status = "SUCCESS"
        else:
            status = "FAILED"
        logger.info(f"  {status:8} - {name}")
    
    logger.info("=" * 60)
    
    return {
        "enabled": True,
        "all_successful": all_successful,
        "results": results,
        "summary": {
            "total": len(operations),
            "successful": sum(1 for r in results.values() if r.get("success")),
            "failed": sum(1 for r in results.values() if not r.get("success")),
            "skipped": sum(1 for r in results.values() if r.get("skipped"))
        }
    }
