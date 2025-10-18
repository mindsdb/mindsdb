#!/usr/bin/env python3
"""
Banking Customer Service API Server

This FastAPI server handles the complete flow:
1. Receives conversation texts via POST /api/process-conversations
2. Inserts into PostgreSQL conversations_summary table
3. MindsDB TRIGGER automatically processes with classification_agent
4. Polls for completion and returns Salesforce-formatted JSON
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
import time
from typing import List, Dict, Any
import uuid
from datetime import datetime
import sys
from pathlib import Path

# Add script directory to Python path for imports
sys.path.insert(0, str(Path(__file__).parent / "script"))
from init_summary_table import ensure_table_exists

app = FastAPI(title="Banking Customer Service API", version="1.0.0")

# Enable CORS for frontend integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# PostgreSQL Configuration
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "demo",
    "user": "postgresql",
    "password": "psqlpasswd"
}

# Request/Response Models
class ConversationRequest(BaseModel):
    conversation_texts: List[str]
    
class SalesforceCase(BaseModel):
    conversation_id: str
    conversation_text: str
    summary: str | None  # Allow None for unprocessed conversations
    status: str  # "RESOLVED" or "UNRESOLVED" or "PROCESSING"
    created_at: str
    processed_at: str

class SalesforceResponse(BaseModel):
    success: bool
    total_conversations: int
    processed_count: int
    cases: List[SalesforceCase]
    processing_time_seconds: float

# Startup Event Handler
@app.on_event("startup")
async def startup_event():
    """Check and initialize database table on server startup"""
    print("\n" + "="*70)
    print("Starting Banking Customer Service API Server...")
    print("="*70)
    print("\nChecking database table...")

    try:
        # Ensure conversations_summary table exists
        if ensure_table_exists(db_config=DB_CONFIG, verbose=True):
            print("✓ Database ready")
        else:
            print("✗ Warning: Could not verify or create database table")
            print("  The server will start, but may encounter errors.")
    except Exception as e:
        print(f"✗ Error during database check: {e}")
        print("  The server will start, but may encounter errors.")

    print("\n" + "="*70)
    print("Server startup complete!")
    print("="*70)
    print("\n")

# Database Helper Functions
def get_db_connection():
    """Create PostgreSQL database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

def insert_conversation(conversation_text: str) -> str:
    """Insert conversation into conversations_summary table and return conversation_id"""
    conversation_id = str(uuid.uuid4())

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            insert_sql = """
                INSERT INTO demo_data.conversations_summary
                (conversation_id, conversation_text, created_at, summary, resolved)
                VALUES (%s, %s, NOW(), NULL, NULL)
                RETURNING conversation_id;
            """
            cur.execute(insert_sql, (conversation_id, conversation_text))
            conn.commit()
            return conversation_id
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to insert conversation: {str(e)}")
    finally:
        conn.close()

def poll_for_processing(conversation_ids: List[str], max_wait_seconds: int = 60, poll_interval: float = 2.0) -> List[Dict[str, Any]]:
    """
    Poll the database until MindsDB Agent completes processing.
    Returns list of processed conversation records.
    """
    start_time = time.time()

    while (time.time() - start_time) < max_wait_seconds:
        conn = get_db_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Check if all conversations have been processed (summary is not NULL)
                placeholders = ', '.join(['%s'] * len(conversation_ids))
                query = f"""
                    SELECT
                        conversation_id,
                        conversation_text,
                        summary,
                        resolved,
                        created_at,
                        updated_at
                    FROM demo_data.conversations_summary
                    WHERE conversation_id IN ({placeholders})
                    AND summary IS NOT NULL;
                """
                cur.execute(query, conversation_ids)
                processed_records = cur.fetchall()

                # If all conversations are processed, return results
                if len(processed_records) == len(conversation_ids):
                    return [dict(record) for record in processed_records]

        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Polling failed: {str(e)}")
        finally:
            conn.close()

        # Wait before next poll
        time.sleep(poll_interval)

    # Timeout: return whatever we have processed so far
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            placeholders = ', '.join(['%s'] * len(conversation_ids))
            query = f"""
                SELECT
                    conversation_id,
                    conversation_text,
                    summary,
                    resolved,
                    created_at,
                    updated_at
                FROM demo_data.conversations_summary
                WHERE conversation_id IN ({placeholders});
            """
            cur.execute(query, conversation_ids)
            all_records = cur.fetchall()
            return [dict(record) for record in all_records]
    finally:
        conn.close()

def format_salesforce_json(processed_records: List[Dict[str, Any]]) -> List[SalesforceCase]:
    """Convert processed records to Salesforce case format"""
    salesforce_cases = []

    for record in processed_records:
        # Parse summary to extract status
        summary_text = record.get('summary')  # Keep None if not processed yet
        resolved = record.get('resolved')

        # Determine status
        if summary_text is None or resolved is None:
            status = "PROCESSING"
        elif resolved is True:
            status = "RESOLVED"
        elif resolved is False:
            status = "UNRESOLVED"
        else:
            status = "PROCESSING"

        case = SalesforceCase(
            conversation_id=record['conversation_id'],
            conversation_text=record['conversation_text'][:500] + "..." if len(record['conversation_text']) > 500 else record['conversation_text'],
            summary=summary_text if summary_text else None,  # Keep None for unprocessed
            status=status,
            created_at=record['created_at'].isoformat() if record.get('created_at') else datetime.now().isoformat(),
            processed_at=record['updated_at'].isoformat() if record.get('updated_at') else datetime.now().isoformat()
        )
        salesforce_cases.append(case)

    return salesforce_cases

# API Endpoints
@app.get("/")
def root():
    """Health check endpoint"""
    return {
        "service": "Banking Customer Service API",
        "status": "running",
        "version": "1.0.0"
    }

@app.get("/health")
def health_check():
    """Database connectivity health check"""
    try:
        conn = get_db_connection()
        conn.close()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database unhealthy: {str(e)}")

@app.post("/api/process-conversations", response_model=SalesforceResponse)
async def process_conversations(request: ConversationRequest):
    """
    Process banking customer service conversations.

    Flow:
    1. Insert conversations into PostgreSQL conversations_summary table
    2. MindsDB TRIGGER automatically invokes classification_agent
    3. Agent analyzes conversation and updates summary + resolved status
    4. Poll for completion and return Salesforce-formatted JSON

    Args:
        request: ConversationRequest with list of conversation texts

    Returns:
        SalesforceResponse with processed cases in Salesforce format
    """
    start_time = time.time()

    if not request.conversation_texts:
        raise HTTPException(status_code=400, detail="conversation_texts cannot be empty")

    # Step 1: Insert all conversations and collect IDs
    conversation_ids = []
    for conv_text in request.conversation_texts:
        if not conv_text or not conv_text.strip():
            continue
        conversation_id = insert_conversation(conv_text)
        conversation_ids.append(conversation_id)

    if not conversation_ids:
        raise HTTPException(status_code=400, detail="No valid conversations to process")

    # Step 2: Wait for MindsDB TRIGGER + Agent processing to complete
    # The TRIGGER will automatically process new records and update summary/resolved
    processed_records = poll_for_processing(conversation_ids, max_wait_seconds=60, poll_interval=2.0)

    # Step 3: Format as Salesforce JSON
    salesforce_cases = format_salesforce_json(processed_records)

    processing_time = time.time() - start_time

    # Step 4: Return response
    return SalesforceResponse(
        success=True,
        total_conversations=len(request.conversation_texts),
        processed_count=len([case for case in salesforce_cases if case.status != "PROCESSING"]),
        cases=salesforce_cases,
        processing_time_seconds=round(processing_time, 2)
    )

@app.get("/api/conversations/{conversation_id}")
async def get_conversation(conversation_id: str):
    """Retrieve a specific conversation by ID"""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    conversation_id,
                    conversation_text,
                    summary,
                    resolved,
                    created_at,
                    updated_at
                FROM demo_data.conversations_summary
                WHERE conversation_id = %s;
            """, (conversation_id,))
            record = cur.fetchone()

            if not record:
                raise HTTPException(status_code=404, detail="Conversation not found")

            return dict(record)
    finally:
        conn.close()

@app.get("/api/conversations")
async def list_conversations(limit: int = 50, offset: int = 0, resolved: bool = None):
    """List conversations with optional filtering"""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            where_clause = ""
            params = []

            if resolved is not None:
                where_clause = "WHERE resolved = %s"
                params.append(resolved)

            query = f"""
                SELECT
                    conversation_id,
                    conversation_text,
                    summary,
                    resolved,
                    created_at,
                    updated_at
                FROM demo_data.conversations_summary
                {where_clause}
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s;
            """
            params.extend([limit, offset])

            cur.execute(query, params)
            records = cur.fetchall()

            return {
                "total": len(records),
                "limit": limit,
                "offset": offset,
                "conversations": [dict(record) for record in records]
            }
    finally:
        conn.close()

if __name__ == "__main__":
    import uvicorn
    print("Starting Banking Customer Service API Server...")
    print("API Documentation: http://localhost:8000/docs")
    print("Health Check: http://localhost:8000/health")
    uvicorn.run(app, host="0.0.0.0", port=8000)
