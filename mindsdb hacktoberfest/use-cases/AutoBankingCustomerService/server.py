#!/usr/bin/env python3
"""
Banking Customer Service API Server

This FastAPI server handles the complete flow:
1. Receives conversation texts via POST /api/process-conversations
2. Uses MindsDB SDK to query classification_agent for analysis
3. Inserts results into PostgreSQL conversations_summary table
4. Returns Salesforce-formatted JSON
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
import mindsdb_sdk
import re

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

# MindsDB Configuration
MINDSDB_URL = 'http://127.0.0.1:47334'
AGENT_NAME = 'classification_agent'

# Global MindsDB connection (initialized on startup)
mindsdb_server = None
classification_agent = None

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

@app.on_event("startup")
async def startup_event():
    """Initialize database table and MindsDB connection on server startup"""
    global mindsdb_server, classification_agent

    print("\n" + "="*70)
    print("Starting Banking Customer Service API Server...")
    print("="*70)

    print("\nChecking database table...")
    try:
        if ensure_table_exists(db_config=DB_CONFIG, verbose=True):
            print("✓ Database ready")
        else:
            print("✗ Warning: Could not verify or create database table")
            print("  The server will start, but may encounter errors.")
    except Exception as e:
        print(f"✗ Error during database check: {e}")
        print("  The server will start, but may encounter errors.")

    print("\nConnecting to MindsDB...")
    try:
        mindsdb_server = mindsdb_sdk.connect(MINDSDB_URL)
        classification_agent = mindsdb_server.agents.get(AGENT_NAME)

    except Exception as e:
        print(f"✗ Error connecting to MindsDB: {e}")
        print("  The server will start, but agent queries will fail.")
        print("  Make sure MindsDB is running at " + MINDSDB_URL)

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

def query_agent(conversation_text: str) -> Dict[str, Any]:
    """
    Query the MindsDB classification agent to analyze conversation.
    Returns dict with 'summary' and 'resolved' (bool).
    """
    if classification_agent is None:
        raise HTTPException(
            status_code=503,
            detail="Classification agent not initialized. Make sure MindsDB is running."
        )

    try:
        # Query the agent with the conversation text
        completion = classification_agent.completion([{
            'question': conversation_text,
            'answer': None
        }])

        # Parse the agent response
        response_text = completion.content

        # Extract summary and status using regex
        summary_match = re.search(r'Summary:\s*(.+?)(?=Status:)', response_text, re.DOTALL)
        status_match = re.search(r'Status:\s*(RESOLVED|UNRESOLVED)', response_text, re.IGNORECASE)

        summary = summary_match.group(1).strip() if summary_match else response_text.strip()
        status = status_match.group(1).upper() if status_match else "UNRESOLVED"

        resolved = (status == "RESOLVED")

        return {
            "summary": summary,
            "resolved": resolved
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Agent query failed: {str(e)}"
        )

def insert_conversation_with_analysis(conversation_id: str, conversation_text: str, summary: str, resolved: bool) -> None:
    """Insert analyzed conversation into conversations_summary table"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            insert_sql = """
                INSERT INTO demo_data.conversations_summary
                (conversation_id, conversation_text, summary, resolved, created_at, updated_at)
                VALUES (%s, %s, %s, %s, NOW(), NOW());
            """
            cur.execute(insert_sql, (conversation_id, conversation_text, summary, resolved))
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to insert conversation: {str(e)}")
    finally:
        conn.close()

def create_salesforce_case(conversation_id: str, conversation_text: str, summary: str, resolved: bool) -> SalesforceCase:
    """Create a Salesforce case from conversation analysis"""
    status = "RESOLVED" if resolved else "UNRESOLVED"
    current_time = datetime.now().isoformat()

    # Truncate long conversation text for display
    display_text = conversation_text[:500] + "..." if len(conversation_text) > 500 else conversation_text

    return SalesforceCase(
        conversation_id=conversation_id,
        conversation_text=display_text,
        summary=summary,
        status=status,
        created_at=current_time,
        processed_at=current_time
    )

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

    Args:
        request: ConversationRequest with list of conversation texts

    Returns:
        SalesforceResponse with processed cases in Salesforce format
    """
    start_time = time.time()

    if not request.conversation_texts:
        raise HTTPException(status_code=400, detail="conversation_texts cannot be empty")

    salesforce_cases = []
    processed_count = 0

    for conv_text in request.conversation_texts:
        if not conv_text or not conv_text.strip():
            continue

        try:
            analysis = query_agent(conv_text)
            conversation_id = str(uuid.uuid4())
            insert_conversation_with_analysis(
                conversation_id=conversation_id,
                conversation_text=conv_text,
                summary=analysis['summary'],
                resolved=analysis['resolved']
            )
            case = create_salesforce_case(
                conversation_id=conversation_id,
                conversation_text=conv_text,
                summary=analysis['summary'],
                resolved=analysis['resolved']
            )
            salesforce_cases.append(case)
            processed_count += 1

        except Exception as e:
            print(f"Error processing conversation: {str(e)}")
            continue

    if not salesforce_cases:
        raise HTTPException(status_code=500, detail="No conversations could be processed")

    processing_time = time.time() - start_time

    # Return response
    return SalesforceResponse(
        success=True,
        total_conversations=len(request.conversation_texts),
        processed_count=processed_count,
        cases=salesforce_cases,
        processing_time_seconds=round(processing_time, 2)
    )

if __name__ == "__main__":
    import uvicorn
    print("Starting Banking Customer Service API Server...")
    print("API Documentation: http://localhost:8000/docs")
    print("Health Check: http://localhost:8000/health")
    uvicorn.run(app, host="0.0.0.0", port=8000)
