# backend/main.py
import os
import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, List, Dict
from datetime import datetime
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

load_dotenv()

MINDSDB_URL = os.getenv("MINDSDB_URL", "http://localhost:47334")
# REST SQL endpoint
SQL_ENDPOINT = f"{MINDSDB_URL}/api/sql/query"

API_KEY = os.getenv("MINDSDB_API_KEY", None)  # optional if MindsDB configured w/o auth

app = FastAPI(title="MediQuery Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

class AgentRequest(BaseModel):
    agent_name: str
    question: str
    # optional: override model or knowledge bases
    using: Optional[Dict] = None  # e.g. {"knowledge_bases": ["project.kb_name"]}

class KBSearchRequest(BaseModel):
    kb_full_name: str            # e.g. "project.patient_reviews_kb"
    query_text: str
    limit: Optional[int] = 10
    metadata_filters: Optional[Dict[str, str]] = None  # simple equality filters

def run_sql(sql: str) -> Dict:
    """
    Run SQL against MindsDB REST SQL endpoint.
    """
    payload = {"query": sql}
    headers = {"Content-Type": "application/json"}
    if API_KEY:
        headers["Authorization"] = f"Bearer {API_KEY}"
    try:
        resp = requests.post(f"{SQL_ENDPOINT}", json=payload, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        raise HTTPException(status_code=500, detail=f"MindsDB request failed: {e}")

@app.post("/ask_agent")
def ask_agent(req: AgentRequest):
    """
    Ask an agent created in MindsDB SQL interface.
    Example SQL:
      SELECT answer FROM <agent_name> WHERE question = '...'
      USING model = {...}, data = {...}
    """
    # Escape single quotes in question
    safe_q = req.question.replace("'", "''")
    using_clause = ""
    if req.using:
        # convert dict to JSON-ish inline SQL USING - basic handling
        import json
        using_json = json.dumps(req.using)
        using_clause = f" USING {using_json}"
    sql = f"SELECT answer FROM {req.agent_name} WHERE question = '{safe_q}'{using_clause};"
    result = run_sql(sql)
    # MindsDB returns rows under "data" -> "rows" typically. We'll return what came.
    return {"sql": sql, "mindsdb_response": result}

@app.post("/kb_search")
def kb_search(req: KBSearchRequest):
    """
    Perform a semantic KB query via SQL.
    Example:
      SELECT review_text, department, similarity_score
      FROM project.patient_reviews_kb
      WHERE content = '<query_text>'
      AND department = 'Cardiology'
      LIMIT N;
    """
    q = req.query_text.replace("'", "''")
    filters = ""
    if req.metadata_filters:
        for k, v in req.metadata_filters.items():
            safe_v = str(v).replace("'", "''")
            filters += f" AND {k} = '{safe_v}'"
    sql = (
        f"SELECT *, similarity_score "
        f"FROM {req.kb_full_name} "
        f"WHERE content = '{q}' {filters} "
        f"LIMIT {int(req.limit)};"
    )
    resp = run_sql(sql)
    return {"sql": sql, "mindsdb_response": resp}

@app.post("/raw_sql")
def raw_sql(query: Dict):
    """
    Run a raw SQL query; useful for debugging. Body: {"query":"SELECT ..."}
    """
    q = query.get("query")
    if not q:
        raise HTTPException(status_code=400, detail="Missing 'query' in request body")
    resp = run_sql(q)
    return resp

@app.get("/health")
def health():
    return {"status": "ok", "mindsdb_url": MINDSDB_URL}
