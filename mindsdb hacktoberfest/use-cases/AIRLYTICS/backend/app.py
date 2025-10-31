
import os
import json
import math
import logging
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

import mindsdb_sdk
from dotenv import load_dotenv

from base_stats import summarize_metadata
from multivalue_stats import (
    conditional_rating_analysis,
    conditional_rating_to_rating_analysis,
    conditional_category_to_category_analysis,
    conditional_distribution_analysis,
    general_percentage_distribution
)

# ---------------------------------------------------------
# Load environment variables
# ---------------------------------------------------------
load_dotenv()

# ---------------------------------------------------------
# FastAPI setup
# ---------------------------------------------------------
app = FastAPI(title="AeroMind API - Airline Review Intelligence")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------
# Logging setup
# ---------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("aeromind_backend")

# ---------------------------------------------------------
# MindsDB Connection
# ---------------------------------------------------------
try:
    con = mindsdb_sdk.connect("http://127.0.0.1:47334")
    project = con.get_project("mindsdb")
    logger.info("✅ Connected to MindsDB successfully.")
except Exception as e:
    logger.error(f"Failed to connect to MindsDB: {e}")
    raise RuntimeError(f"Failed to connect to MindsDB: {e}")

# ---------------------------------------------------------
# Utilities
# ---------------------------------------------------------
def escape_sql_string(value: str) -> str:
    """Safely escape single quotes for SQL queries."""
    return value.replace("'", "''") if value else ""

def sanitize_for_json(obj: Any) -> Any:
    """Recursively replace NaN/Inf with None for JSON compliance."""
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize_for_json(x) for x in obj]
    return obj

def short_text(s: Optional[str], n: int = 280) -> str:
    if not s:
        return ""
    s = str(s).strip()
    return s if len(s) <= n else s[: n - 1] + "…"

# ---------------------------------------------------------
# Pydantic Models
# ---------------------------------------------------------
class AirlineSearchRequest(BaseModel):
    query: str
    airline_name: Optional[str] = None
    aircraft: Optional[str] = None
    type_of_traveller: Optional[str] = None
    seat_type: Optional[str] = None
    recommended: Optional[str] = None
    verified: Optional[bool] = None

    # Numeric rating fields
    overall_rating: Optional[float] = None
    seat_comfort: Optional[float] = None
    cabin_staff_service: Optional[float] = None
    food_beverages: Optional[float] = None
    ground_service: Optional[float] = None
    inflight_entertainment: Optional[float] = None
    wifi_connectivity: Optional[float] = None
    value_for_money: Optional[float] = None

    # Limit for MindsDB query
    limit: Optional[int] = 100


class InterpretAgentRequest(BaseModel):
    query: str
    reintr_query: Optional[str] = None
    top_reviews: List[Dict[str, Any]]  # expects a small set (top 5)
    base_stats: Dict[str, Any]
    special_stats: Optional[Dict[str, Any]] = None


# ---------------------------------------------------------
# ✅ Base Case Endpoint (Sanitized + Enhanced)
# ---------------------------------------------------------
@app.post("/base_case")
async def base_case_analysis(request: AirlineSearchRequest, limit: int = 500):
    """
    🧠 Base Case API (Final)
    Handles simple NLP search queries with all metadata filters.
    Sanitizes any invalid float values (NaN/Inf) before JSON return.
    """
    try:
        escaped_query = escape_sql_string(request.query)

        # --- Step 1: Build Base SQL Query ---
        sql_query = f"""
            SELECT *
            FROM airline_kb_10000
            WHERE content = '{escaped_query}'
        """

        # --- Step 2: Apply Filters ---
        if request.airline_name:
            sql_query += f" AND airline_name = '{escape_sql_string(request.airline_name)}'"
        if request.type_of_traveller:
            sql_query += f" AND type_of_traveller = '{escape_sql_string(request.type_of_traveller)}'"
        if request.seat_type:
            sql_query += f" AND seat_type = '{escape_sql_string(request.seat_type)}'"
        if request.recommended:
            sql_query += f" AND recommended = '{escape_sql_string(request.recommended)}'"
        if request.verified is not None:
            sql_query += f" AND verified = {str(request.verified).lower()}"

        numeric_fields = [
            "overall_rating",
            "seat_comfort",
            "cabin_staff_service",
            "food_beverages",
            "ground_service",
            "inflight_entertainment",
            "wifi_connectivity",
            "value_for_money",
        ]

        for field in numeric_fields:
            value = getattr(request, field, None)
            if value is not None:
                sql_query += f" AND {field} >= {value}"

        # --- Step 3: Apply Limit ---
        limit_value = getattr(request, "limit", limit) or limit
        sql_query += f" LIMIT {limit_value};"

        logger.info(f"Running Sanitized Base Case Query: {sql_query}")

        # --- Step 4: Execute Query ---
        result = project.query(sql_query)
        rows = result.fetch()
        if rows.empty:
            raise HTTPException(status_code=404, detail="No matching reviews found.")

        # --- Step 5: Parse Metadata ---
        metadata_list = []
        for _, row in rows.iterrows():
            try:
                meta = json.loads(row.get("metadata", "{}"))
                metadata_list.append(meta)
            except json.JSONDecodeError:
                continue

        if not metadata_list:
            raise HTTPException(status_code=500, detail="Metadata could not be parsed from results.")

        # --- Step 6: Compute Summary Stats ---
        stats_summary = summarize_metadata(metadata_list)
        if "error" in stats_summary:
            raise HTTPException(status_code=500, detail=stats_summary["error"])

        # --- Step 7: Prepare Display Rows ---
        display_rows = []
        for _, row in rows.head(50).iterrows():
            try:
                meta = json.loads(row.get("metadata", "{}"))
            except json.JSONDecodeError:
                meta = {}

            display_rows.append({
                "id": row.get("id"),
                "airline_name": meta.get("airline_name", "Unknown"),
                "review": row.get("chunk_content", ""),
                "overall_rating": meta.get("overall_rating"),
                "seat_comfort": meta.get("seat_comfort"),
                "cabin_staff_service": meta.get("cabin_staff_service"),
                "food_beverages": meta.get("food_beverages"),
                "ground_service": meta.get("ground_service"),
                "inflight_entertainment": meta.get("inflight_entertainment"),
                "wifi_connectivity": meta.get("wifi_connectivity"),
                "value_for_money": meta.get("value_for_money"),
                "recommended": meta.get("recommended"),
                "verified": meta.get("verified"),
                "type_of_traveller": meta.get("type_of_traveller"),
                "seat_type": meta.get("seat_type")
            })

        # --- Step 8: Build and Sanitize Response ---
        response_data = {
            "mode": "base_case",
            "query": request.query,
            "filters": request.dict(exclude_none=True),
            "total_results_fetched": len(rows),
            "display_rows": display_rows,
            "summary_stats": stats_summary,
            "note": f"Returning top 50 rows for UI display; stats computed on {len(rows)} samples."
        }

        sanitized = sanitize_for_json(response_data)
        return JSONResponse(content=sanitized)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Base Case API failed: {e}")
        raise HTTPException(status_code=500, detail=f"Base case analysis failed: {str(e)}")




# ---------------------------------------------------------
# 🧠 Smart Case Endpoint — Agent-driven Query Interpretation + Base Stats + Metadata Filters
# ---------------------------------------------------------
@app.post("/smart_case")
async def smart_case_analysis(request: AirlineSearchRequest, limit: int = 500):
    """
    🧩 Smart Case API (Enhanced with Metadata Filters)
    Uses analytics_query_agent to interpret complex natural-language queries.
    Applies all metadata filters like base_case.
    """
    try:
        # Step 1: Ask Agent to interpret query
        agent_sql = f"""
            SELECT answer
            FROM analytics_query_agent
            WHERE question = '{escape_sql_string(request.query)}';
        """
        logger.info(f"🧠 Running agent query: {agent_sql}")

        agent_result = project.query(agent_sql)
        agent_rows = agent_result.fetch()
        if agent_rows.empty:
            raise HTTPException(status_code=500, detail="Agent returned no response.")

        # Parse JSON
        try:
            agent_json = json.loads(agent_rows.iloc[0]["answer"])
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Agent response parse error: {str(e)}")

        mode = agent_json.get("mode", "base_case")
        new_query = agent_json.get("new_query", request.query)
        user_msg = agent_json.get("message", "")
        func_name = agent_json.get("function_to_call")
        params = agent_json.get("parameters", {})

        # Step 2: Build SQL Query with Metadata Filters (using agent's new_query)
        sql_query = f"""
            SELECT *
            FROM airline_kb_10000
            WHERE content = '{escape_sql_string(new_query)}'
        """

        # --- Apply Metadata Filters ---
        if request.airline_name:
            sql_query += f" AND airline_name = '{escape_sql_string(request.airline_name)}'"
        if request.type_of_traveller:
            sql_query += f" AND type_of_traveller = '{escape_sql_string(request.type_of_traveller)}'"
        if request.seat_type:
            sql_query += f" AND seat_type = '{escape_sql_string(request.seat_type)}'"
        if request.recommended:
            sql_query += f" AND recommended = '{escape_sql_string(request.recommended)}'"
        if request.verified is not None:
            sql_query += f" AND verified = {str(request.verified).lower()}"

        numeric_fields = [
            "overall_rating",
            "seat_comfort",
            "cabin_staff_service",
            "food_beverages",
            "ground_service",
            "inflight_entertainment",
            "wifi_connectivity",
            "value_for_money",
        ]

        for field in numeric_fields:
            value = getattr(request, field, None)
            if value is not None:
                sql_query += f" AND {field} >= {value}"

        # Apply Limit
        limit_value = getattr(request, "limit", limit) or limit
        sql_query += f" LIMIT {limit_value};"

        logger.info(f"📊 Running KB Query with agent's reinterpreted query: {sql_query}")
        
        # Step 3: Execute Query (single query for both base_case and special_case modes)
        result = project.query(sql_query)
        rows = result.fetch()
        if rows.empty:
            raise HTTPException(status_code=404, detail="No matching reviews found.")

        # Step 4: Parse Metadata
        metadata_list = []
        for _, row in rows.iterrows():
            try:
                meta = json.loads(row.get("metadata", "{}"))
                metadata_list.append(meta)
            except json.JSONDecodeError:
                continue

        if not metadata_list:
            raise HTTPException(status_code=500, detail="Metadata could not be parsed from MindsDB rows.")

        # Step 5: Prepare display rows (top 50)
        display_rows = []
        for _, row in rows.head(50).iterrows():
            try:
                meta = json.loads(row.get("metadata", "{}"))
            except json.JSONDecodeError:
                meta = {}

            display_rows.append({
                "id": row.get("id"),
                "airline_name": meta.get("airline_name", "Unknown"),
                "review": row.get("chunk_content", ""),
                "overall_rating": meta.get("overall_rating"),
                "seat_comfort": meta.get("seat_comfort"),
                "cabin_staff_service": meta.get("cabin_staff_service"),
                "food_beverages": meta.get("food_beverages"),
                "ground_service": meta.get("ground_service"),
                "inflight_entertainment": meta.get("inflight_entertainment"),
                "wifi_connectivity": meta.get("wifi_connectivity"),
                "value_for_money": meta.get("value_for_money"),
                "recommended": meta.get("recommended"),
                "verified": meta.get("verified"),
                "type_of_traveller": meta.get("type_of_traveller"),
                "seat_type": meta.get("seat_type")
            })

        # Step 6: Handle Base Case Mode
        if mode == "base_case":
            logger.info(f"⚙️ Agent interpreted as base case: {user_msg}")
            
            # Compute summary stats (same as base_case endpoint)
            stats_summary = summarize_metadata(metadata_list)
            if "error" in stats_summary:
                raise HTTPException(status_code=500, detail=stats_summary["error"])

            response_data = {
                "mode": "base_case",
                "query": request.query,
                "interpreted_query": new_query,
                "filters": request.dict(exclude_none=True),
                "total_results_fetched": len(rows),
                "display_rows": display_rows,
                "summary_stats": stats_summary,
                "note": f"{user_msg} Returning top 50 rows for UI display; stats computed on {len(rows)} samples."
            }

            return JSONResponse(content=sanitize_for_json(response_data))

        # Step 7: Handle Special Case Mode - Execute Analytical Function
        if not func_name:
            raise HTTPException(status_code=400, detail="Agent did not specify a function to call.")

        func_map = {
            "conditional_rating_analysis": conditional_rating_analysis,
            "conditional_rating_to_rating_analysis": conditional_rating_to_rating_analysis,
            "conditional_category_to_category_analysis": conditional_category_to_category_analysis,
            "conditional_distribution_analysis": conditional_distribution_analysis,
            "general_percentage_distribution": general_percentage_distribution
        }

        target_func = func_map.get(func_name)
        if not target_func:
            raise HTTPException(status_code=400, detail=f"Unknown analytical function: {func_name}")

        if "data" in params:
            params.pop("data")

        try:
            analysis_result = target_func(metadata_list, **params)
        except TypeError as e:
            logger.error(f"TypeError in {func_name}: {e}")
            raise HTTPException(status_code=500, detail=f"Function parameter mismatch: {str(e)}")
        except Exception as e:
            logger.error(f"Execution error in {func_name}: {e}")
            raise HTTPException(status_code=500, detail=f"Function execution failed: {str(e)}")

        if "error" in analysis_result:
            raise HTTPException(status_code=400, detail=analysis_result["error"])

        # Step 8: Compute Base Stats
        try:
            base_stats_result = summarize_metadata(metadata_list)
        except Exception as e:
            logger.warning(f"Base stats computation failed: {e}")
            base_stats_result = {"error": f"Base stats unavailable: {str(e)}"}

        # Step 9: Return Smart Response
        response_payload = {
            "mode": "special_case",
            "original_query": request.query,
            "semantic_query_used": new_query,
            "filters_applied": request.dict(exclude_none=True),
            "user_message": user_msg,
            "function_executed": func_name,
            "parameters_passed": params,
            "total_results_fetched": len(rows),
            "display_rows": display_rows,
            "multivalue_stats": analysis_result,
            "base_stats": base_stats_result,
            "note": f"Top 50 shown; both analytical and general stats computed on {len(rows)} filtered samples."
        }

        return JSONResponse(content=sanitize_for_json(response_payload))

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Smart Case Analysis failed: {e}")
        raise HTTPException(status_code=500, detail=f"Smart Case Analysis failed: {str(e)}")



# ---------------------------------------------------------
# 🧠🗣️ Interpret Agent Endpoint — explains results from either API
# ---------------------------------------------------------
@app.post("/interpret_agent")
async def interpret_agent(payload: InterpretAgentRequest):
    """
    Takes context from frontend and asks the MindsDB agent to produce a human
    readable, decision-oriented explanation.
    """
    try:
        # Keep only lightweight version of top reviews
        top5 = []
        for r in (payload.top_reviews or [])[:5]:
            top5.append({
                "airline_name": r.get("airline_name", "Unknown"),
                "overall_rating": r.get("overall_rating"),
                "recommended": r.get("recommended"),
                "verified": r.get("verified"),
                "seat_type": r.get("seat_type"),
                "type_of_traveller": r.get("type_of_traveller"),
                "snippet": short_text(r.get("review"), 320)
            })

        # 🔍 Build context with clear handling of optional fields
        context = {
            "query": payload.query or "No query provided",
            "reintr_query": payload.reintr_query or "Not reinterpreted",
            "top_reviews": top5,
            "base_stats": sanitize_for_json(payload.base_stats or {}),
            "special_stats": sanitize_for_json(payload.special_stats) if payload.special_stats else None,
            # 🆕 Add metadata for agent to understand context
            "metadata": {
                "has_reinterpretation": bool(payload.reintr_query and payload.reintr_query.strip()),
                "has_special_analysis": payload.special_stats is not None,
                "review_count": len(top5)
            }
        }

        # Convert to JSON and escape for SQL
        context_json = json.dumps(context, ensure_ascii=False, indent=2)
        escaped_context = escape_sql_string(context_json)

        # Query the agent
        sql = f"""
            SELECT answer
            FROM insight_interpreter_agent
            WHERE question = '{escaped_context}';
        """

        logger.info("🧠 Running insight_interpreter_agent")
        logger.debug(f"Context sent: {context_json[:500]}...")  # Log first 500 chars

        agent_res = project.query(sql)
        df = agent_res.fetch()
        
        if df.empty or "answer" not in df.columns:
            raise HTTPException(status_code=500, detail="Agent returned no response.")

        answer = df.iloc[0]["answer"]
        if not isinstance(answer, str):
            answer = json.dumps(answer)

        response = {
            "agent": "insight_interpreter_agent",
            "answer": answer,
            "echo": {
                "query": payload.query,
                "reinterpreted_query": payload.reintr_query,
                "top_reviews_count": len(top5),
                "has_special_stats": payload.special_stats is not None,
                "has_reinterpretation": bool(payload.reintr_query and payload.reintr_query.strip())
            }
        }
        return JSONResponse(content=sanitize_for_json(response))

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Interpret Agent failed: {e}")
        logger.exception(e)  # Full stack trace
        raise HTTPException(status_code=500, detail=f"Interpret Agent failed: {str(e)}")