"""FastAPI application factory and startup hooks."""

from __future__ import annotations

import mindsdb_sdk
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

from . import services
from .api import router as api_router
from .db import ensure_table_exists
from .jira_client import JiraClientError, build_default_client
from .salesforce_client import SalesforceClientError, build_default_client as build_salesforce_client
from .recommendation_client import RecommendationClientError, build_recommendation_client

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "demo",
    "user": "postgresql",
    "password": "psqlpasswd",
}

MINDSDB_URL = "http://127.0.0.1:47334"
AGENT_NAME = "classification_agent"
RECOMMENDATION_AGENT_NAME = "recommendation_agent"

services.set_db_config(DB_CONFIG)
load_dotenv()

app = FastAPI(title="Banking Customer Service API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)


@app.on_event("startup")
async def startup_event() -> None:
    print("\n" + "=" * 70)
    print("Starting Banking Customer Service API Server...")
    print("=" * 70)

    print("\nChecking database table...")
    try:
        if ensure_table_exists(db_config=DB_CONFIG, verbose=True):
            print("✓ Database ready")
        else:
            print("✗ Warning: Could not verify or create database table")
            print("  The server will start, but may encounter errors.")
    except Exception as exc:  # pragma: no cover - startup diagnostics
        print(f"✗ Error during database check: {exc}")
        print("  The server will start, but may encounter errors.")

    print("\nConnecting to MindsDB...")
    try:
        mindsdb_server = mindsdb_sdk.connect(MINDSDB_URL)
        services.set_mindsdb_server(mindsdb_server)
        
        # Initialize classification agent
        classification_agent = mindsdb_server.agents.get(AGENT_NAME)
        services.set_agent(classification_agent)
        print("✓ Classification agent configured")
        
        # Initialize recommendation agent
        try:
            recommendation_agent = mindsdb_server.agents.get(RECOMMENDATION_AGENT_NAME)
            recommendation_client = build_recommendation_client(recommendation_agent)
            services.set_recommendation_client(recommendation_client)
            print("✓ Recommendation agent configured")
        except Exception as rec_exc:
            print(f"✗ Recommendation agent not available: {rec_exc}")
            print("  Recommendation features will be disabled.")
            
    except Exception as exc:  # pragma: no cover - startup diagnostics
        print(f"✗ Error connecting to MindsDB: {exc}")
        print("  The server will start, but agent queries will fail.")
        print(f"  Make sure MindsDB is running at {MINDSDB_URL}")

    print("\nInitializing Jira client...")
    try:
        jira_client = build_default_client()
        if jira_client:
            services.set_jira_client(jira_client)
            print("✓ Jira client configured")
        else:
            print("✗ Jira client not configured. Missing Jira environment variables.")
    except JiraClientError as exc:
        print(f"✗ Jira client configuration error: {exc}")
    except Exception as exc:  # pragma: no cover - startup diagnostics
        print(f"✗ Unexpected Jira initialization error: {exc}")

    print("\nInitializing Salesforce client...")
    try:
        salesforce_client = build_salesforce_client()
        if salesforce_client:
            services.set_salesforce_client(salesforce_client)
            print("✓ Salesforce client configured")
        else:
            print("✗ Salesforce client not configured. Missing Salesforce environment variables.")
    except SalesforceClientError as exc:
        print(f"✗ Salesforce client configuration error: {exc}")
    except Exception as exc:  # pragma: no cover - startup diagnostics
        print(f"✗ Unexpected Salesforce initialization error: {exc}")

    print("\n" + "=" * 70)
    print("Server startup complete!")
    print("=" * 70)
    print("\n")


@app.on_event("shutdown")
async def shutdown_event() -> None:
    services.clear_state()


__all__ = ["app"]
