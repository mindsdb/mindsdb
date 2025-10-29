"""FastAPI application factory and startup hooks."""

from __future__ import annotations

import os
import mindsdb_sdk
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

from . import services
from .api import router as api_router
from .db import init_postgres, DEFAULT_DB_CONFIG
from .jira_client import JiraClientError, build_default_client
from .salesforce_client import SalesforceClientError, build_default_client as build_salesforce_client
from .mindsdb import register_agent, clear_agents

load_dotenv()

# Configuration
MINDSDB_URL = os.getenv("MINDSDB_URL", "http://127.0.0.1:47334")
AGENT_NAME = "classification_agent"
RECOMMENDATION_AGENT_NAME = "recommendation_agent"

services.set_db_config(DEFAULT_DB_CONFIG)

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

    # Step 1: Initialize PostgreSQL (conversations + analytics tables)
    print("\nStep 1: Initializing PostgreSQL database...")
    try:
        if init_postgres(db_config=DEFAULT_DB_CONFIG, verbose=True):
            print("✓ PostgreSQL ready")
        else:
            print("⚠ PostgreSQL initialization had some issues")
            print("  The server will start, but may encounter errors.")
    except Exception as exc:  # pragma: no cover - startup diagnostics
        print(f"✗ Error during PostgreSQL initialization: {exc}")
        print("  The server will start, but may encounter errors.")

    # Step 2: Initialize MindsDB (create database, engine, agents)
    print("\nStep 2: Initializing MindsDB...")
    try:
        from .mindsdb import init_mindsdb
        init_success = init_mindsdb(verbose=True, init_jobs=False)
        if init_success:
            print("✓ MindsDB initialization completed")
        else:
            print("⚠ MindsDB initialization had some issues, but continuing...")
    except Exception as init_exc:  # pragma: no cover - startup diagnostics
        print(f"✗ MindsDB initialization failed: {init_exc}")
        print("  Attempting to connect to MindsDB anyway...")

    # Step 3: Connect to MindsDB and register agents
    print("\nStep 3: Connecting to MindsDB and registering agents...")
    mindsdb_server = None
    try:
        mindsdb_server = mindsdb_sdk.connect(MINDSDB_URL)
        services.set_mindsdb_server(mindsdb_server)
        print("✓ Connected to MindsDB")

        # Register classification agent
        try:
            classification_agent = mindsdb_server.agents.get(AGENT_NAME)
            register_agent(AGENT_NAME, classification_agent)
            services.set_agent(classification_agent)
            print(f"✓ Registered agent: {AGENT_NAME}")
        except Exception as agent_exc:
            print(f"✗ Failed to register {AGENT_NAME}: {agent_exc}")

        # Register recommendation agent
        try:
            recommendation_agent = mindsdb_server.agents.get(RECOMMENDATION_AGENT_NAME)
            register_agent(RECOMMENDATION_AGENT_NAME, recommendation_agent)
            print(f"✓ Registered agent: {RECOMMENDATION_AGENT_NAME}")
        except Exception as rec_exc:
            print(f"✗ Recommendation agent not available: {rec_exc}")
            print("  Recommendation features will be disabled.")

        # Initialize analytics JOBs
        print("\nStep 3.5: Initializing MindsDB analytics JOBs...")
        try:
            from .mindsdb import init_mindsdb_jobs
            jobs_success = init_mindsdb_jobs(mindsdb_server, recreate=False, verbose=True)
            if jobs_success:
                print("✓ MindsDB JOBs initialized")
            else:
                print("⚠ MindsDB JOBs had some issues")
        except Exception as jobs_exc:
            print(f"⚠ MindsDB JOBs initialization failed: {jobs_exc}")

    except Exception as exc:  # pragma: no cover - startup diagnostics
        print(f"✗ Error connecting to MindsDB: {exc}")
        print("  The server will start, but agent queries will fail.")
        print(f"  Make sure MindsDB is running at {MINDSDB_URL}")

    # Step 4: Initialize Jira client
    print("\nStep 4: Initializing Jira client...")
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

    # Step 5: Initialize Salesforce client
    print("\nStep 5: Initializing Salesforce client...")
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
    """Cleanup on server shutdown."""
    services.clear_state()
    clear_agents()


__all__ = ["app"]
