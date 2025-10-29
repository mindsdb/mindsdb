"""MindsDB integration package for AutoBankingCustomerService.

This package provides:
- Agent management (classification, recommendation, analytics)
- Database connection setup
- Job scheduling for automated analytics
- Knowledge base management for Confluence integration
- Initialization and orchestration

Usage:
    from app.mindsdb import init_mindsdb, get_agent

    # Initialize MindsDB components
    init_mindsdb(verbose=True)

    # Get an agent for queries
    agent = get_agent("classification_agent")
"""

from .agents import (
    AGENT_CONFIGS,
    check_agent_exists,
    clear_agents,
    create_agent,
    create_agents,
    get_agent,
    query_classification_agent,
    query_recommendation_agent,
    register_agent,
)
from .connection import (
    check_database_exists,
    check_ml_engine_exists,
    create_openai_engine,
    create_postgres_connection,
    get_mindsdb_server,
    set_mindsdb_server,
)
from .jobs import (
    check_job_exists,
    create_daily_analytics_job,
    create_weekly_trends_job,
    drop_job_if_exists,
    init_mindsdb_jobs,
)
from .knowledge_bases import (
    check_kb_exists,
    create_confluence_database,
    create_confluence_kb,
    drop_kb_if_exists,
    init_knowledge_bases,
)
from .setup import init_mindsdb

__all__ = [
    # Main initialization
    "init_mindsdb",
    # Agent functions
    "AGENT_CONFIGS",
    "check_agent_exists",
    "clear_agents",
    "create_agent",
    "create_agents",
    "get_agent",
    "query_classification_agent",
    "query_recommendation_agent",
    "register_agent",
    # Connection functions
    "check_database_exists",
    "check_ml_engine_exists",
    "create_openai_engine",
    "create_postgres_connection",
    "get_mindsdb_server",
    "set_mindsdb_server",
    # Job functions
    "check_job_exists",
    "create_daily_analytics_job",
    "create_weekly_trends_job",
    "drop_job_if_exists",
    "init_mindsdb_jobs",
    # Knowledge base functions
    "check_kb_exists",
    "create_confluence_database",
    "create_confluence_kb",
    "drop_kb_if_exists",
    "init_knowledge_bases",
]
