"""MindsDB initialization orchestration for AutoBankingCustomerService.

This module coordinates the complete MindsDB setup process:
1. Connect to MindsDB server
2. Create PostgreSQL database connection
3. Create OpenAI ML engine
4. Create all required agents
5. Initialize scheduled analytics JOBs
"""

import sys
from typing import Optional

import mindsdb_sdk

from .agents import create_agents
from .connection import (
    MINDSDB_URL,
    connect_to_mindsdb,
    create_openai_engine,
    create_postgres_connection,
)
from .jobs import init_mindsdb_jobs
from .knowledge_bases import init_knowledge_bases


def init_mindsdb(
    url: Optional[str] = None,
    verbose: bool = True,
    init_jobs: bool = True,
    recreate_jobs: bool = False,
    init_kbs: bool = True,
    recreate_kbs: bool = False
) -> bool:
    """Initialize MindsDB with all required components.

    This function:
    1. Connects to MindsDB
    2. Creates PostgreSQL database connection
    3. Creates OpenAI ML engine
    4. Creates all agents (classification, recommendation, analytics)
    5. Initializes scheduled analytics JOBs (optional)
    6. Initializes knowledge bases for Confluence integration (optional)

    Args:
        url: MindsDB URL (uses MINDSDB_URL env var if not provided)
        verbose: Whether to print detailed logs
        init_jobs: Whether to initialize scheduled JOBs
        recreate_jobs: If True, drop and recreate existing JOBs
        init_kbs: Whether to initialize knowledge bases
        recreate_kbs: If True, drop and recreate existing knowledge bases

    Returns:
        True if initialization was successful

    Example:
        >>> from app.mindsdb import init_mindsdb
        >>> success = init_mindsdb(verbose=True)
        >>> if success:
        ...     print("MindsDB ready!")
    """
    if verbose:
        print("=" * 70)
        print("MindsDB Initialization")
        print("=" * 70)

    # Step 1: Connect to MindsDB
    server = connect_to_mindsdb(url=url, verbose=verbose)
    if server is None:
        return False

    # Step 2: Create PostgreSQL connection
    if verbose:
        print("\nStep 2: Setting up PostgreSQL connection...")
    success = create_postgres_connection(server)
    if not success and verbose:
        print("\n⚠ PostgreSQL connection setup failed, but continuing...")

    # Step 3: Create OpenAI engine
    if verbose:
        print("\nStep 3: Setting up OpenAI ML engine...")
    success = create_openai_engine(server)
    if not success and verbose:
        print("\n⚠ OpenAI engine setup failed, but continuing...")

    # Step 4: Create all agents
    if verbose:
        print("\nStep 4: Creating MindsDB agents...")
    agent_results = create_agents(server)

    # Step 5: Initialize analytics JOBs (optional)
    if init_jobs:
        if verbose:
            print("\nStep 5: Initializing analytics JOBs...")
        try:
            jobs_success = init_mindsdb_jobs(server, recreate=recreate_jobs, verbose=verbose)
            if not jobs_success and verbose:
                print("\n⚠ Some JOBs had issues during initialization")
        except Exception as jobs_exc:
            if verbose:
                print(f"\n⚠ JOB initialization failed: {jobs_exc}")

    # Step 6: Initialize knowledge bases (optional)
    if init_kbs:
        if verbose:
            print("\nStep 6: Initializing knowledge bases...")
        try:
            kbs_success = init_knowledge_bases(server, recreate=recreate_kbs, verbose=verbose)
            if not kbs_success and verbose:
                print("\n⚠ Some knowledge bases had issues during initialization")
        except Exception as kbs_exc:
            if verbose:
                print(f"\n⚠ Knowledge base initialization failed: {kbs_exc}")

    # Summary
    if verbose:
        print("\n" + "=" * 70)
        print("✓ MindsDB initialization completed!")
        print("=" * 70)
        print("\nAvailable components:")
        print("  - Database: banking_postgres_db")
        print("  - ML Engine: openai_engine")
        print("  - Agents:")
        for agent_name, success in agent_results.items():
            status = "✓" if success else "✗"
            print(f"    {status} {agent_name}")
        if init_jobs:
            print("  - Analytics JOBs:")
            print("    • daily_conversation_analysis (Daily at 23:00)")
            print("    • weekly_trends_analysis (Sunday at 23:30)")
        if init_kbs:
            print("  - Knowledge Bases:")
            print("    • my_confluence_kb (Confluence documentation)")

    return True


def main(verbose: bool = True, init_jobs: bool = True) -> bool:
    """Main entry point for standalone execution.

    Args:
        verbose: Print detailed initialization logs
        init_jobs: Whether to initialize scheduled JOBs

    Returns:
        True if initialization was successful

    Example:
        Run from command line:
        $ python -m app.mindsdb.setup
    """
    success = init_mindsdb(verbose=verbose, init_jobs=init_jobs)
    return success


if __name__ == "__main__":
    success = main(verbose=True, init_jobs=True)
    sys.exit(0 if success else 1)
