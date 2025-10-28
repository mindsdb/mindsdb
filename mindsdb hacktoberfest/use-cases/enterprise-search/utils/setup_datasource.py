"""Setup and refresh Confluence KB with pgvector storage."""

import os

import mindsdb_sdk
from dotenv import load_dotenv

# Load environment variables from .env files
# Try root .env first, then fall back to utils/confluence/.env
root_env = os.path.join(os.path.dirname(__file__), "..", "..", ".env")

if os.path.exists(root_env):
    load_dotenv(root_env)

def drop_all_datasources() -> None:
    server = mindsdb_sdk.connect()
    for db in server.databases.list():
        if db.name in ["pgvector_datasource"] :
            server.databases.drop(db.name)

def setup_pgvector_datasource() -> None:
    """Create pgvector datasource in MindsDB."""
    server = mindsdb_sdk.connect()

    # Get credentials from environment
    host = os.getenv("PGVECTOR_HOST", "host.docker.internal")
    port = os.getenv("PGVECTOR_PORT", "5432")
    database = os.getenv("PGVECTOR_DATABASE", "mindsdb")
    user = os.getenv("PGVECTOR_USER", "mindsdb")
    password = os.getenv("PGVECTOR_PASSWORD", "mindsdb")

    # Drop existing datasource if needed
    try:
        server.databases.drop("pgvector_datasource")
        print("✓ Dropped existing pgvector_datasource")
    except Exception:
        pass

    # Create pgvector datasource using SDK
    server.databases.create(
        name="pgvector_datasource",
        engine="pgvector",
        connection_args={
            "host": host,
            "port": port,
            "database": database,
            "user": user,
            "password": password,
        },
    )
    print("✓ Created pgvector_datasource")


def setup_confluence_datasource() -> None:
    """Create Confluence datasource in MindsDB."""
    server = mindsdb_sdk.connect()

    # Get credentials from environment
    api_base = os.getenv("CONFLUENCE_API_BASE")
    username = os.getenv("CONFLUENCE_USERNAME")
    password = os.getenv("CONFLUENCE_PASSWORD")

    if not all([api_base, username, password]):
        raise ValueError(
            "Missing Confluence credentials. Set CONFLUENCE_API_BASE, "
            "CONFLUENCE_USERNAME, and CONFLUENCE_PASSWORD environment variables"
        )

    # Drop existing datasource if needed
    try:
        server.databases.drop("confluence_datasource")
        print("✓ Dropped existing confluence_datasource")
    except Exception:
        pass

    # Create Confluence datasource
    server.databases.create(
        name="confluence_datasource",
        engine="confluence",
        connection_args={
            "api_base": api_base,
            "username": username,
            "password": password,
        },
    )
    print("✓ Created confluence_datasource")

def setup_jira_datasource() -> None:
    """Create Jira datasource in MindsDB."""
    server = mindsdb_sdk.connect()

    api_base = os.getenv("JIRA_API_BASE")
    username = os.getenv("JIRA_USERNAME")
    password = os.getenv("JIRA_API_TOKEN")

    if not all([api_base, username, password]):
        raise ValueError(
            "Missing Jira credentials. Set JIRA_API_BASE, JIRA_USERNAME, JIRA_PASSWORD"
        )

    try:
        server.databases.drop("jira_datasource")
        print("✓ Dropped existing jira_datasource")
    except Exception:
        pass

    server.databases.create(
        name="jira_datasource",
        engine="jira",
        connection_args={
            "url": api_base,
            "username": username,
            "api_token": password,
        },
    )
    print("✓ Created jira_datasource")


def setup_zendesk_datasource() -> None:
    """Create Zendesk datasource in MindsDB."""
    server = mindsdb_sdk.connect()

    api_base = os.getenv("ZENDESK_SUBDOMAIN")
    username = os.getenv("ZENDESK_EMAIL")
    password = os.getenv("ZENDESK_API_TOKEN")

    if not all([api_base, username, password]):
        raise ValueError(
            "Missing Zendesk credentials. Set ZENDESK_API_BASE, ZENDESK_USERNAME, ZENDESK_PASSWORD"
        )

    try:
        server.databases.drop("zendesk_datasource")
        print("✓ Dropped existing zendesk_datasource")
    except Exception:
        pass

    server.databases.create(
        name="zendesk_datasource",
        engine="zendesk",
        connection_args={
            "sub_domain": api_base,
            "email": username,
            "api_key": password,
        },
    )
    print("✓ Created zendesk_datasource")
