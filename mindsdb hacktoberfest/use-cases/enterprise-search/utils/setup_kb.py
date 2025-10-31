"""Setup and refresh Confluence KB with pgvector storage."""

import os
import time
from typing import Dict, List

import mindsdb_sdk
from dotenv import load_dotenv

# Load environment variables from .env files
# Try root .env first, then fall back to utils/confluence/.env
root_env = os.path.join(os.path.dirname(__file__), "..", "..", ".env")

if os.path.exists(root_env):
    load_dotenv(root_env)


def drop_all_kbs() -> None:
    server = mindsdb_sdk.connect()

    for kb in server.knowledge_bases.list():
        server.knowledge_bases.drop(kb.name)


def create_confluence_kb(
    api_key: str, azure_config: dict = None, use_pgvector: bool = True
) -> None:
    """Create Confluence knowledge base with pgvector storage."""
    server = mindsdb_sdk.connect()

    # Drop existing KB if it exists
    try:
        server.knowledge_bases.drop("confluence_kb")
        print("✓ Dropped existing confluence_kb")
    except Exception:
        pass

    # Configure embedding model
    if azure_config:
        embedding_model = {
            "provider": "azure_openai",
            "model_name": azure_config.get("deployment", "text-embedding-3-large"),
            "api_key": azure_config.get("api_key"),
            "base_url": azure_config.get("endpoint"),
            "api_version": azure_config.get("api_version", "2024-02-01"),
            "deployment": azure_config.get("deployment", "text-embedding-3-large"),
            "rate_limit": 20,
        }
        reranking_model = {
            "provider": "azure_openai",
            "model_name": azure_config.get("inference_deployment", "gpt-4.1"),
            "api_key": azure_config.get("api_key"),
            "base_url": azure_config.get("endpoint"),
            "api_version": azure_config.get("api_version", "2024-02-01"),
            "deployment": azure_config.get("inference_deployment", "gpt-4.1"),
            "rate_limit": 20,
        }
    else:
        embedding_model = {
            "provider": "openai",
            "model_name": "text-embedding-3-small",
            "api_key": api_key,
        }
        reranking_model = {
            "provider": "openai",
            "model_name": "gpt-4o",
            "api_key": api_key,
        }

    # Create knowledge base with pgvector storage
    kb_params = {
        "name": "confluence_kb",
        "embedding_model": embedding_model,
        "reranking_model": reranking_model,
        "metadata_columns": [
            "id",
            "status",
            "title",
            "spaceId",
            "authorId",
            "createdAt",
        ],
        "content_columns": ["body_storage_value"],
        "id_column": "id",
    }

    if use_pgvector:
        # Use pgvector storage
        kb_params["storage"] = server.databases.pgvector_datasource.tables.pages
        print("Creating KB with pgvector storage...")
    else:
        print("Creating KB without storage...")

    server.knowledge_bases.create(**kb_params)
    print("✓ Created confluence_kb")


def create_jira_kb(
    api_key: str, azure_config: dict = None, use_pgvector: bool = True
) -> None:
    """Create Jira knowledge base."""
    server = mindsdb_sdk.connect()

    try:
        server.knowledge_bases.drop("jira_kb")
        print("✓ Dropped existing jira_kb")
    except Exception:
        pass

    embedding_model = {
        "provider": "azure_openai" if azure_config else "openai",
        "model_name": azure_config.get("deployment", "text-embedding-3-large")
        if azure_config
        else "text-embedding-3-small",
        "api_key": azure_config.get("api_key") if azure_config else api_key,
        "base_url": azure_config.get("endpoint") if azure_config else None,
        "api_version": azure_config.get("api_version", "2024-02-01")
        if azure_config
        else None,
        "deployment": azure_config.get("deployment") if azure_config else None,
        "rate_limit": 20 if azure_config else None,
    }

    reranking_model = {
        "provider": "azure_openai" if azure_config else "openai",
        "model_name": azure_config.get("inference_deployment", "gpt-4.1")
        if azure_config
        else "gpt-4o",
        "api_key": azure_config.get("api_key") if azure_config else api_key,
        "base_url": azure_config.get("endpoint") if azure_config else None,
        "api_version": azure_config.get("api_version", "2024-02-01")
        if azure_config
        else None,
        "deployment": azure_config.get("inference_deployment")
        if azure_config
        else None,
        "rate_limit": 20 if azure_config else None,
    }

    kb_params = {
        "name": "jira_kb",
        "embedding_model": embedding_model,
        "reranking_model": reranking_model,
        "metadata_columns": [
            "id",
            "key",
            "project_id",
            "project_key",
            "project_name",
            "priority",
            "creator",
            "assignee",
            "status",
        ],
        "content_columns": ["summary", "description"],
        "id_column": "id",
    }

    if use_pgvector:
        kb_params["storage"] = server.databases.pgvector_datasource.tables.jira_content
        print("Creating Jira KB with pgvector storage...")
    else:
        print("Creating Jira KB without storage...")

    server.knowledge_bases.create(**kb_params)
    print("✓ Created jira_kb")


def create_zendesk_kb(
    api_key: str, azure_config: dict = None, use_pgvector: bool = True
) -> None:
    """Create Zendesk knowledge base."""
    server = mindsdb_sdk.connect()

    try:
        server.knowledge_bases.drop("zendesk_kb")
        print("✓ Dropped existing zendesk_kb")
    except Exception:
        pass

    embedding_model = {
        "provider": "azure_openai" if azure_config else "openai",
        "model_name": azure_config.get("deployment", "text-embedding-3-large")
        if azure_config
        else "text-embedding-3-small",
        "api_key": azure_config.get("api_key") if azure_config else api_key,
        "base_url": azure_config.get("endpoint") if azure_config else None,
        "api_version": azure_config.get("api_version", "2024-02-01")
        if azure_config
        else None,
        "deployment": azure_config.get("deployment") if azure_config else None,
        "rate_limit": 20 if azure_config else None,
    }

    reranking_model = {
        "provider": "azure_openai" if azure_config else "openai",
        "model_name": azure_config.get("inference_deployment", "gpt-4.1")
        if azure_config
        else "gpt-4o",
        "api_key": azure_config.get("api_key") if azure_config else api_key,
        "base_url": azure_config.get("endpoint") if azure_config else None,
        "api_version": azure_config.get("api_version", "2024-02-01")
        if azure_config
        else None,
        "deployment": azure_config.get("inference_deployment")
        if azure_config
        else None,
        "rate_limit": 20 if azure_config else None,
    }

    kb_params = {
        "name": "zendesk_kb",
        "embedding_model": embedding_model,
        "reranking_model": reranking_model,
        "metadata_columns": [
            "id",
            "status",
            "priority",
            "type",
            "assignee_id",
            "requester_id",
            "tags",
            "url",
            "created_at",
            "updated_at",
        ],
        "content_columns": ["subject", "description"],
        "id_column": "id",
    }

    if use_pgvector:
        kb_params["storage"] = (
            server.databases.pgvector_datasource.tables.zendesk_tickets
        )
        print("Creating Zendesk KB with pgvector storage...")
    else:
        print("Creating Zendesk KB without storage...")

    server.knowledge_bases.create(**kb_params)


def insert_kb_data(
    kb_name: str, source_datasource=None, source_table=None, batch_size=5, delay=1
) -> None:
    server = mindsdb_sdk.connect()
    kb = server.knowledge_bases.get(kb_name)

    # Fetch rows from datasource
    rows = server.query(f"SELECT * FROM {source_datasource}.{source_table}").fetch()

    for i in range(0, len(rows), batch_size):
        try:
            batch = rows[i : i + batch_size]
            kb.insert(batch)
            print(f"Inserted {i + len(batch)} / {len(rows)} into {kb_name}")
            time.sleep(delay)
        except Exception as e:
            print(f"Error inserting batch {i} into {kb_name}: {e}")


def refresh_kb(
    kb_name: str, source_datasource: str = None, source_table: str = None
) -> None:
    server = mindsdb_sdk.connect()
    server.query(f"REFRESH {kb_name.replace('_kb', '_datasource')}")
    print(f"✓ Refreshed {kb_name.replace('_kb', '_datasource')}")
    insert_kb_data(kb_name, source_datasource, source_table)
    print(f"✓ Updated {kb_name}")


def search_confluence_kb(query: str, filters: Dict = None) -> List[Dict]:
    """Search Confluence knowledge base."""
    server = mindsdb_sdk.connect()

    # Build query following MindsDB docs pattern
    sql = f"SELECT * FROM confluence_kb WHERE content='{query}'"

    if filters:
        for key, value in filters.items():
            sql += f" AND {key}='{value}'"

    return server.query(sql).fetch()


def create_mindsdb_agent(azure_config: dict = None):
    """Create MindsDB agent with Azure OpenAI support."""
    server = mindsdb_sdk.connect()

    # Build model config based on whether Azure is available
    if azure_config and azure_config.get("api_key"):
        model_config = f'''{{
            "provider": "azure_openai",
            "model_name": "{azure_config.get("inference_deployment", "gpt-4.1")}",
            "api_key": "{azure_config["api_key"]}",
            "base_url": "{azure_config.get("endpoint", "https://tx-dev.openai.azure.com/")}",
            "api_version": "{azure_config.get("api_version", "2024-02-01")}"
        }}'''
        print("✓ Creating agent with Azure OpenAI")
    else:
        # Fallback to OpenAI
        import os

        api_key = os.getenv("OPENAI_API_KEY", "")
        model_config = f'''{{
            "provider": "openai",
            "model_name": "gpt-4.1",
            "api_key": "{api_key}"
        }}'''
        print("✓ Creating agent with OpenAI")

    query = f"""    
        CREATE AGENT support_agent
        USING
        model = {model_config},
        data = {{
             "knowledge_bases": ["mindsdb.confluence_kb", "mindsdb.zendesk_kb", "mindsdb.jira_kb"],
             "tables": ["zendesk_datasource.tickets", "jira_datasource.issues"]
        }},
        prompt_template='
            mindsdb.confluence_kb stores confluence docs related knowledge base
            mindsdb.zendesk_kb stores zendesk tickets related knowledge base
            mindsdb.jira_kb stores jira issues related knowledge base
            zendesk_datasource.tickets stores tickets data
            jira_datasource.issues stores issues data
        ';
    """

    try:
        server.query(query).fetch()
        print("✓ MindsDB agent created successfully")
    except Exception as e:
        print(f"⚠️  Agent creation failed: {e}")
