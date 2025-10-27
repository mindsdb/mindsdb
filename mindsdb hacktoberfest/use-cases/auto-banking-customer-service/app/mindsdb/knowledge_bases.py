"""MindsDB Knowledge Base management for Confluence integration.

This module creates and manages knowledge bases that:
1. Connect to Confluence documentation
2. Create embeddings for semantic search
3. Integrate with recommendation agents
"""

import os
from typing import Optional

from dotenv import load_dotenv

load_dotenv()


def check_kb_exists(server, kb_name: str) -> bool:
    """Check if a knowledge base exists in MindsDB.

    Args:
        server: MindsDB server connection
        kb_name: Name of the knowledge base to check

    Returns:
        True if knowledge base exists, False otherwise
    """
    try:
        kbs = server.knowledge_bases.list()
        return any(kb.name == kb_name for kb in kbs)
    except Exception as e:
        print(f"⚠ Error checking knowledge base '{kb_name}': {e}")
        return False


def drop_kb_if_exists(server, kb_name: str) -> bool:
    """Drop a knowledge base if it exists.

    Args:
        server: MindsDB server connection
        kb_name: Name of the knowledge base to drop

    Returns:
        True if operation was successful
    """
    try:
        if check_kb_exists(server, kb_name):
            server.knowledge_bases.drop(kb_name)
            print(f"✓ Dropped existing knowledge base '{kb_name}'")
        return True
    except Exception as e:
        print(f"✗ Error dropping knowledge base '{kb_name}': {e}")
        return False


def create_confluence_database(server) -> bool:
    """Create Confluence database connection in MindsDB.

    Args:
        server: MindsDB server connection

    Returns:
        True if database was created successfully or already exists
    """
    confluence_base_url = os.getenv("CONFLUENCE_BASE_URL", "")
    confluence_email = os.getenv("CONFLUENCE_EMAIL", "")
    confluence_token = os.getenv("CONFLUENCE_API_TOKEN", "")

    if not all([confluence_base_url, confluence_email, confluence_token]):
        print("⚠ Confluence credentials not found in environment variables")
        print("  Skipping Confluence database creation")
        return False

    db_name = "my_confluence"

    # Check if database already exists
    try:
        databases = server.list_databases()
        if any(db.name == db_name for db in databases):
            print(f"✓ Confluence database '{db_name}' already exists")
            return True
    except Exception:
        pass

    print(f"\nCreating Confluence database '{db_name}'...")

    try:
        server.create_database(
            name=db_name,
            engine="confluence",
            connection_args={
                "api_base": confluence_base_url,
                "username": confluence_email,
                "password": confluence_token
            }
        )
        print(f"✓ Successfully created Confluence database '{db_name}'")
        return True
    except Exception as e:
        print(f"✗ Failed to create Confluence database: {e}")
        return False


def create_confluence_kb(server, recreate: bool = False) -> bool:
    """Create knowledge base from Confluence pages.

    This creates a knowledge base with embeddings from specific Confluence pages
    for use with the recommendation agent.

    Args:
        server: MindsDB server connection
        recreate: If True, drop existing KB and recreate it

    Returns:
        True if KB was created successfully
    """
    kb_name = "my_confluence_kb"
    confluence_db = "my_confluence"

    # Get OpenAI API key
    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key:
        print("⚠ OPENAI_API_KEY not found in environment variables")
        print("  Skipping knowledge base creation")
        return False

    # Ensure Confluence database exists
    if not create_confluence_database(server):
        return False

    if recreate:
        drop_kb_if_exists(server, kb_name)
    elif check_kb_exists(server, kb_name):
        print(f"✓ Knowledge base '{kb_name}' already exists, skipping creation")
        return True

    print(f"\nCreating knowledge base '{kb_name}'...")

    try:
        # Create knowledge base with embeddings
        kb = server.knowledge_bases.create(
            name=kb_name,
            embedding_model={
                "provider": "openai",
                "model_name": "text-embedding-3-small",
                "api_key": api_key
            }
        )
        print(f"✓ Successfully created knowledge base '{kb_name}'")

        # Insert data from Confluence pages
        print(f"\nInserting data from Confluence pages...")

        # Note: The SQL INSERT approach from mindsdb_setup.sql:
        # INSERT INTO my_confluence_kb (
        #     SELECT id, title, body_storage_value
        #     FROM my_confluence.pages
        #     WHERE id IN ('360449','589825')
        # );

        # For SDK, we need to use SQL for inserting data
        try:
            result = server.query(f"""
                INSERT INTO {kb_name} (
                    SELECT id, title, body_storage_value
                    FROM {confluence_db}.pages
                    WHERE id IN ('360449','589825')
                )
            """)
            print(f"✓ Inserted Confluence pages into knowledge base")
        except Exception as insert_exc:
            print(f"⚠ Could not insert data from Confluence: {insert_exc}")
            print(f"  Knowledge base created but empty")

        print(f"✓ Knowledge base '{kb_name}' is ready")
        return True

    except Exception as e:
        print(f"✗ Failed to create knowledge base '{kb_name}': {e}")
        return False


def init_knowledge_bases(server, recreate: bool = False, verbose: bool = True) -> bool:
    """Initialize all MindsDB knowledge bases.

    Args:
        server: MindsDB server connection
        recreate: If True, drop and recreate all knowledge bases
        verbose: Print progress messages

    Returns:
        True if all knowledge bases created successfully
    """
    if verbose:
        print("=" * 70)
        print("Initializing MindsDB Knowledge Bases")
        print("=" * 70)

    success = True

    # Create Confluence knowledge base
    if verbose:
        print("\n[1/1] Setting up Confluence knowledge base...")

    if not create_confluence_kb(server, recreate=recreate):
        if verbose:
            print("⚠ Confluence knowledge base creation had issues")
        success = False

    if verbose:
        print("\n" + "=" * 70)
        if success:
            print("MindsDB Knowledge Bases Initialization Complete!")
        else:
            print("MindsDB Knowledge Bases Initialization Completed with Warnings")
        print("=" * 70)
        if success:
            print("\nAvailable knowledge bases:")
            print("  • my_confluence_kb - Confluence documentation")

    return success


if __name__ == "__main__":
    # Can be run standalone for testing
    import mindsdb_sdk

    MINDSDB_URL = os.getenv("MINDSDB_URL", "http://127.0.0.1:47334")

    server = mindsdb_sdk.connect(MINDSDB_URL)
    init_knowledge_bases(server, recreate=False, verbose=True)
