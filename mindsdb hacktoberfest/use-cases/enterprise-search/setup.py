import argparse
import os

from dotenv import load_dotenv

from utils import setup_datasource, setup_jobs, setup_kb

# Load environment variables from .env files
# Try root .env first, then fall back to utils/confluence/.env
if os.path.exists(".env"):
    load_dotenv(".env")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Setup or refresh Confluence KB")
    parser.add_argument(
        "--mode",
        choices=["setup", "refresh"],
        default="setup",
        help="Operation mode: setup (full setup) or refresh (update existing KB)",
    )
    args = parser.parse_args()

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("Please set OPENAI_API_KEY environment variable")
        return

    # Azure OpenAI config (optional)
    azure_config = None
    azure_key = os.getenv("AZURE_OPENAI_API_KEY")
    if azure_key:
        azure_config = {
            "api_key": azure_key,
            "endpoint": os.getenv("AZURE_ENDPOINT", "https://tx-dev.openai.azure.com/"),
            "api_version": os.getenv("AZURE_API_VERSION", "2024-02-01"),
            "deployment": os.getenv("AZURE_DEPLOYMENT", "text-embedding-3-large"),
            "inference_deployment": os.getenv("AZURE_INFERENCE_DEPLOYMENT", "gpt-4.1"),
        }
        print("Using Azure OpenAI for embeddings")

    if args.mode == "setup":
        print("=" * 60)
        print("Setting up Confluence KB...")
        print("=" * 60)

        # Start Docker containers first
        # setup_docker_containers()

        setup_kb.drop_all_kbs()
        setup_datasource.drop_all_datasources()

        # Setup pgvector datasource
        setup_datasource.setup_pgvector_datasource()

        # Setup datasources
        setup_datasource.setup_confluence_datasource()
        setup_datasource.setup_jira_datasource()
        setup_datasource.setup_zendesk_datasource()

        # Create KB with pgvector storage
        setup_kb.create_confluence_kb(api_key, azure_config, use_pgvector=True)
        setup_kb.create_zendesk_kb(api_key, azure_config, use_pgvector=True)
        setup_kb.create_jira_kb(api_key, azure_config, use_pgvector=True)

        # Insert data
        setup_kb.insert_kb_data("confluence_kb", "confluence_datasource", "pages")
        setup_kb.insert_kb_data("jira_kb", "jira_datasource", "issues")
        setup_kb.insert_kb_data("zendesk_kb", "zendesk_datasource", "tickets")

        # Create agent with Azure config
        setup_kb.create_mindsdb_agent()

        # Example search
        results = setup_kb.search_confluence_kb("authentication")
        print(f"✓ Found {len(results)} results")

        setup_jobs.add_refresh_kb_jobs()

    elif args.mode == "refresh":
        print("=" * 60)
        print("Refreshing Confluence KB...")
        print("=" * 60)
        # Create agent with Azure config
        setup_kb.create_mindsdb_agent()
        # Refresh datasource and update KB
        setup_kb.refresh_kb("jira_kb", "jira_datasource", "issues")
        setup_kb.refresh_kb("zendesk_kb", "zendesk_datasource", "tickets")
        setup_kb.refresh_kb("confluence_kb", "confluence_datasource", "pages")


if __name__ == "__main__":
    main()
