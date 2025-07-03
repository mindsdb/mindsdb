import sys
import logging
import click
from dotenv import load_dotenv

# A2A specific imports
from mindsdb.api.a2a.common.types import (
    AgentCard,
    AgentCapabilities,
    AgentSkill,
    MissingAPIKeyError,
)
from mindsdb.api.a2a.common.server.server import A2AServer
from mindsdb.api.a2a.task_manager import AgentTaskManager
from mindsdb.api.a2a.agent import MindsDBAgent

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


@click.command()
@click.option("--host", default="localhost", help="A2A server host")
@click.option("--port", default=10002, help="A2A server port", type=int)
@click.option("--mindsdb-host", default="localhost", help="MindsDB server host")
@click.option("--mindsdb-port", default=47334, help="MindsDB server port", type=int)
@click.option("--project-name", default="mindsdb", help="MindsDB project name")
@click.option(
    "--log-level",
    default="INFO",
    help="Logging level (DEBUG, INFO, WARNING, ERROR)",
)
def main(
    host: str,
    port: int,
    mindsdb_host: str,
    mindsdb_port: int,
    project_name: str,
    log_level: str,
):
    """Entry-point that starts an A2A compliant server which forwards tasks to an existing MindsDB agent."""

    # Configure logging level
    logging.getLogger().setLevel(getattr(logging, log_level.upper(), logging.INFO))

    # Load .env if present
    load_dotenv()

    try:
        logger.info(
            "Starting MindsDB A2A connector on http://%s:%s (project=%s)",
            host,
            port,
            project_name,
        )

        # --- 1. Prepare A2A artefacts (agent card & task-manager) --------------
        capabilities = AgentCapabilities(streaming=True)
        skill = AgentSkill(
            id="mindsdb_query",
            name="MindsDB Query",
            description="Executes natural-language queries via MindsDB agents.",
            tags=["database", "mindsdb", "query", "analytics"],
            examples=[
                "What trends exist in my sales data?",
                "Generate insights from the support tickets dataset.",
            ],
            inputModes=MindsDBAgent.SUPPORTED_CONTENT_TYPES,
            outputModes=MindsDBAgent.SUPPORTED_CONTENT_TYPES,
        )

        agent_card = AgentCard(
            name="MindsDB Agent Connector",
            description=(f"A2A connector that proxies requests to MindsDB agents in project '{project_name}'."),
            url=f"http://{host}:{port}",
            version="1.0.0",
            defaultInputModes=MindsDBAgent.SUPPORTED_CONTENT_TYPES,
            defaultOutputModes=MindsDBAgent.SUPPORTED_CONTENT_TYPES,
            capabilities=capabilities,
            skills=[skill],
        )

        task_manager = AgentTaskManager(
            project_name=project_name,
            mindsdb_host=mindsdb_host,
            mindsdb_port=mindsdb_port,
        )

        # --- 2. Start A2A server ----------------------------------------------
        logger.info("Starting A2A server…")
        server = A2AServer(
            agent_card=agent_card,
            task_manager=task_manager,
            host=host,
            port=port,
            endpoint="/a2a",  # Keep the same route used by the client
        )
        server.start()

    except MissingAPIKeyError as exc:
        logger.error("Authentication error: %s", exc)
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Unexpected error: %s", exc)
        sys.exit(1)


# This function is called by mindsdb/api/a2a/run_a2a.py
def main_with_config(config=None, *args, **kwargs):
    """
    Alternative entry point that accepts configuration as a dictionary.
    This is used when A2A is started as a subprocess of the main MindsDB process.

    Args:
        config (dict): Configuration dictionary
        args: Additional positional arguments
        kwargs: Additional keyword arguments
    """
    if config is None:
        config = {}

    # Extract configuration values with fallbacks to default values
    host = config.get("host", "localhost")
    port = int(config.get("port", 47338))
    mindsdb_host = config.get("mindsdb_host", "localhost")
    mindsdb_port = int(config.get("mindsdb_port", 47334))
    project_name = config.get("project_name", "mindsdb")
    log_level = config.get("log_level", "INFO")

    # Call the main function with the extracted configuration
    main.callback(
        host=host,
        port=port,
        mindsdb_host=mindsdb_host,
        mindsdb_port=mindsdb_port,
        project_name=project_name,
        log_level=log_level,
    )


if __name__ == "__main__":
    main()
