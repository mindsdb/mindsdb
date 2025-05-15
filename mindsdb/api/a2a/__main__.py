import sys
import logging
import click
from dotenv import load_dotenv

# A2A specific imports
from common.types import (
    AgentCard,
    AgentCapabilities,
    AgentSkill,
    MissingAPIKeyError,
)
from common.server.server import A2AServer
from task_manager import AgentTaskManager
from agent import MindsDBAgent

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
@click.option(
    "--agent-name", default="my_agent", help="MindsDB agent name to connect to"
)
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
    agent_name: str,
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
            "Starting MindsDB A2A connector on http://%s:%s (project=%s, agent=%s)",
            host,
            port,
            project_name,
            agent_name,
        )

        # --- 1. Initialise MindsDB agent helper ---------------------------------
        mindsdb_agent = MindsDBAgent(
            agent_name=agent_name,
            project_name=project_name,
            host=mindsdb_host,
            port=mindsdb_port,
        )

        # --- 2. Prepare A2A artefacts (agent card & task-manager) --------------
        capabilities = AgentCapabilities(streaming=True)
        skill = AgentSkill(
            id="mindsdb_query",
            name="MindsDB Query",
            description=f"Executes natural-language queries via MindsDB agent '{agent_name}'.",
            tags=["database", "mindsdb", "query", "analytics"],
            examples=[
                "What trends exist in my sales data?",
                "Generate insights from the support tickets dataset.",
            ],
            inputModes=mindsdb_agent.SUPPORTED_CONTENT_TYPES,
            outputModes=mindsdb_agent.SUPPORTED_CONTENT_TYPES,
        )

        agent_card = AgentCard(
            name=f"MindsDB {agent_name} Connector",
            description=(
                "A2A connector that proxies requests to a MindsDB agent "
                f"'{project_name}.{agent_name}'."
            ),
            url=f"http://{host}:{port}",
            version="1.0.0",
            defaultInputModes=mindsdb_agent.SUPPORTED_CONTENT_TYPES,
            defaultOutputModes=mindsdb_agent.SUPPORTED_CONTENT_TYPES,
            capabilities=capabilities,
            skills=[skill],
        )

        task_manager = AgentTaskManager(agent=mindsdb_agent)

        # --- 3. Start A2A server ----------------------------------------------
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


if __name__ == "__main__":
    main()
