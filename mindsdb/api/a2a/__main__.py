import sys
import logging
import click
from dotenv import load_dotenv

# A2A specific imports
from mindsdb.api.a2a import get_a2a_server
from mindsdb.api.a2a.common.types import (
    MissingAPIKeyError,
)

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

        server = get_a2a_server(
            host=host,
            mindsdb_host=mindsdb_host,
            mindsdb_port=mindsdb_port,
            project_name=project_name,
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
