from common.server import A2AServer
from common.types import AgentCard, AgentCapabilities, AgentSkill, MissingAPIKeyError
from task_manager import AgentTaskManager
from agent import MindsDBAgent
import click
import os
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@click.command()
@click.option("--host", default="localhost")
@click.option("--port", default=10002)
def main(host, port):
    try:
        
        capabilities = AgentCapabilities(streaming=True)
        skill = AgentSkill(
            id="chat_with_your_data",
            name="Chat with your data",
            description="Interact with your databases and tables through natural language queries using MindsDB.",
            tags=["database", "sql", "mindsdb", "data analysis"],
            examples=[
                "What TABLES are in my database?",
                "What are some good queries to run on my data?"
            ],
        )
        agent_card = AgentCard(
            name="MindsDB Data Chat Agent",
            description="An agent that allows you to interact with your data through natural language queries using MindsDB's capabilities. Query and analyze your databases conversationally.",
            url=f"http://{host}:{port}/",
            version="1.0.0",
            defaultInputModes=MindsDBAgent.SUPPORTED_CONTENT_TYPES,
            defaultOutputModes=MindsDBAgent.SUPPORTED_CONTENT_TYPES,
            capabilities=capabilities,
            skills=[skill],
        )
        server = A2AServer(
            agent_card=agent_card,
            task_manager=AgentTaskManager(agent=MindsDBAgent()),
            host=host,
            port=port,
        )
        server.start()
    except MissingAPIKeyError as e:
        logger.error(f"Error: {e}")
        exit(1)
    except Exception as e:
        logger.error(f"An error occurred during server startup: {e}")
        exit(1)
    
if __name__ == "__main__":
    main()

