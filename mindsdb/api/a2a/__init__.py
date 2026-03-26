# A2A specific imports
from mindsdb.api.a2a.common.types import (
    AgentCard,
    AgentCapabilities,
    AgentSkill,
)
from mindsdb.api.a2a.common.server.server import A2AServer
from mindsdb.api.a2a.task_manager import AgentTaskManager
from mindsdb.api.a2a.agent import MindsDBAgent
from mindsdb.utilities.config import config


def get_a2a_app(
    project_name: str = "mindsdb",
):
    mindsdb_port = config.get("api", {}).get("http", {}).get("port", 47334)

    # Prepare A2A artefacts (agent card & task-manager)
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
        url=f"http://127.0.0.1:{mindsdb_port}/a2a/",
        version="1.0.0",
        defaultInputModes=MindsDBAgent.SUPPORTED_CONTENT_TYPES,
        defaultOutputModes=MindsDBAgent.SUPPORTED_CONTENT_TYPES,
        capabilities=capabilities,
        skills=[skill],
    )

    task_manager = AgentTaskManager(
        project_name=project_name,
    )

    server = A2AServer(
        agent_card=agent_card,
        task_manager=task_manager,
    )
    return server.app
