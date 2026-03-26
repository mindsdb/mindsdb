import datetime
from typing import Dict, Iterator, List, Union, Tuple, Optional, Any, Text
import copy

from enum import Enum
from pydantic import BaseModel
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy import null
import pandas as pd

from mindsdb.interfaces.storage import db
from mindsdb.interfaces.storage.db import Predictor
from mindsdb.utilities.context import context as ctx
from mindsdb.interfaces.database.projects import ProjectController
from mindsdb.interfaces.model.functions import PredictorRecordNotFound
from mindsdb.interfaces.model.model_controller import ModelController
from mindsdb.utilities.config import config
from mindsdb.utilities.utils import validate_pydantic_params
from mindsdb.utilities import log
from mindsdb.interfaces.agents.utils.sql_toolkit import MindsDBQuery

from mindsdb.utilities.exception import EntityExistsError, EntityNotExistsError

from .utils.constants import ASSISTANT_COLUMN, SUPPORTED_PROVIDERS, PROVIDER_TO_MODELS
from .utils.pydantic_ai_model_factory import get_llm_provider
from .pydantic_ai_agent import check_agent_llm

logger = log.getLogger(__name__)

default_project = config.get("default_project")


def check_agent_data(data):
    tables = data.get("tables", [])
    knowledge_bases = data.get("knowledge_bases", [])
    if tables or knowledge_bases:
        sql_toolkit = MindsDBQuery(tables=tables, knowledge_bases=knowledge_bases)

        if tables and len(sql_toolkit.get_usable_table_names(lazy=False)) == 0:
            raise ValueError(f"No tables found: {tables}")

        if knowledge_bases and len(sql_toolkit.get_usable_knowledge_base_names(lazy=False)) == 0:
            raise ValueError(f"No knowledge bases found: {knowledge_bases}")


class AgentParamsData(BaseModel):
    knowledge_bases: List[str] | None = None
    tables: List[str] | None = None

    class Config:
        extra = "forbid"


class AgentMode(Enum):
    TEXT = "text"
    SQL = "sql"


class AgentParams(BaseModel):
    prompt_template: str | None = None
    model: Dict[Text, Any] | None = None
    data: AgentParamsData | None = None
    timeout: int | None = None
    mode: AgentMode = AgentMode.TEXT

    class Config:
        extra = "forbid"


class AgentsController:
    """Handles CRUD operations at the database level for Agents"""

    assistant_column = ASSISTANT_COLUMN

    def __init__(
        self,
        project_controller: ProjectController = None,
        model_controller: ModelController = None,
    ):
        if project_controller is None:
            project_controller = ProjectController()
        if model_controller is None:
            model_controller = ModelController()
        self.project_controller = project_controller
        self.model_controller = model_controller

    def check_model_provider(self, model_name: str, provider: str = None) -> Tuple[Optional[str], str]:
        """
        Checks if a model exists, and gets the provider of the model.

        The provider is either the provider of the model or the provider given as an argument.

        Parameters:
            model_name (str): The name of the model
            provider (str): The provider to check

        Returns:
            model (dict): The model object
            provider (str): The provider of the model
        """
        model = None

        # Handle the case when model_name is None (using default LLM)
        if model_name is None:
            return model, provider

        try:
            model_name_no_version, model_version = Predictor.get_name_and_version(model_name)
            model = self.model_controller.get_model(model_name_no_version, version=model_version)
            provider = "mindsdb" if model.get("provider") is None else model.get("provider")
        except PredictorRecordNotFound:
            if not provider:
                # If provider is not given, get it from the model name
                provider = get_llm_provider({"model_name": model_name})

            elif provider not in SUPPORTED_PROVIDERS and model_name not in PROVIDER_TO_MODELS.get(provider, []):
                raise ValueError(f"Model with name does not exist for provider {provider}: {model_name}")

        return model, provider

    def get_agent(self, agent_name: str, project_name: str = default_project) -> Optional[db.Agents]:
        """
        Gets an agent by name.

        Parameters:
            agent_name (str): The name of the agent
            project_name (str): The name of the containing project - must exist

        Returns:
            agent (Optional[db.Agents]): The database agent object
        """

        project = self.project_controller.get(name=project_name)
        agent_query = db.Agents.query.filter(
            db.Agents.name == agent_name,
            db.Agents.project_id == project.id,
            db.Agents.company_id == ctx.company_id,
            db.Agents.deleted_at == null(),
        )
        if ctx.enforce_user_id:
            agent_query = agent_query.filter(db.Agents.user_id == ctx.user_id)
        return agent_query.first()

    def get_agent_by_id(self, id: int, project_name: str = default_project) -> db.Agents:
        """
        Gets an agent by id.

        Parameters:
            id (int): The id of the agent
            project_name (str): The name of the containing project - must exist

        Returns:
            agent (db.Agents): The database agent object
        """

        project = self.project_controller.get(name=project_name)
        agent_query = db.Agents.query.filter(
            db.Agents.id == id,
            db.Agents.project_id == project.id,
            db.Agents.company_id == ctx.company_id,
            db.Agents.deleted_at == null(),
        )
        if ctx.enforce_user_id:
            agent_query = agent_query.filter(db.Agents.user_id == ctx.user_id)
        return agent_query.first()

    def get_agents(self, project_name: str) -> List[dict]:
        """
        Gets all agents in a project.

        Parameters:
            project_name (str): The name of the containing project - must exist

        Returns:
            all-agents (List[db.Agents]): List of database agent object
        """

        all_agents = db.Agents.query.filter(db.Agents.company_id == ctx.company_id, db.Agents.deleted_at == null())
        if ctx.enforce_user_id:
            all_agents = all_agents.filter(db.Agents.user_id == ctx.user_id)

        if project_name is not None:
            project = self.project_controller.get(name=project_name)

            all_agents = all_agents.filter(db.Agents.project_id == project.id)

        return all_agents.all()

    def add_agent(
        self,
        name: str,
        project_name: str = None,
        model: dict = None,
        params: Dict[str, Any] = None,
    ) -> db.Agents:
        """
        Adds an agent to the database.

        Parameters:
            name (str): The name of the new agent
            project_name (str): The containing project
            model: Dict, parameters for the model to use
                - provider: The provider of the model (e.g., 'openai', 'google')
                - Other model-specific parameters like 'api_key', 'model_name', etc.

            params (Dict[str, str]): Parameters to use when running the agent
                data: Dict, data sources for an agent, keys:
                  - knowledge_bases: List of KBs to use
                  - tables: list of tables to use
                <provider>_api_key: API key for the provider (e.g., openai_api_key)

        Returns:
            agent (db.Agents): The created agent

        Raises:
            EntityExistsError: Agent with given name already exists, or model with given name does not exist.
        """
        if project_name is None:
            project_name = default_project
        project = self.project_controller.get(name=project_name)

        agent = self.get_agent(name, project_name)

        if agent is not None:
            raise EntityExistsError("Agent already exists", name)

        # No need to copy params since we're not preserving the original reference
        params = params or {}
        params["model"] = model

        # check agent params
        validate_pydantic_params(params, AgentParams, "agent")

        # check llm works
        llm_params = self.get_agent_llm_params(model)
        check_agent_llm(llm_params)

        # check data
        data = params.get("data", {})
        if data:
            check_agent_data(data)

        agent = db.Agents(
            name=name,
            project_id=project.id,
            company_id=ctx.company_id,
            user_id=ctx.user_id,
            user_class=ctx.user_class,
            params=params,
        )

        db.session.add(agent)
        db.session.commit()

        return agent

    def update_agent(
        self,
        agent_name: str,
        project_name: str = default_project,
        name: str = None,
        model: dict = None,
        params: Dict[str, Any] = None,
    ):
        """
        Updates an agent in the database.

        Parameters:
            agent_name (str): The name of the new agent, or existing agent to update
            project_name (str): The containing project
            name (str): The updated name of the agent
            model dict: model parameters
            params: (Dict[str, str]): Parameters to use when running the agent

        Returns:
            agent (db.Agents): The created or updated agent

        Raises:
            EntityExistsError: if agent with new name already exists
            EntityNotExistsError: if agent with name not found
        """

        existing_agent = self.get_agent(agent_name, project_name=project_name)
        if existing_agent is None:
            raise EntityNotExistsError(f"Agent with name not found: {agent_name}")
        existing_params = existing_agent.params or {}

        is_demo = (existing_agent.params or {}).get("is_demo", False)
        if is_demo:
            raise ValueError("It is forbidden to change properties of the demo object")

        if name is not None and name != agent_name:
            # Check to see if updated name already exists
            agent_with_new_name = self.get_agent(name, project_name=project_name)
            if agent_with_new_name is not None:
                raise EntityExistsError(f"Agent with updated name already exists: {name}")
            existing_agent.name = name

        params = params or {}

        if model:
            params["model"] = model

        if params:
            validate_pydantic_params(params, AgentParams, "agent")
        else:
            # do nothing
            return existing_agent

        if model:
            # check llm works
            llm_params = self.get_agent_llm_params(model)
            check_agent_llm(llm_params)

        data = params.get("data", {})
        if data:
            check_agent_data(data)

        # Merge params on update
        existing_params.update(params)
        # Remove None values entirely.
        params = {k: v for k, v in existing_params.items() if v is not None}
        existing_agent.params = params
        # Some versions of SQL Alchemy won't handle JSON updates correctly without this.
        # See: https://docs.sqlalchemy.org/en/20/orm/session_api.html#sqlalchemy.orm.attributes.flag_modified
        flag_modified(existing_agent, "params")
        db.session.commit()

        return existing_agent

    def delete_agent(self, agent_name: str, project_name: str = default_project):
        """
        Deletes an agent by name.

        Parameters:
            agent_name (str): The name of the agent to delete
            project_name (str): The name of the containing project

        Raises:
            ValueError: Agent does not exist.
        """

        agent = self.get_agent(agent_name, project_name)
        if agent is None:
            raise ValueError(f"Agent with name does not exist: {agent_name}")
        if isinstance(agent.params, dict) and agent.params.get("is_demo") is True:
            raise ValueError("Unable to delete demo object")

        agent.deleted_at = datetime.datetime.now()
        db.session.commit()

    def get_agent_llm_params(self, model_params):
        """
        Get agent LLM parameters by combining default config with user provided parameters.
        Uses the same pattern as knowledge bases get_model_params function.
        """

        combined_model_params = copy.deepcopy(config.get("default_llm", {}))

        if model_params:
            # If provider mismatches - don't use default values (same as knowledge bases)
            if "provider" in model_params and model_params["provider"] != combined_model_params.get("provider"):
                return model_params

            combined_model_params.update(model_params)

        # Remove use_default_llm flag if present
        combined_model_params.pop("use_default_llm", None)

        return combined_model_params

    def get_completion(
        self,
        agent: db.Agents,
        messages: list[Dict[str, str]],
        project_name: str = default_project,
        tools: list[Any] = None,
        stream: bool = False,
        params: dict | None = None,
    ) -> Union[Iterator[object], pd.DataFrame]:
        """
        Queries an agent to get a completion.

        Parameters:
            agent (db.Agents): Existing agent to get completion from
            messages (list[Dict[str, str]]): Chat history to send to the agent
            project_name (str): Project the agent belongs to (default mindsdb)
            tools (list[Any]): Tools to use while getting the completion
            stream (bool): Whether to stream the response
            params (dict | None): params to redefine agent params

        Returns:
            response (Union[Iterator[object], pd.DataFrame]): Completion as a DataFrame or iterator of completion chunks

        Raises:
            ValueError: Agent's model does not exist.
        """
        # Extract SQL context from params if present

        from .pydantic_ai_agent import PydanticAIAgent

        # Get agent parameters and combine with default LLM parameters at runtime
        llm_params = self.get_agent_llm_params(agent.params.get("model"))

        pydantic_agent = PydanticAIAgent(agent, llm_params=llm_params)

        if stream:
            return pydantic_agent.get_completion(messages, stream=True, params=params)
        else:
            return pydantic_agent.get_completion(messages, params=params)
