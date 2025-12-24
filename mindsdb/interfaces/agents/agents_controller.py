import datetime
from typing import Dict, Iterator, List, Union, Tuple, Optional, Any
import copy

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
from mindsdb.utilities import log

from mindsdb.utilities.exception import EntityExistsError, EntityNotExistsError

from .utils.constants import ASSISTANT_COLUMN, SUPPORTED_PROVIDERS, PROVIDER_TO_MODELS
from .utils.pydantic_ai_model_factory import get_llm_provider
import re

logger = log.getLogger(__name__)

default_project = config.get("default_project")


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
        agent = db.Agents.query.filter(
            db.Agents.name == agent_name,
            db.Agents.project_id == project.id,
            db.Agents.company_id == ctx.company_id,
            db.Agents.deleted_at == null(),
        ).first()
        return agent

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
        agent = db.Agents.query.filter(
            db.Agents.id == id,
            db.Agents.project_id == project.id,
            db.Agents.company_id == ctx.company_id,
            db.Agents.deleted_at == null(),
        ).first()
        return agent

    def get_agents(self, project_name: str) -> List[dict]:
        """
        Gets all agents in a project.

        Parameters:
            project_name (str): The name of the containing project - must exist

        Returns:
            all-agents (List[db.Agents]): List of database agent object
        """

        all_agents = db.Agents.query.filter(db.Agents.company_id == ctx.company_id, db.Agents.deleted_at == null())

        if project_name is not None:
            project = self.project_controller.get(name=project_name)

            all_agents = all_agents.filter(db.Agents.project_id == project.id)

        return all_agents.all()

    def add_agent(
        self,
        name: str,
        project_name: str = None,
        model_name: Union[str, dict] = None,
        provider: str = None,
        params: Dict[str, Any] = None,
    ) -> db.Agents:
        """
        Adds an agent to the database.

        Parameters:
            name (str): The name of the new agent
            project_name (str): The containing project
            model_name (str | dict): The name of the existing ML model the agent will use
            provider (str): The provider of the model
            params (Dict[str, str]): Parameters to use when running the agent
                data: Dict, data sources for an agent, keys:
                  - knowledge_bases: List of KBs to use
                  - tables: list of tables to use
                model: Dict, parameters for the model to use
                  - provider: The provider of the model (e.g., 'openai', 'google')
                  - Other model-specific parameters like 'api_key', 'model_name', etc.
                <provider>_api_key: API key for the provider (e.g., openai_api_key)

                # Deprecated parameters:
                database: The database to use (default is 'mindsdb')
                knowledge_base_database: The database to use for knowledge base queries (default is 'mindsdb')
                include_tables: List of tables to include
                ignore_tables: List of tables to ignore
                include_knowledge_bases: List of knowledge bases to include
                ignore_knowledge_bases: List of knowledge bases to ignore

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

        if isinstance(model_name, dict):
            # move into params
            params["model"] = model_name
            model_name = None

        if model_name is not None:
            _, provider = self.check_model_provider(model_name, provider)

        if model_name is None:
            logger.warning("'model_name' param is not provided. Using default global llm model at runtime.")

        # If model_name is not provided, we use default global llm model at runtime
        # Default parameters will be applied at runtime via get_agent_llm_params
        # This allows global default updates to apply to all agents immediately

        # Extract API key if provided in the format <provider>_api_key
        if provider is not None:
            provider_api_key_param = f"{provider.lower()}_api_key"
            if provider_api_key_param in params:
                # Keep the API key in params for the agent to use
                # It will be picked up by get_api_key() in handler_utils.py
                pass

        # Handle generic api_key parameter if provided
        if "api_key" in params:
            # Keep the generic API key in params for the agent to use
            # It will be picked up by get_api_key() in handler_utils.py
            pass

        depreciated_params = [
            "database",
            "knowledge_base_database",
            "include_tables",
            "ignore_tables",
            "include_knowledge_bases",
            "ignore_knowledge_bases",
        ]
        if any(param in params for param in depreciated_params):
            raise ValueError(
                f"Parameters {', '.join(depreciated_params)} are deprecated. "
                "Use 'data' parameter with 'tables' and 'knowledge_bases' keys instead."
            )

        include_tables = None
        include_knowledge_bases = None
        if "data" in params:
            include_knowledge_bases = params["data"].get("knowledge_bases")
            include_tables = params["data"].get("tables")

        # Convert string parameters to lists if needed
        if isinstance(include_tables, str):
            include_tables = [t.strip() for t in include_tables.split(",")]
        if isinstance(include_knowledge_bases, str):
            include_knowledge_bases = [kb.strip() for kb in include_knowledge_bases.split(",")]

        agent = db.Agents(
            name=name,
            project_id=project.id,
            company_id=ctx.company_id,
            user_class=ctx.user_class,
            model_name=model_name,
            provider=provider,
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
        model_name: Union[str, dict] = None,
        provider: str = None,
        params: Dict[str, str] = None,
    ):
        """
        Updates an agent in the database.

        Parameters:
            agent_name (str): The name of the new agent, or existing agent to update
            project_name (str): The containing project
            name (str): The updated name of the agent
            model_name (str | dict): The name of the existing ML model the agent will use
            provider (str): The provider of the model
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
        if is_demo and (
            (name is not None and name != agent_name)
            or (model_name is not None and existing_agent.model_name != model_name)
            or (provider is not None and existing_agent.provider != provider)
            or (isinstance(params, dict) and len(params) > 0 and "prompt_template" not in params)
        ):
            raise ValueError("It is forbidden to change properties of the demo object")

        if name is not None and name != agent_name:
            # Check to see if updated name already exists
            agent_with_new_name = self.get_agent(name, project_name=project_name)
            if agent_with_new_name is not None:
                raise EntityExistsError(f"Agent with updated name already exists: {name}")
            existing_agent.name = name

        if model_name or provider:
            if isinstance(model_name, dict):
                # move into params
                existing_params["model"] = model_name
                model_name = None

            # check model and provider
            model, provider = self.check_model_provider(model_name, provider)
            # Update model and provider
            existing_agent.model_name = model_name
            existing_agent.provider = provider

        if params is not None:
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

    def get_agent_llm_params(self, agent_params: dict):
        """
        Get agent LLM parameters by combining default config with user provided parameters.
        Uses the same pattern as knowledge bases get_model_params function.
        """
        # Import get_model_params pattern from knowledge_base (or implement inline for consistency)
        from mindsdb.utilities.config import config
        
        # Get default LLM config from system config
        default_llm_config = copy.deepcopy(config.get("default_llm", {}))
        
        # Get model params from agent params (same structure as knowledge bases)
        if "model" in agent_params:
            model_params = agent_params.get("model", {})
        else:
            # params for LLM can be arbitrary (backward compatibility)
            model_params = agent_params

        if model_params:
            if not isinstance(model_params, dict):
                raise ValueError("Model parameters must be passed as a JSON object")
            
            # If provider mismatches - don't use default values (same as knowledge bases)
            if "provider" in model_params and model_params["provider"] != default_llm_config.get("provider"):
                combined_model_params = model_params.copy()
            else:
                combined_model_params = copy.deepcopy(default_llm_config)
                combined_model_params.update(model_params)
        else:
            # No model params provided - use defaults from config
            combined_model_params = default_llm_config
        
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

        model, provider = self.check_model_provider(agent.model_name, agent.provider)
        # update old agents
        if agent.provider is None and provider is not None:
            agent.provider = provider
            db.session.commit()

        # Get agent parameters and combine with default LLM parameters at runtime
        llm_params = self.get_agent_llm_params(agent.params)

        pydantic_agent = PydanticAIAgent(agent, model, llm_params=llm_params)

        if stream:
            return pydantic_agent.get_completion(messages, stream=True, params=params)
        else:
            return pydantic_agent.get_completion(messages, params=params)
