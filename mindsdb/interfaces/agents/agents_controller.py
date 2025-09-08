import datetime
from typing import Dict, Iterator, List, Union, Tuple, Optional, Any
import copy

from langchain_core.tools import BaseTool
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy import null
import pandas as pd

from mindsdb.interfaces.storage import db
from mindsdb.interfaces.storage.db import Predictor
from mindsdb.utilities.context import context as ctx
from mindsdb.interfaces.data_catalog.data_catalog_loader import DataCatalogLoader
from mindsdb.interfaces.database.projects import ProjectController
from mindsdb.interfaces.model.functions import PredictorRecordNotFound
from mindsdb.interfaces.model.model_controller import ModelController
from mindsdb.interfaces.skills.skills_controller import SkillsController
from mindsdb.utilities.config import config
from mindsdb.utilities import log

from mindsdb.utilities.exception import EntityExistsError, EntityNotExistsError

from .constants import ASSISTANT_COLUMN, SUPPORTED_PROVIDERS, PROVIDER_TO_MODELS
from .langchain_agent import get_llm_provider

logger = log.getLogger(__name__)

default_project = config.get("default_project")


class AgentsController:
    """Handles CRUD operations at the database level for Agents"""

    assistant_column = ASSISTANT_COLUMN

    def __init__(
        self,
        project_controller: ProjectController = None,
        skills_controller: SkillsController = None,
        model_controller: ModelController = None,
    ):
        if project_controller is None:
            project_controller = ProjectController()
        if skills_controller is None:
            skills_controller = SkillsController()
        if model_controller is None:
            model_controller = ModelController()
        self.project_controller = project_controller
        self.skills_controller = skills_controller
        self.model_controller = model_controller

    def check_model_provider(self, model_name: str, provider: str = None) -> Tuple[dict, str]:
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

    def _create_default_sql_skill(
        self,
        name,
        project_name,
        include_tables: List[str] = None,
        include_knowledge_bases: List[str] = None,
    ):
        # Create a default SQL skill
        skill_name = f"{name}_sql_skill"
        skill_params = {
            "type": "sql",
            "description": f"Auto-generated SQL skill for agent {name}",
        }

        # Add restrictions provided
        if include_tables:
            skill_params["include_tables"] = include_tables
        if include_knowledge_bases:
            skill_params["include_knowledge_bases"] = include_knowledge_bases

        try:
            # Check if skill already exists
            existing_skill = self.skills_controller.get_skill(skill_name, project_name)
            if existing_skill is None:
                # Create the skill
                skill_type = skill_params.pop("type")
                self.skills_controller.add_skill(
                    name=skill_name, project_name=project_name, type=skill_type, params=skill_params
                )
            else:
                # Update the skill if parameters have changed
                params_changed = False

                # Check if skill parameters need to be updated
                for param_key, param_value in skill_params.items():
                    if existing_skill.params.get(param_key) != param_value:
                        existing_skill.params[param_key] = param_value
                        params_changed = True

                # Update the skill if needed
                if params_changed:
                    flag_modified(existing_skill, "params")
                    db.session.commit()

        except Exception as e:
            raise ValueError(f"Failed to auto-create or update SQL skill: {str(e)}")

        return skill_name

    def add_agent(
        self,
        name: str,
        project_name: str = None,
        model_name: Union[str, dict] = None,
        skills: List[Union[str, dict]] = None,
        provider: str = None,
        params: Dict[str, Any] = None,
    ) -> db.Agents:
        """
        Adds an agent to the database.

        Parameters:
            name (str): The name of the new agent
            project_name (str): The containing project
            model_name (str | dict): The name of the existing ML model the agent will use
            skills (List[Union[str, dict]]): List of existing skill names to add to the new agent, or list of dicts
                 with one of keys is "name", and other is additional parameters for relationship agent<>skill
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
                database: The database to use for text2sql skills (default is 'mindsdb')
                knowledge_base_database: The database to use for knowledge base queries (default is 'mindsdb')
                include_tables: List of tables to include for text2sql skills
                ignore_tables: List of tables to ignore for text2sql skills
                include_knowledge_bases: List of knowledge bases to include for text2sql skills
                ignore_knowledge_bases: List of knowledge bases to ignore for text2sql skills

        Returns:
            agent (db.Agents): The created agent

        Raises:
            EntityExistsError: Agent with given name already exists, or skill/model with given name does not exist.
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

        # Auto-create SQL skill if no skills are provided but include_tables or include_knowledge_bases params are provided
        if not skills and (include_tables or include_knowledge_bases):
            skill = self._create_default_sql_skill(
                name,
                project_name,
                include_tables=include_tables,
                include_knowledge_bases=include_knowledge_bases,
            )
            skills = [skill]

        agent = db.Agents(
            name=name,
            project_id=project.id,
            company_id=ctx.company_id,
            user_class=ctx.user_class,
            model_name=model_name,
            provider=provider,
            params=params,
        )

        for skill in skills:
            if isinstance(skill, str):
                skill_name = skill
                parameters = {}
            else:
                parameters = skill.copy()
                skill_name = parameters.pop("name")

            existing_skill = self.skills_controller.get_skill(skill_name, project_name)
            if existing_skill is None:
                db.session.rollback()
                raise ValueError(f"Skill with name does not exist: {skill_name}")

            # Run Data Catalog loader if enabled.
            if include_tables:
                self._run_data_catalog_loader_for_table_entries(include_tables, project_name, skill=existing_skill)
            else:
                self._run_data_catalog_loader_for_skill(existing_skill, project_name, tables=parameters.get("tables"))

            if existing_skill.type == "sql":
                # Add table restrictions if this is a text2sql skill
                if include_tables:
                    parameters["tables"] = include_tables

                # Add knowledge base parameters to both the skill and the association parameters
                if include_knowledge_bases:
                    parameters["include_knowledge_bases"] = include_knowledge_bases
                    if "include_knowledge_bases" not in existing_skill.params:
                        existing_skill.params["include_knowledge_bases"] = include_knowledge_bases
                        flag_modified(existing_skill, "params")

            association = db.AgentSkillsAssociation(parameters=parameters, agent=agent, skill=existing_skill)
            db.session.add(association)

        db.session.add(agent)
        db.session.commit()

        return agent

    def update_agent(
        self,
        agent_name: str,
        project_name: str = default_project,
        name: str = None,
        model_name: Union[str, dict] = None,
        skills_to_add: List[Union[str, dict]] = None,
        skills_to_remove: List[str] = None,
        skills_to_rewrite: List[Union[str, dict]] = None,
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
            skills_to_add (List[Union[str, dict]]): List of skill names to add to the agent, or list of dicts
                 with one of keys is "name", and other is additional parameters for relationship agent<>skill
            skills_to_remove (List[str]): List of skill names to remove from the agent
            skills_to_rewrite (List[Union[str, dict]]): new list of skills for the agent
            provider (str): The provider of the model
            params: (Dict[str, str]): Parameters to use when running the agent

        Returns:
            agent (db.Agents): The created or updated agent

        Raises:
            EntityExistsError: if agent with new name already exists
            EntityNotExistsError: if agent with name or skill not found
            ValueError: if conflict in skills list
        """

        skills_to_add = skills_to_add or []
        skills_to_remove = skills_to_remove or []
        skills_to_rewrite = skills_to_rewrite or []

        if len(skills_to_rewrite) > 0 and (len(skills_to_remove) > 0 or len(skills_to_add) > 0):
            raise ValueError(
                "'skills_to_rewrite' and 'skills_to_add' (or 'skills_to_remove') cannot be used at the same time"
            )

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
            if not name.islower():
                raise ValueError(f"The name must be in lower case: {name}")
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

        if "data" in params:
            if len(skills_to_add) > 0 or len(skills_to_remove) > 0:
                raise ValueError(
                    "'data' parameter cannot be used with 'skills_to_remove' or 'skills_to_add' parameters"
                )

            include_knowledge_bases = params["data"].get("knowledge_bases")
            include_tables = params["data"].get("tables")

            skill = self._create_default_sql_skill(
                agent_name,
                project_name,
                include_tables=include_tables,
                include_knowledge_bases=include_knowledge_bases,
            )
            skills_to_rewrite = [{"name": skill}]

        # check that all skills exist
        skill_name_to_record_map = {}
        for skill_meta in skills_to_add + skills_to_remove + skills_to_rewrite:
            skill_name = skill_meta["name"] if isinstance(skill_meta, dict) else skill_meta
            if skill_name not in skill_name_to_record_map:
                skill_record = self.skills_controller.get_skill(skill_name, project_name)
                if skill_record is None:
                    raise EntityNotExistsError(f"Skill with name does not exist: {skill_name}")
                skill_name_to_record_map[skill_name] = skill_record

        if len(skills_to_add) > 0 or len(skills_to_remove) > 0:
            skills_to_add = [{"name": x} if isinstance(x, str) else x for x in skills_to_add]
            skills_to_add_names = [x["name"] for x in skills_to_add]

            # there are no intersection between lists
            if not set(skills_to_add_names).isdisjoint(set(skills_to_remove)):
                raise ValueError("Conflict between skills to add and skills to remove.")

            existing_agent_skills_names = [rel.skill.name for rel in existing_agent.skills_relationships]

            # remove skills
            for skill_name in skills_to_remove:
                for rel in existing_agent.skills_relationships:
                    if rel.skill.name == skill_name:
                        db.session.delete(rel)

            # add skills
            for skill_name in set(skills_to_add_names) - set(existing_agent_skills_names):
                # Run Data Catalog loader if enabled for the new skill
                self._run_data_catalog_loader_for_skill(
                    skill_name,
                    project_name,
                    tables=next((x for x in skills_to_add if x["name"] == skill_name), {}).get("tables"),
                )

                skill_parameters = next(x for x in skills_to_add if x["name"] == skill_name).copy()
                del skill_parameters["name"]
                association = db.AgentSkillsAssociation(
                    parameters=skill_parameters, agent=existing_agent, skill=skill_name_to_record_map[skill_name]
                )
                db.session.add(association)

        elif len(skills_to_rewrite) > 0:
            skill_name_to_parameters = {
                x["name"]: {k: v for k, v in x.items() if k != "name"} for x in skills_to_rewrite
            }
            existing_skill_names = set()
            for rel in existing_agent.skills_relationships:
                if rel.skill.name not in skill_name_to_parameters:
                    db.session.delete(rel)
                else:
                    existing_skill_names.add(rel.skill.name)
                    skill_parameters = skill_name_to_parameters[rel.skill.name]

                    # Run Data Catalog loader if enabled for the updated skill
                    self._run_data_catalog_loader_for_skill(
                        rel.skill.name, project_name, tables=skill_parameters.get("tables")
                    )

                    rel.parameters = skill_parameters
                    flag_modified(rel, "parameters")
            for new_skill_name in set(skill_name_to_parameters) - existing_skill_names:
                # Run Data Catalog loader if enabled for the new skill
                self._run_data_catalog_loader_for_skill(
                    new_skill_name,
                    project_name,
                    tables=skill_name_to_parameters[new_skill_name].get("tables"),
                )

                association = db.AgentSkillsAssociation(
                    parameters=skill_name_to_parameters[new_skill_name],
                    agent=existing_agent,
                    skill=skill_name_to_record_map[new_skill_name],
                )
                db.session.add(association)

        if params is not None:
            if params.get("data", {}).get("tables"):
                new_table_entries = set(params["data"]["tables"]) - set(
                    existing_params.get("data", {}).get("tables", [])
                )
                if new_table_entries:
                    # Run Data Catalog loader for new table entries if enabled.
                    self._run_data_catalog_loader_for_table_entries(new_table_entries, project_name)

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

    def _run_data_catalog_loader_for_skill(
        self,
        skill: Union[str, db.Skills],
        project_name: str,
        tables: List[str] = None,
    ):
        """
        Runs Data Catalog loader for a skill if enabled in the config.
        This is used to load metadata for SQL skills when they are added or updated.
        """
        if not config.get("data_catalog", {}).get("enabled", False):
            return

        skill = skill if isinstance(skill, db.Skills) else self.skills_controller.get_skill(skill, project_name)
        if skill.type == "sql":
            if "database" in skill.params:
                valid_table_names = skill.params.get("tables") if skill.params.get("tables") else tables
                data_catalog_loader = DataCatalogLoader(
                    database_name=skill.params["database"], table_names=valid_table_names
                )
                data_catalog_loader.load_metadata()
            else:
                raise ValueError(
                    "Data Catalog loading is enabled, but the provided parameters for the new skills are insufficient to load metadata. "
                )

    def _run_data_catalog_loader_for_table_entries(
        self,
        table_entries: List[str],
        project_name: str,
        skill: Union[str, db.Skills] = None,
    ):
        """
        Runs Data Catalog loader for a list of table entries if enabled in the config.
        This is used to load metadata for SQL skills when they are added or updated.
        """
        if not config.get("data_catalog", {}).get("enabled", False):
            return

        skill = skill if isinstance(skill, db.Skills) else self.skills_controller.get_skill(skill, project_name)
        if not skill or skill.type == "sql":
            database_table_map = {}
            for table_entry in table_entries:
                parts = table_entry.split(".", 1)

                # Ensure the table name is in 'database.table' format.
                if len(parts) != 2:
                    logger.warning(
                        f"Invalid table name format: {table_entry}. Expected 'database.table' format."
                        "Metadata will not be loaded for this entry."
                    )
                    continue

                database, table = parts[0], parts[1]

                # Wildcards in database names are not supported at the moment by data catalog loader.
                if "*" in database:
                    logger.warning(
                        f"Invalid database name format: {database}. Wildcards are not supported."
                        "Metadata will not be loaded for this entry."
                    )
                    continue

                # Wildcards in table names are supported either.
                # However, the table name itself can be a wildcard representing all tables.
                if table == "*":
                    table = None
                elif "*" in table:
                    logger.warning(
                        f"Invalid table name format: {table}. Wildcards are not supported."
                        "Metadata will not be loaded for this entry."
                    )
                    continue

                database_table_map[database] = database_table_map.get(database, []) + [table]

            for database_name, table_names in database_table_map.items():
                data_catalog_loader = DataCatalogLoader(database_name=database_name, table_names=table_names)
                data_catalog_loader.load_metadata()

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
        Similar to how knowledge bases handle default parameters.
        """
        combined_model_params = copy.deepcopy(config.get("default_llm", {}))

        if "model" in agent_params:
            model_params = agent_params["model"]
        else:
            # params for LLM can be arbitrary
            model_params = agent_params

        if model_params:
            combined_model_params.update(model_params)

        return combined_model_params

    def get_completion(
        self,
        agent: db.Agents,
        messages: list[Dict[str, str]],
        project_name: str = default_project,
        tools: list[BaseTool] = None,
        stream: bool = False,
        params: dict | None = None,
    ) -> Union[Iterator[object], pd.DataFrame]:
        """
        Queries an agent to get a completion.

        Parameters:
            agent (db.Agents): Existing agent to get completion from
            messages (list[Dict[str, str]]): Chat history to send to the agent
            project_name (str): Project the agent belongs to (default mindsdb)
            tools (list[BaseTool]): Tools to use while getting the completion
            stream (bool): Whether to stream the response
            params (dict | None): params to redefine agent params

        Returns:
            response (Union[Iterator[object], pd.DataFrame]): Completion as a DataFrame or iterator of completion chunks

        Raises:
            ValueError: Agent's model does not exist.
        """
        if stream:
            return self._get_completion_stream(agent, messages, project_name=project_name, tools=tools, params=params)
        from .langchain_agent import LangchainAgent

        model, provider = self.check_model_provider(agent.model_name, agent.provider)
        # update old agents
        if agent.provider is None and provider is not None:
            agent.provider = provider
            db.session.commit()

        # Get agent parameters and combine with default LLM parameters at runtime
        llm_params = self.get_agent_llm_params(agent.params)

        lang_agent = LangchainAgent(agent, model, llm_params=llm_params)
        return lang_agent.get_completion(messages, params=params)

    def _get_completion_stream(
        self,
        agent: db.Agents,
        messages: list[Dict[str, str]],
        project_name: str = default_project,
        tools: list[BaseTool] = None,
        params: dict | None = None,
    ) -> Iterator[object]:
        """
        Queries an agent to get a stream of completion chunks.

        Parameters:
            agent (db.Agents): Existing agent to get completion from
            messages (list[Dict[str, str]]): Chat history to send to the agent
            trace_id (str): ID of Langfuse trace to use
            observation_id (str): ID of parent Langfuse observation to use
            project_name (str): Project the agent belongs to (default mindsdb)
            tools (list[BaseTool]): Tools to use while getting the completion
            params (dict | None): params to redefine agent params

        Returns:
            chunks (Iterator[object]): Completion chunks as an iterator

        Raises:
            ValueError: Agent's model does not exist.
        """
        # For circular dependency.
        from .langchain_agent import LangchainAgent

        model, provider = self.check_model_provider(agent.model_name, agent.provider)

        # update old agents
        if agent.provider is None and provider is not None:
            agent.provider = provider
            db.session.commit()

        # Get agent parameters and combine with default LLM parameters at runtime
        llm_params = self.get_agent_llm_params(agent.params)

        lang_agent = LangchainAgent(agent, model=model, llm_params=llm_params)
        return lang_agent.get_completion(messages, stream=True, params=params)
