import datetime
from typing import Dict, Iterator, List, Any, Union, Tuple, Optional

from langchain_core.tools import BaseTool
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy import null
import pandas as pd

from mindsdb.interfaces.storage import db
from mindsdb.interfaces.storage.db import Predictor
from mindsdb.utilities.context import context as ctx
from mindsdb.interfaces.database.projects import ProjectController
from mindsdb.interfaces.model.functions import PredictorRecordNotFound
from mindsdb.interfaces.model.model_controller import ModelController
from mindsdb.interfaces.skills.skills_controller import SkillsController
from mindsdb.utilities.config import config
from mindsdb.utilities.exception import EntityExistsError, EntityNotExistsError

from .constants import ASSISTANT_COLUMN, SUPPORTED_PROVIDERS, PROVIDER_TO_MODELS
from .langchain_agent import get_llm_provider
from .crewai_pipeline import CrewAITextToSQLPipeline


default_project = config.get('default_project')


class AgentsController:
    '''Handles CRUD operations at the database level for Agents'''

    assistant_column = ASSISTANT_COLUMN

    def __init__(
        self,
        project_controller: ProjectController = None,
        skills_controller: SkillsController = None,
        model_controller: ModelController = None
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
        '''
        Checks if a model exists, and gets the provider of the model.

        The provider is either the provider of the model, or the provider given as an argument.

        Parameters:
            model_name (str): The name of the model
            provider (str): The provider to check

        Returns:
            model (dict): The model object
            provider (str): The provider of the model
        '''
        model = None

        try:
            model_name_no_version, model_version = Predictor.get_name_and_version(model_name)
            model = self.model_controller.get_model(model_name_no_version, version=model_version)
            provider = 'mindsdb' if model.get('provider') is None else model.get('provider')
        except PredictorRecordNotFound:
            if not provider:
                # If provider is not given, get it from the model name
                provider = get_llm_provider({"model_name": model_name})

            elif provider not in SUPPORTED_PROVIDERS and model_name not in PROVIDER_TO_MODELS.get(provider, []):
                raise ValueError(f'Model with name does not exist for provider {provider}: {model_name}')

        return model, provider

    def get_agent(self, agent_name: str, project_name: str = default_project) -> Optional[db.Agents]:
        '''
        Gets an agent by name.

        Parameters:
            agent_name (str): The name of the agent
            project_name (str): The name of the containing project - must exist

        Returns:
            agent (Optional[db.Agents]): The database agent object
        '''

        project = self.project_controller.get(name=project_name)
        agent = db.Agents.query.filter(
            db.Agents.name == agent_name,
            db.Agents.project_id == project.id,
            db.Agents.company_id == ctx.company_id,
            db.Agents.deleted_at == null()
        ).first()
        return agent

    def get_agent_by_id(self, id: int, project_name: str = default_project) -> db.Agents:
        '''
        Gets an agent by id.

        Parameters:
            id (int): The id of the agent
            project_name (str): The name of the containing project - must exist

        Returns:
            agent (db.Agents): The database agent object
        '''

        project = self.project_controller.get(name=project_name)
        agent = db.Agents.query.filter(
            db.Agents.id == id,
            db.Agents.project_id == project.id,
            db.Agents.company_id == ctx.company_id,
            db.Agents.deleted_at == null()
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

        all_agents = db.Agents.query.filter(
            db.Agents.company_id == ctx.company_id,
            db.Agents.deleted_at == null()
        )

        if project_name is not None:
            project = self.project_controller.get(name=project_name)

            all_agents = all_agents.filter(db.Agents.project_id == project.id)

        return all_agents.all()

    def add_agent(
            self,
            name: str,
            project_name: str,
            model_name: str,
            skills: List[Union[str, dict]],
            provider: str = None,
            params: Dict[str, str] = {}) -> db.Agents:
        '''
        Adds an agent to the database.

        Parameters:
            name (str): The name of the new agent
            project_name (str): The containing project
            model_name (str): The name of the existing ML model the agent will use
            skills (List[Union[str, dict]]): List of existing skill names to add to the new agent, or list of dicts
                 with one of keys is "name", and other is additional parameters for relationship agent<>skill
            provider (str): The provider of the model
            params (Dict[str, str]): Parameters to use when running the agent

        Returns:
            agent (db.Agents): The created agent

        Raises:
            ValueError: Agent with given name already exists, or skill/model with given name does not exist.
        '''
        if project_name is None:
            project_name = default_project
        project = self.project_controller.get(name=project_name)

        agent = self.get_agent(name, project_name)

        if agent is not None:
            raise ValueError(f'Agent with name already exists: {name}')

        _, provider = self.check_model_provider(model_name, provider)

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
                skill_name = parameters.pop('name')
            existing_skill = self.skills_controller.get_skill(skill_name, project_name)
            if existing_skill is None:
                db.session.rollback()
                raise ValueError(f'Skill with name does not exist: {skill_name}')
            association = db.AgentSkillsAssociation(
                parameters=parameters,
                agent=agent,
                skill=existing_skill
            )
            db.session.add(association)

        db.session.add(agent)
        db.session.commit()

        return agent

    def update_agent(
            self,
            agent_name: str,
            project_name: str = default_project,
            name: str = None,
            model_name: str = None,
            skills_to_add: List[Union[str, dict]] = None,
            skills_to_remove: List[str] = None,
            skills_to_rewrite: List[Union[str, dict]] = None,
            provider: str = None,
            params: Dict[str, str] = None):
        '''
        Updates an agent in the database.

        Parameters:
            agent_name (str): The name of the new agent, or existing agent to update
            project_name (str): The containing project
            name (str): The updated name of the agent
            model_name (str): The name of the existing ML model the agent will use
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
        '''

        skills_to_add = skills_to_add or []
        skills_to_remove = skills_to_remove or []
        skills_to_rewrite = skills_to_rewrite or []

        if len(skills_to_rewrite) > 0 and (len(skills_to_remove) > 0 or len(skills_to_add) > 0):
            raise ValueError(
                "'skills_to_rewrite' and 'skills_to_add' (or 'skills_to_remove') cannot be used at the same time"
            )

        existing_agent = self.get_agent(agent_name, project_name=project_name)
        if existing_agent is None:
            raise EntityNotExistsError(f'Agent with name not found: {agent_name}')
        is_demo = (existing_agent.params or {}).get('is_demo', False)
        if (
            is_demo and (
                (name is not None and name != agent_name)
                or (model_name is not None and existing_agent.model_name != model_name)
                or (provider is not None and existing_agent.provider != provider)
                or (isinstance(params, dict) and len(params) > 0 and 'prompt_template' not in params)
            )
        ):
            raise ValueError("It is forbidden to change properties of the demo object")

        if name is not None and name != agent_name:
            # Check to see if updated name already exists
            agent_with_new_name = self.get_agent(name, project_name=project_name)
            if agent_with_new_name is not None:
                raise EntityExistsError(f'Agent with updated name already exists: {name}')
            existing_agent.name = name

        if model_name or provider:
            # check model and provider
            model, provider = self.check_model_provider(model_name, provider)
            # Update model and provider
            existing_agent.model_name = model_name
            existing_agent.provider = provider

        # check that all skills exist
        skill_name_to_record_map = {}
        for skill_meta in (skills_to_add + skills_to_remove + skills_to_rewrite):
            skill_name = skill_meta['name'] if isinstance(skill_meta, dict) else skill_meta
            if skill_name not in skill_name_to_record_map:
                skill_record = self.skills_controller.get_skill(skill_name, project_name)
                if skill_record is None:
                    raise EntityNotExistsError(f'Skill with name does not exist: {skill_name}')
                skill_name_to_record_map[skill_name] = skill_record

        if len(skills_to_add) > 0 or len(skills_to_remove) > 0:
            skills_to_add = [{'name': x} if isinstance(x, str) else x for x in skills_to_add]
            skills_to_add_names = [x['name'] for x in skills_to_add]

            # there are no intersection between lists
            if not set(skills_to_add_names).isdisjoint(set(skills_to_remove)):
                raise ValueError('Conflict between skills to add and skills to remove.')

            existing_agent_skills_names = [rel.skill.name for rel in existing_agent.skills_relationships]

            # remove skills
            for skill_name in skills_to_remove:
                for rel in existing_agent.skills_relationships:
                    if rel.skill.name == skill_name:
                        db.session.delete(rel)

            # add skills
            for skill_name in (set(skills_to_add_names) - set(existing_agent_skills_names)):
                skill_parameters = next(x for x in skills_to_add if x['name'] == skill_name).copy()
                del skill_parameters['name']
                association = db.AgentSkillsAssociation(
                    parameters=skill_parameters,
                    agent=existing_agent,
                    skill=skill_name_to_record_map[skill_name]
                )
                db.session.add(association)
        elif len(skills_to_rewrite) > 0:
            skill_name_to_parameters = {x['name']: {
                k: v for k, v in x.items() if k != 'name'
            } for x in skills_to_rewrite}
            existing_skill_names = set()
            for rel in existing_agent.skills_relationships:
                if rel.skill.name not in skill_name_to_parameters:
                    db.session.delete(rel)
                else:
                    existing_skill_names.add(rel.skill.name)
                    rel.parameters = skill_name_to_parameters[rel.skill.name]
                    flag_modified(rel, 'parameters')
            for new_skill_name in (set(skill_name_to_parameters) - existing_skill_names):
                association = db.AgentSkillsAssociation(
                    parameters=skill_name_to_parameters[new_skill_name],
                    agent=existing_agent,
                    skill=skill_name_to_record_map[new_skill_name]
                )
                db.session.add(association)

        if params is not None:
            # Merge params on update
            existing_params = existing_agent.params or {}
            existing_params.update(params)
            # Remove None values entirely.
            params = {k: v for k, v in existing_params.items() if v is not None}
            existing_agent.params = params
            # Some versions of SQL Alchemy won't handle JSON updates correctly without this.
            # See: https://docs.sqlalchemy.org/en/20/orm/session_api.html#sqlalchemy.orm.attributes.flag_modified
            flag_modified(existing_agent, 'params')
        db.session.commit()

        return existing_agent

    def delete_agent(self, agent_name: str, project_name: str = default_project):
        '''
        Deletes an agent by name.

        Parameters:
            agent_name (str): The name of the agent to delete
            project_name (str): The name of the containing project

        Raises:
            ValueError: Agent does not exist.
        '''

        agent = self.get_agent(agent_name, project_name)
        if agent is None:
            raise ValueError(f'Agent with name does not exist: {agent_name}')
        if isinstance(agent.params, dict) and agent.params.get('is_demo') is True:
            raise ValueError('Unable to delete demo object')
        agent.deleted_at = datetime.datetime.now()
        db.session.commit()

    def create_crewai_agents(
            self,
            name: str,
            project_name: str,
            model_name: str,
            tables: List[str] = None,
            knowledge_bases: List[str] = None,
            provider: str = 'openai',
            params: Dict[str, str] = {}):
        '''
        Creates a CrewAI agent pipeline.

        Parameters:
            name (str): The name of the new CrewAI agent group
            project_name (str): The containing project
            model_name (str): The name of the model the agent will use
            tables (List[str]): List of tables to use in format 'database.table'
            knowledge_bases (List[str]): List of knowledge bases to use
            provider (str): The provider of the model (only 'openai' supported)
            params (Dict[str, str]): Parameters to use when running the agent

        Returns:
            agent (db.Agents): The created agent

        Raises:
            ValueError: Agent with given name already exists, or provider is not supported
        '''
        if project_name is None:
            project_name = default_project

        project = self.project_controller.get(name=project_name)
        agent = self.get_agent(name, project_name)

        if agent is not None:
            raise ValueError(f'Agent with name already exists: {name}')

        if provider.lower() != 'openai':
            raise ValueError(f'Provider {provider} is not supported for CrewAI agents. Only "openai" is supported.')

        # Set default parameters if not provided
        if 'verbose' not in params:
            params['verbose'] = True
        if 'max_tokens' not in params:
            params['max_tokens'] = 2000

        # Store CrewAI type in params
        params['agent_type'] = 'crewai'

        # Store tables and knowledge bases in params
        params['tables'] = tables or []
        params['knowledge_bases'] = knowledge_bases or []
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

    def get_completion_crewai(
            self,
            agent: db.Agents,
            user_query: str,
            project_name: str = default_project) -> pd.DataFrame:
        """
        Processes a query using the CrewAI text-to-SQL pipeline.

        Parameters:
            agent (db.Agents): Existing CrewAI agent to get completion from
            user_query (str): The natural language query from the user
            project_name (str): Project the agent belongs to (default mindsdb)

        Returns:
            response (pd.DataFrame): The processed query result formatted as a DataFrame

        Raises:
            ValueError: If the agent is not a CrewAI agent
        """
        if not agent.params.get('agent_type') == 'crewai':
            raise ValueError(f'Agent {agent.name} is not a CrewAI agent')

        # Check for API key in agent parameters using both common parameter names
        api_key = agent.params.get('api_key')

        # If not in agent params, check configuration
        if not api_key:
            api_key = config.get('openai', {}).get('api_key')

        # Convert the user_query to a proper format if it's in JSON format
        import json
        try:
            # Try to parse as JSON in case the query comes in the format:
            # '{"role": "user", "content": "What are the available databases"}'
            query_data = json.loads(user_query)
            if isinstance(query_data, dict) and 'content' in query_data:
                user_query = query_data['content']
        except (json.JSONDecodeError, TypeError):
            # If not JSON, just use the raw query
            pass

        # Create the CrewAI pipeline
        pipeline = CrewAITextToSQLPipeline(
            tables=agent.params.get('tables', []),
            knowledge_bases=agent.params.get('knowledge_bases', []),
            model=agent.model_name,
            api_key=api_key,
            verbose=agent.params.get('verbose', True),
            max_tokens=agent.params.get('max_tokens', 2000)
        )

        # Process the query
        result = pipeline.process_query(user_query)

        # Convert the result to a DataFrame for compatibility with MindsDB SQL interface
        import pandas as pd

        # Format the result data as a DataFrame
        # Use the column names expected by MindsDB - typically 'output' for the result
        df = pd.DataFrame({
            'output': [result['result']],
        })

        return df

    def get_completion(
            self,
            agent: db.Agents,
            messages: List[Dict[str, str]],
            project_name: str = default_project,
            tools: List[BaseTool] = None,
            stream: bool = False) -> Union[Iterator[object], pd.DataFrame, Dict[str, Any]]:
        """
        Queries an agent to get a completion.

        Parameters:
            agent (db.Agents): Existing agent to get completion from
            messages (List[Dict[str, str]]): Chat history to send to the agent
            project_name (str): Project the agent belongs to (default mindsdb)
            tools (List[BaseTool]): Tools to use while getting the completion
            stream (bool): Whether to stream the response

        Returns:
            response (Union[Iterator[object], pd.DataFrame, Dict[str, Any]]):  Completion as DataFrame, iterator of chunks, or dict for CrewAI

        Raises:
            ValueError: Agent's model does not exist.
        """
        # Check if this is a CrewAI agent
        if agent.params and agent.params.get('agent_type') == 'crewai':
            # For CrewAI agents, extract the user query
            user_query = None

            # First check if the last message is a user message
            if messages and len(messages) > 0:
                last_message = messages[-1]
                if isinstance(last_message, dict):
                    # Try several possible field names that might contain the query
                    for field in ['content', 'question', 'text', 'input']:
                        if field in last_message:
                            user_query = last_message[field]
                            break
                    # If we still don't have a query and there's a role field
                    if user_query is None and 'role' in last_message and last_message['role'] == 'user':
                        # Try to find any string value to use as query
                        for key, value in last_message.items():
                            if isinstance(value, str) and key != 'role':
                                user_query = value
                                break

            # If we couldn't extract a query from messages but have a DataFrame in messages
            if user_query is None and isinstance(messages, pd.DataFrame):
                # Try to find the query in any common column names
                for col in ['question', 'content', 'text', 'input', 'query']:
                    if col in messages.columns:
                        # Get the first non-empty value in the column
                        values = messages[col].dropna()
                        if not values.empty:
                            user_query = values.iloc[0]
                            break

            # If we still don't have a query, try to extract from any string in messages
            if user_query is None:
                # Just use the string representation as a last resort
                user_query = str(messages)

            # Process the query with CrewAI
            return self.get_completion_crewai(agent, user_query, project_name)

        # Handle regular LangChain agents
        if stream:
            return self._get_completion_stream(
                agent,
                messages,
                project_name=project_name,
                tools=tools
            )
        from .langchain_agent import LangchainAgent

        model, provider = self.check_model_provider(agent.model_name, agent.provider)
        # update old agents
        if agent.provider is None and provider is not None:
            agent.provider = provider
            db.session.commit()

        lang_agent = LangchainAgent(agent, model)
        return lang_agent.get_completion(messages)

    def _get_completion_stream(
            self,
            agent: db.Agents,
            messages: List[Dict[str, str]],
            project_name: str = default_project,
            tools: List[BaseTool] = None) -> Iterator[object]:
        '''
        Queries an agent to get a stream of completion chunks.

        Parameters:
            agent (db.Agents): Existing agent to get completion from
            messages (List[Dict[str, str]]): Chat history to send to the agent
            trace_id (str): ID of Langfuse trace to use
            observation_id (str): ID of parent Langfuse observation to use
            project_name (str): Project the agent belongs to (default mindsdb)
            tools (List[BaseTool]): Tools to use while getting the completion

        Returns:
            chunks (Iterator[object]): Completion chunks as an iterator

        Raises:
            ValueError: Agent's model does not exist.
        '''
        # For circular dependency.
        from .langchain_agent import LangchainAgent

        model, provider = self.check_model_provider(agent.model_name, agent.provider)

        # update old agents
        if agent.provider is None and provider is not None:
            agent.provider = provider
            db.session.commit()

        lang_agent = LangchainAgent(agent, model=model)
        return lang_agent.get_completion(messages, stream=True)
