from typing import Dict, List

from langchain_core.tools import BaseTool
from sqlalchemy.orm.attributes import flag_modified
import pandas as pd

from mindsdb.interfaces.model.functions import PredictorRecordNotFound
from mindsdb.interfaces.storage.db import Predictor
from mindsdb.interfaces.model.model_controller import ModelController
from mindsdb.interfaces.skills.skills_controller import SkillsController
from mindsdb.interfaces.storage import db
from mindsdb.interfaces.database.projects import ProjectController
from mindsdb.utilities.context import context as ctx


class AgentsController:
    '''Handles CRUD operations at the database level for Agents'''

    def __init__(
        self,
        datahub,
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
        self.datahub = datahub

    def get_agent(self, agent_name: str, project_name: str = 'mindsdb') -> db.Agents:
        '''
        Gets an agent by name.

        Parameters:
            agent_name (str): The name of the agent
            project_name (str): The name of the containing project - must exist

        Returns:
            agent (db.Agents): The database agent object
        '''

        project = self.project_controller.get(name=project_name)
        agent = db.Agents.query.filter(
            db.Agents.name == agent_name,
            db.Agents.project_id == project.id,
            db.Agents.company_id == ctx.company_id
        ).first()
        return agent

    def get_agent_by_id(self, id: int, project_name: str = 'mindsdb') -> db.Agents:
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
            db.Agents.company_id == ctx.company_id
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
            db.Agents.company_id == ctx.company_id
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
            skills: List[str],
            params: Dict[str, str] = {}) -> db.Agents:
        '''
        Adds an agent to the database.

        Parameters:
            name (str): The name of the new agent
            project_name (str): The containing project
            model_name (str): The name of the existing ML model the agent will use
            skills (List[str]): List of existing skill names to add to the new agent
            params (Dict[str, str]): Parameters to use when running the agent

        Returns:
            agent (db.Agents): The created agent

        Raises:
            ValueError: Agent with given name already exists, or skill/model with given name does not exist.
        '''
        if project_name is None:
            project_name = 'mindsdb'
        project = self.project_controller.get(name=project_name)

        agent = self.get_agent(name, project_name)

        if agent is not None:
            raise ValueError(f'Agent with name already exists: {name}')

        # Check if model exists.
        model_name_no_version, model_version = Predictor.get_name_and_version(model_name)
        try:
            self.model_controller.get_model(model_name_no_version, version=model_version, project_name=project_name)
        except PredictorRecordNotFound:
            raise ValueError(f'Model with name does not exist: {model_name}')

        agent = db.Agents(
            name=name,
            project_id=project.id,
            company_id=ctx.company_id,
            user_class=ctx.user_class,
            model_name=model_name,
            params=params,
        )
        skills_to_add = []
        # Check if given skills exist.
        for skill in skills:
            existing_skill = self.skills_controller.get_skill(skill, project_name)
            if existing_skill is None:
                raise ValueError(f'Skill with name does not exist: {skill}')
            skills_to_add.append(existing_skill)
        agent.skills = skills_to_add

        db.session.add(agent)
        db.session.commit()

        return agent

    def update_agent(
            self,
            agent_name: str,
            project_name: str = 'mindsdb',
            name: str = None,
            model_name: str = None,
            skills_to_add: List[str] = None,
            skills_to_remove: List[str] = None,
            params: Dict[str, str] = None):
        '''
        Updates an agent in the database.

        Parameters:
            agent_name (str): The name of the new agent, or existing agent to update
            project_name (str): The containing project
            name (str): The updated name of the agent
            model_name (str): The name of the existing ML model the agent will use
            skills_to_add (List[str]): List of skill names to add to the agent
            skills_to_remove (List[str]): List of skill names to remove from the agent
            params: (Dict[str, str]): Parameters to use when running the agent

        Returns:
            agent (db.Agents): The created or updated agent

        Raises:
            ValueError: Agent with name not found, agent with new name already exists, or model/skill does not exist.
        '''

        existing_agent = self.get_agent(agent_name, project_name=project_name)
        if existing_agent is None:
            raise ValueError(f'Agent with name not found: {agent_name}')

        if name is not None and name != agent_name:
            # Check to see if updated name already exists
            agent_with_new_name = self.get_agent(name, project_name=project_name)
            if agent_with_new_name is not None:
                raise ValueError(f'Agent with updated name already exists: {name}')

        # Check if model exists.
        if model_name is not None:
            model_name_no_version, model_version = Predictor.get_name_and_version(model_name)
            try:
                _ = self.model_controller.get_model(model_name_no_version, version=model_version, project_name=project_name)
                existing_agent.model_name = model_name
            except PredictorRecordNotFound:
                return ValueError(f'Model with name does not exist: {model_name}')

        # Check if given skills exist.
        new_skills = []
        for skill in skills_to_add:
            existing_skill = self.skills_controller.get_skill(skill, project_name)
            if existing_skill is None:
                raise ValueError(f'Skill with name does not exist: {skill}')
            new_skills.append(existing_skill)
        existing_agent.skills += new_skills

        removed_skills = []
        for skill in existing_agent.skills:
            if skill.name in skills_to_remove:
                removed_skills.append(skill)
        for skill_to_remove in removed_skills:
            existing_agent.skills.remove(skill_to_remove)

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

    def delete_agent(self, agent_name: str, project_name: str = 'mindsdb'):
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
        db.session.delete(agent)
        db.session.commit()

    def get_completion(
            self,
            agent: db.Agents,
            messages: List[Dict[str, str]],
            trace_id: str = None,
            observation_id: str = None,
            project_name: str = 'mindsdb',
            tools: List[BaseTool] = None) -> pd.DataFrame:
        '''
        Queries an agent to get a completion.

        Parameters:
            agent (db.Agents): Existing agent to get completion from
            messages (List[Dict[str, str]]): Chat history to send to the agent
            project_name (str): Project the agent belongs to (default mindsdb)
            tools (List[BaseTool]): Tools to use while getting the completion

        Returns:
            pd.DataFrame (pd.DataFrame): Completion as a DataFrame

        Raises:
            ValueError: Agent's model does not exist.
        '''
        # Model needs to exist.
        model_name_no_version, version = db.Predictor.get_name_and_version(agent.model_name)
        try:
            _ = self.model_controller.get_model(model_name_no_version, version=version, project_name=project_name)
        except PredictorRecordNotFound:
            return ValueError(f'Model with name {agent.model_name} not found')

        if tools is None:
            tools = []
        predict_params = {
            # Underlying handler (e.g. Langchain) will handle default tools like mdb_read, mdb_write, etc.
            'tools': tools,
            'skills': [s for s in agent.skills],
            **(agent.params or {})
        }
        if observation_id is not None:
            predict_params['observation_id'] = observation_id
        if trace_id is not None:
            predict_params['trace_id'] = trace_id
        project_datanode = self.datahub.get(project_name)
        return project_datanode.predict(
            model_name=agent.model_name,
            df=pd.DataFrame(messages),
            params=predict_params
        )
