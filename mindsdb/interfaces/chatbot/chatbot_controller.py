from typing import Dict, List

from mindsdb.utilities.config import Config
from mindsdb.interfaces.storage import db
from mindsdb.interfaces.database.projects import ProjectController

from mindsdb.utilities.context import context as ctx


class ChatBotController:
    '''Handles CRUD operations at the database level for Chatbots'''

    def __init__(self, project_controller: ProjectController = None):
        if project_controller is None:
            project_controller = ProjectController()
        self.project_controller = project_controller

    def _raise_if_on_cloud(self):
        # TODO(tmichaeldb): Remove for public Chatbots release.
        is_cloud = Config().get('cloud', False)
        if is_cloud is True:
            raise Exception('Chatbots are disabled on cloud')

    def get_chatbot(self, chatbot_name: str, project_name: str = 'mindsdb') -> db.ChatBots:
        '''
        Gets a chatbot by name.

        Parameters:
            chatbot_name (str): The name of the chatbot
            project_name (str): The name of the containing project

        Returns:
            bot (db.ChatBots): The database chatbot object
        '''
        self._raise_if_on_cloud()

        project = self.project_controller.get(name=project_name)
        bot = db.ChatBots.query.filter(
            db.ChatBots.company_id == ctx.company_id,
            db.ChatBots.name == chatbot_name,
            db.ChatBots.project_id == project.id
        ).first()
        return bot

    def get_chatbots(self, project_name: str = 'mindsdb') -> List[db.ChatBots]:
        '''
        Gets all chatbots in a project.

        Parameters:
            project_name (str): The name of the containing project

        Returns:
            all_bots (List[db.ChatBots]): List of database chatbot object
        '''
        self._raise_if_on_cloud()

        project = self.project_controller.get(name=project_name)
        all_bots = db.ChatBots.query.filter(
            db.ChatBots.company_id == ctx.company_id,
            db.ChatBots.project_id == project.id
        ).all()
        return all_bots

    def add_chatbot(
            self,
            name: str,
            project_name: str,
            model_name: str,
            database_id: int = None,
            chat_engine: str = None,
            is_running: bool = False,
            params: Dict[str, str] = {}) -> db.ChatBots:
        '''
        Adds a chatbot to the database.

        Parameters:
            name (str): The name of the new chatbot
            project_name (str): The containing project
            model_name (str): The name of the existing ML model the chatbot will use
            database_id (int): The ID of the existing database the chatbot will use
            chat_engine (str): The name of the chat integration the chatbot will use (e.g. slack)
            is_running (bool): Whether or not to start the chatbot right after creation
            params: (Dict[str, str]): Parameters to use when running the chatbot

        Returns:
            bot (db.ChatBots): The created chatbot
        '''
        self._raise_if_on_cloud()

        if project_name is None:
            project_name = 'mindsdb'
        project = self.project_controller.get(name=project_name)

        bot = db.ChatBots.query.filter(
            db.ChatBots.company_id == ctx.company_id,
            db.ChatBots.name == name,
            db.ChatBots.project_id == project.id
        ).first()

        if bot is not None:
            raise Exception(f'Chat bot already exists: {name}')

        bot = db.ChatBots(
            company_id=ctx.company_id,
            name=name,
            project_id=project.id,
            model_name=model_name,
            database_id=database_id,
            chat_engine=chat_engine,
            is_running=is_running,
            params=params,
        )
        db.session.add(bot)
        db.session.commit()

        return bot

    def update_chatbot(
            self,
            chatbot_name: str,
            project_name: str = 'mindsdb',
            name: str = None,
            model_name: str = None,
            database_id: int = None,
            chat_engine: str = None,
            is_running: bool = None,
            params: Dict[str, str] = None):
        '''
        Updates a chatbot in the database, creating it if it doesn't already exist.

        Parameters:
            chatbot_name (str): The name of the new chatbot, or existing chatbot to update
            project_name (str): The containing project
            name (str): The updated name of the chatbot
            model_name (str): The name of the existing ML model the chatbot will use
            database_id (int): The ID of the existing database the chatbot will use
            chat_engine (str): The name of the chat integration the chatbot will use (e.g. slack)
            is_running (bool): Whether or not the chatbot will run after update/creation
            params: (Dict[str, str]): Parameters to use when running the chatbot

        Returns:
            bot (db.ChatBots): The created or updated chatbot
        '''
        self._raise_if_on_cloud()

        existing_chatbot = self.get_chatbot(chatbot_name, project_name=project_name)
        if existing_chatbot is None:
            return None

        if name is not None:
            existing_chatbot.name = name
        if model_name is not None:
            existing_chatbot.model_name = model_name
        if database_id is not None:
            existing_chatbot.database_id = database_id
        if chat_engine is not None:
            existing_chatbot.chat_engine = chat_engine
        if is_running is not None:
            existing_chatbot.is_running = is_running
        if params is not None:
            existing_chatbot.params = params
        db.session.commit()

        return existing_chatbot

    def delete_chatbot(self, chatbot_name: str, project_name: str = 'mindsdb'):
        '''
        Deletes a chatbot by name.

        Parameters:
            chatbot_name (str): The name of the chatbot to delete
            project_name (str): The name of the containing project
        '''

        self._raise_if_on_cloud()

        project = self.project_controller.get(name=project_name)

        bot = db.ChatBots.query.filter(
            db.ChatBots.company_id == ctx.company_id,
            db.ChatBots.name == chatbot_name,
            db.ChatBots.project_id == project.id
        ).first()

        if bot is None:
            raise Exception(f'Chat bot not found: {chatbot_name}')

        db.session.delete(bot)
        db.session.commit()
