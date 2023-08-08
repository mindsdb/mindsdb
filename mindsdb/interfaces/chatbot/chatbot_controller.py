from typing import Dict, List

from mindsdb.interfaces.storage import db
from mindsdb.interfaces.database.projects import ProjectController

from mindsdb.utilities.context import context as ctx

from mindsdb.api.mysql.mysql_proxy.controllers.session_controller import SessionController
from mindsdb.utilities.config import Config


class ChatBotController:
    '''Handles CRUD operations at the database level for Chatbots'''

    OBJECT_TYPE = 'chatbot'

    def __init__(self, project_controller: ProjectController = None):
        if project_controller is None:
            project_controller = ProjectController()
        self.project_controller = project_controller

    def get_chatbot(self, chatbot_name: str, project_name: str = 'mindsdb') -> db.ChatBots:
        '''
        Gets a chatbot by name.

        Parameters:
            chatbot_name (str): The name of the chatbot
            project_name (str): The name of the containing project

        Returns:
            bot (db.ChatBots): The database chatbot object
        '''

        project = self.project_controller.get(name=project_name)

        query = db.session.query(
            db.ChatBots
        ).join(
            db.Tasks, db.ChatBots.id == db.Tasks.object_id
        ).filter(
            db.ChatBots.name == chatbot_name,
            db.ChatBots.project_id == project.id,
            db.Tasks.object_type == self.OBJECT_TYPE,
            db.Tasks.company_id == ctx.company_id,
        )

        return query.first()

    def get_chatbots(self, project_name: str = 'mindsdb') -> List[dict]:
        '''
        Gets all chatbots in a project.

        Parameters:
            project_name (str): The name of the containing project

        Returns:
            all_bots (List[db.ChatBots]): List of database chatbot object
        '''

        project = self.project_controller.get(name=project_name)

        query = db.session.query(
            db.ChatBots, db.Tasks
        ).join(
            db.Tasks, db.ChatBots.id == db.Tasks.object_id
        ).filter(
            db.ChatBots.project_id == project.id,
            db.Tasks.object_type == self.OBJECT_TYPE,
            db.Tasks.company_id == ctx.company_id,
        )

        session = SessionController()
        database_names = {
            i['id']: i['name']
            for i in session.database_controller.get_list()
        }

        bots = []
        for bot, task in query.all():
            bots.append(
                {
                    'id': bot.id,
                    'name': bot.name,
                    'project': project_name,
                    'database_id': bot.database_id,  # TODO remove in future
                    'database': database_names.get(bot.database_id, '?'),
                    'model_name': bot.model_name,
                    'params': bot.params,
                    'created_at': bot.created_at,
                    'is_running': task.active,
                    'last_error': task.last_error,
                }
            )

        return bots

    def add_chatbot(
            self,
            name: str,
            project_name: str,
            model_name: str,
            database_id: int = None,
            is_running: bool = True,
            params: Dict[str, str] = {}) -> db.ChatBots:
        '''
        Adds a chatbot to the database.

        Parameters:
            name (str): The name of the new chatbot
            project_name (str): The containing project
            model_name (str): The name of the existing ML model the chatbot will use
            database_id (int): The ID of the existing database the chatbot will use
            is_running (bool): Whether or not to start the chatbot right after creation
            params: (Dict[str, str]): Parameters to use when running the chatbot

        Returns:
            bot (db.ChatBots): The created chatbot
        '''

        config = Config()

        is_cloud = config.get('cloud', False)
        if is_cloud and ctx.user_class == 0:
            raise Exception("You can't create chatbot")

        if project_name is None:
            project_name = 'mindsdb'
        project = self.project_controller.get(name=project_name)

        bot = self.get_chatbot(name, project_name)

        if bot is not None:
            raise Exception(f'Chat bot already exists: {name}')

        # TODO check input: model_name, database_id

        # check database
        session_controller = SessionController()
        db_record = session_controller.integration_controller.get_by_id(database_id)
        if db_record is None:
            raise Exception(f"Database doesn't exits {database_id}")

        bot = db.ChatBots(
            name=name,
            project_id=project.id,
            model_name=model_name,
            database_id=database_id,
            params=params,
        )
        db.session.add(bot)
        db.session.flush()

        task_record = db.Tasks(
            company_id=ctx.company_id,
            user_class=ctx.user_class,

            object_type=self.OBJECT_TYPE,
            object_id=bot.id,
            active=is_running
        )
        db.session.add(task_record)

        db.session.commit()

        return bot

    def update_chatbot(
            self,
            chatbot_name: str,
            project_name: str = 'mindsdb',
            name: str = None,
            model_name: str = None,
            database_id: int = None,
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
            is_running (bool): Whether or not the chatbot will run after update/creation
            params: (Dict[str, str]): Parameters to use when running the chatbot

        Returns:
            bot (db.ChatBots): The created or updated chatbot
        '''

        existing_chatbot = self.get_chatbot(chatbot_name, project_name=project_name)
        if existing_chatbot is None:
            raise Exception(f'Chat bot not found: {chatbot_name}')

        if name is not None and name != chatbot_name:
            # check new name
            bot2 = self.get_chatbot(name, project_name=project_name)
            if bot2 is not None:
                raise Exception(f'Chat already exists: {name}')

            existing_chatbot.name = name
        if model_name is not None:
            # TODO check model_name
            existing_chatbot.model_name = model_name
        if database_id is not None:
            # TODO check database_id
            existing_chatbot.database_id = database_id

        task = db.Tasks.query.filter(
            db.Tasks.object_type == self.OBJECT_TYPE,
            db.Tasks.object_id == existing_chatbot.id,
            db.Tasks.company_id == ctx.company_id,
        ).first()

        if task is not None:
            if is_running is not None:
                task.active = is_running

            # reload task
            task.reload = True

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

        bot = self.get_chatbot(chatbot_name, project_name)
        if bot is None:
            raise Exception(f"Chat bot doesn't exist: {chatbot_name}")

        task = db.Tasks.query.filter(
            db.Tasks.object_type == self.OBJECT_TYPE,
            db.Tasks.object_id == bot.id,
            db.Tasks.company_id == ctx.company_id,
        ).first()

        if task is not None:
            db.session.delete(task)

        db.session.delete(bot)

        db.session.commit()
