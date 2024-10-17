from typing import Dict, List

from mindsdb.interfaces.agents.agents_controller import AgentsController
from mindsdb.interfaces.chatbot.chatbot_task import ChatBotTask
from mindsdb.interfaces.database.projects import ProjectController
from mindsdb.interfaces.storage import db

from mindsdb.utilities.context import context as ctx

from mindsdb.api.executor.controllers.session_controller import SessionController
from mindsdb.utilities.config import Config


class ChatBotController:
    '''Handles CRUD operations at the database level for Chatbots'''

    OBJECT_TYPE = 'chatbot'

    def __init__(self, project_controller: ProjectController = None, agents_controller: AgentsController = None):
        if project_controller is None:
            project_controller = ProjectController()
        if agents_controller is None:
            agents_controller = AgentsController()
        self.project_controller = project_controller
        self.agents_controller = agents_controller

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
            db.ChatBots, db.Tasks
        ).join(
            db.Tasks, db.ChatBots.id == db.Tasks.object_id
        ).filter(
            db.ChatBots.name == chatbot_name,
            db.ChatBots.project_id == project.id,
            db.Tasks.object_type == self.OBJECT_TYPE,
            db.Tasks.company_id == ctx.company_id,
        )

        return self._get_chatbot(query, project)

    def get_chatbot_by_id(self, chatbot_id: int) -> db.ChatBots:
        '''
        Gets a chatbot by id.

        Parameters:
            chatbot_id (int): The id of the chatbot

        Returns:
            bot (db.ChatBots): The database chatbot object
        '''

        query = db.session.query(
            db.ChatBots, db.Tasks
        ).join(
            db.Tasks, db.ChatBots.id == db.Tasks.object_id
        ).filter(
            db.ChatBots.id == chatbot_id,
            db.Tasks.object_type == self.OBJECT_TYPE,
            db.Tasks.company_id == ctx.company_id,
        )

        return self._get_chatbot(query)

    def _get_chatbot(self, query, project: db.Project = None) -> db.ChatBots:
        '''
        Gets a chatbot by query.

        Parameters:
            query: The query to get the chatbot

        Returns:
            bot (db.ChatBots): The database chatbot object
        '''

        query_result = query.first()
        if query_result is None:
            return None
        bot, task = query_result

        # Include DB, Agent, and Task information in response.
        session = SessionController()
        database_names = {
            i['id']: i['name']
            for i in session.database_controller.get_list()
        }

        agent = self.agents_controller.get_agent_by_id(bot.agent_id)
        agent_obj = agent.as_dict() if agent is not None else None
        bot_obj = {
            'id': bot.id,
            'name': bot.name,
            'project': project.name if project else self.project_controller.get(bot.project_id).name,
            'agent': agent_obj,
            'database_id': bot.database_id,  # TODO remove in future
            'database': database_names.get(bot.database_id, '?'),
            'model_name': bot.model_name,
            'params': bot.params,
            'created_at': bot.created_at,
            'is_running': task.active,
            'last_error': task.last_error,
            'webhook_token': bot.webhook_token,
        }

        return bot_obj

    def get_chatbots(self, project_name: str = 'mindsdb') -> List[dict]:
        '''
        Gets all chatbots in a project.

        Parameters:
            project_name (str): The name of the containing project

        Returns:
            all_bots (List[db.ChatBots]): List of database chatbot object
        '''

        query = db.session.query(db.Project).filter_by(
            company_id=ctx.company_id,
            deleted_at=None
        )
        if project_name is not None:
            query = query.filter_by(name=project_name)
        project_names = {
            i.id: i.name
            for i in query
        }

        query = db.session.query(
            db.ChatBots, db.Tasks
        ).join(
            db.Tasks, db.ChatBots.id == db.Tasks.object_id
        ).filter(
            db.ChatBots.project_id.in_(list(project_names.keys())),
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
            agent = self.agents_controller.get_agent_by_id(bot.agent_id)
            agent_obj = agent.as_dict() if agent is not None else None
            bots.append(
                {
                    'id': bot.id,
                    'name': bot.name,
                    'project': project_names[bot.project_id],
                    'agent': agent_obj,
                    'database_id': bot.database_id,  # TODO remove in future
                    'database': database_names.get(bot.database_id, '?'),
                    'model_name': bot.model_name,
                    'params': bot.params,
                    'created_at': bot.created_at,
                    'is_running': task.active,
                    'last_error': task.last_error,
                    'webhook_token': bot.webhook_token,
                }
            )

        return bots

    def add_chatbot(
            self,
            name: str,
            project_name: str,
            model_name: str = None,
            agent_name: str = None,
            database_id: int = None,
            is_running: bool = True,
            params: Dict[str, str] = {}) -> db.ChatBots:
        '''
        Adds a chatbot to the database.

        Parameters:
            name (str): The name of the new chatbot
            project_name (str): The containing project
            model_name (str): The name of the existing ML model the chatbot will use
            agent_name (str): The name of the existing agent the chatbot will use
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

        # check database
        session_controller = SessionController()
        db_record = session_controller.integration_controller.get_by_id(database_id)
        if db_record is None:
            raise Exception(f"Database doesn't exist: {database_id}")

        if model_name is None and agent_name is None:
            raise ValueError('Need to provide either "model_name" or "agent_name" when creating a chatbot')
        if agent_name is not None:
            agent = self.agents_controller.get_agent(agent_name, project_name)
            if agent is None:
                raise ValueError(f"Agent with name doesn't exist: {agent_name}")
        else:
            # Create a new agent with the given model name.
            agent = self.agents_controller.add_agent(name, project_name, model_name, [])

        bot = db.ChatBots(
            name=name,
            project_id=project.id,
            agent_id=agent.id,
            model_name=agent.model_name,
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
            agent_name: str = None,
            database_id: int = None,
            is_running: bool = None,
            params: Dict[str, str] = None,
            webhook_token: str = None) -> db.ChatBots:
        '''
        Updates a chatbot in the database, creating it if it doesn't already exist.

        Parameters:
            chatbot_name (str): The name of the new chatbot, or existing chatbot to update
            project_name (str): The containing project
            name (str): The updated name of the chatbot
            model_name (str): The name of the existing ML model the chatbot will use
            agent_name (str): The name of the existing agent the chatbot will use
            database_id (int): The ID of the existing database the chatbot will use
            is_running (bool): Whether or not the chatbot will run after update/creation
            params: (Dict[str, str]): Parameters to use when running the chatbot

        Returns:
            bot (db.ChatBots): The created or updated chatbot
        '''

        existing_chatbot = self.get_chatbot(chatbot_name, project_name=project_name)
        if existing_chatbot is None:
            raise Exception(f'Chat bot not found: {chatbot_name}')

        existing_chatbot_rec = db.ChatBots.query.get(existing_chatbot['id'])

        if name is not None and name != chatbot_name:
            # check new name
            bot2 = self.get_chatbot(name, project_name=project_name)
            if bot2 is not None:
                raise Exception(f'Chat already exists: {name}')

            existing_chatbot_rec.name = name

        if agent_name is not None:
            agent = self.agents_controller.get_agent(agent_name, project_name)
            if agent is None:
                raise ValueError(f"Agent with name doesn't exist: {agent_name}")
            existing_chatbot_rec.agent_id = agent.id

        if model_name is not None:
            # TODO check model_name
            existing_chatbot_rec.model_name = model_name
        if database_id is not None:
            # TODO check database_id
            existing_chatbot_rec.database_id = database_id

        task = db.Tasks.query.filter(
            db.Tasks.object_type == self.OBJECT_TYPE,
            db.Tasks.object_id == existing_chatbot_rec.id,
            db.Tasks.company_id == ctx.company_id,
        ).first()

        if task is not None:
            if is_running is not None:
                task.active = is_running

            # reload task
            task.reload = True

        if params is not None:
            # Merge params on update
            existing_params = existing_chatbot_rec.params or {}
            params.update(existing_params)
            existing_chatbot_rec.params = params

        if webhook_token is not None:
            existing_chatbot_rec.webhook_token = webhook_token

        db.session.commit()

        return existing_chatbot_rec

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

        bot_rec = db.ChatBots.query.get(bot['id'])

        task = db.Tasks.query.filter(
            db.Tasks.object_type == self.OBJECT_TYPE,
            db.Tasks.object_id == bot_rec.id,
            db.Tasks.company_id == ctx.company_id,
        ).first()

        if task is not None:
            db.session.delete(task)

        db.session.delete(bot_rec)

        db.session.commit()

    def on_webhook(self, webhook_token: str, request: dict, chat_bot_memory: dict):
        """
        Handles incoming webhook requests.
        Finds the chat bot associated with the webhook token and passes the request to the chat bot task.

        Args:
            webhook_token (str): The token to uniquely identify the webhook.
            request (dict): The incoming webhook request.
            chat_bot_memory (dict): The memory of the various chat-bots mapped by their webhook tokens.
        """
        query = db.session.query(
            db.ChatBots, db.Tasks
        ).join(
            db.Tasks, db.ChatBots.id == db.Tasks.object_id
        ).filter(
            db.ChatBots.webhook_token == webhook_token,
            db.Tasks.object_type == self.OBJECT_TYPE,
            db.Tasks.company_id == ctx.company_id,
        )
        result = query.first()

        chat_bot, task = result if result is not None else (None, None)

        if chat_bot is None:
            raise Exception(f"No chat bot exists for webhook token: {webhook_token}")

        if not task.active:
            raise Exception(f"Chat bot is not running: {chat_bot.name}")

        chat_bot_task = ChatBotTask(task_id=task.id, object_id=chat_bot.id)

        if webhook_token in chat_bot_memory:
            chat_bot_task.set_memory(chat_bot_memory[webhook_token])
        else:
            chat_bot_memory[webhook_token] = chat_bot_task.get_memory()

        chat_bot_task.on_webhook(request)
