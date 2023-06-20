from mindsdb.utilities.config import Config
from mindsdb.interfaces.storage import db
from mindsdb.interfaces.database.projects import ProjectController

from mindsdb.utilities.context import context as ctx


class ChatBotController:

    def add_chatbot(self, name, project_name, model_name, database_id, params):
        is_cloud = Config().get('cloud', False)
        if is_cloud is True:
            raise Exception('Chatbots are disabled on cloud')

        if project_name is None:
            project_name = 'mindsdb'
        project_controller = ProjectController()
        project = project_controller.get(name=project_name)

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
            params=params,
        )
        db.session.add(bot)
        db.session.commit()

    def delete_chatbot(self, name, project_name):
        project_controller = ProjectController()
        project = project_controller.get(name=project_name)

        bot = db.ChatBots.query.filter(
            db.ChatBots.company_id == ctx.company_id,
            db.ChatBots.name == name,
            db.ChatBots.project_id == project.id
        ).first()

        if bot is None:
            raise Exception(f'Chat bot not found: {name}')

        db.session.delete(bot)
        db.session.commit()
