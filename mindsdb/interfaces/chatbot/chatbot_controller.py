import threading
import time

from mindsdb.utilities.config import Config
from mindsdb.interfaces.storage import db
from mindsdb.utilities import log
from mindsdb.interfaces.database.projects import ProjectController

from mindsdb.utilities.context import context as ctx

from .chatbot_task import ChatBotTask



class Task(threading.Thread):
    def __init__(self, bot_record):
        threading.Thread.__init__(self)
        self.bot_record = bot_record
        self._to_stop = False

    def run(self):
        # create context and session

        ctx.set_default()
        ctx.company_id = self.bot_record.company_id
        if self.bot_record.user_class is not None:
            ctx.user_class = self.bot_record.user_class

        from mindsdb.api.mysql.mysql_proxy.controllers.session_controller import SessionController
        session = SessionController()

        # TODO check deleted, raise errors
        # TODO checks on delete predictor/ project/ integration
        model_name = db.Predictor.query.get(self.bot_record.model_id).name
        project_name = db.Project.query.get(self.bot_record.project_id).name

        database_name = db.Integration.query.get(self.bot_record.handler_id).name

        task = ChatBotTask(session,
                           database=database_name,
                           project_name=project_name,
                           model_name=model_name,
                           params=self.bot_record.params)

        while True:
            try:
                task.run()
            except Exception as e:
                log.logger.error(e)

            if self._to_stop:
                return
            print('running ' + self.name)
            time.sleep(3)

    def stop(self):
        self._to_stop = True


class ChatBotController:
    def __int__(self):
        self._active_bots = {}

    def init(self):

        for bot in db.ChatBots.query.all():
            self.start_bot(bot)

    def add(self, name, chat_handler, model):

        # TODO   !!!!
        bot = db.ChatBots(
            name=name,
            project_id=..
            ...
        )
        db.session.add(bot)
        db.session.commit()

        self.start_bot(bot)

    def delete(self, name, project_name):
        project_controller = ProjectController()
        project = project_controller.get(name=project_name)

        bot = db.ChatBots.query.filter(
            db.ChatBots.company_id == ctx.company_id,
            db.ChatBots.name == name,
            db.ChatBots.project_id == project.id
        ).first()

        if bot is None:
            raise Exception()

        db.session.delete(bot)
        db.session.commit()

        self.stop_bot(bot)


    def start_bot(self, bot):
        thread = Task(bot)
        thread.start()
        self._active_bots[bot.id] = self._active_bots


    def stop_bot(self, bot):
        bot_id = bot.id
        if bot_id in self._active_bots:
            self._active_bots[bot_id].stop()


chatbot_controller = None

def init():
    is_cloud = Config().get('cloud', False)
    if is_cloud is True:
        # Chatbots are disabled on clou
        pass
    else:
        chatbot_controller = ChatBotController()
        chatbot_controller.init()

