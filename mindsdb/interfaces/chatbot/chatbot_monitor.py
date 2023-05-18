
import threading
import time
from mindsdb.utilities import log


from mindsdb.utilities.config import Config
from mindsdb.interfaces.storage import db
from .chatbot_task import ChatBotTask
from mindsdb.utilities.context import context as ctx

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
        model_name = self.bot_record.model_name
        project_name = db.Project.query.get(self.bot_record.project_id).name

        database_name = db.Integration.query.get(self.bot_record.database_id).name

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



class MonitorTask(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self._to_stop = False
        self._active_bots = {}

    def run(self):
        # create context and session

        while True:
            try:
                allowed_bots = []

                # check new bots to start
                for bot in db.ChatBots.query.all():
                    bot_id = bot.id
                    allowed_bots.append(bot_id)

                    if bot_id not in self._active_bots:
                        # start new bot
                        thread = Task(bot)
                        thread.start()

                        self._active_bots[bot.id] = thread

                # check old bots to stop
                active_bots = list(self._active_bots.keys())
                for bot_id in active_bots:
                    if bot_id not in allowed_bots:
                        self.stop_bot(bot_id)

            except Exception as e:
                log.logger.error(e)

            if self._to_stop:
                # stop all bots
                active_bots = list(self._active_bots.keys())
                for bot_id in active_bots:
                    self.stop_bot(bot_id)

                return

            db.session.rollback()  # disable cache
            time.sleep(1)

    def stop_bot(self, bot_id):
        self._active_bots[bot_id].stop()
        del self._active_bots[bot_id]

    def stop(self):
        self._to_stop = True


class ChatBotMonitor:

    def __init__(self):
        self.thread = MonitorTask()
        self.thread.start()

    def __del__(self):
        self.thread.stop()

    def stop(self):
        self.thread.stop()


monitor = None


def start():

    is_cloud = Config().get('cloud', False)
    if is_cloud is True:
        # Chatbots are disabled on cloud
        return

    # run monitoring thread
    global monitor
    monitor = ChatBotMonitor()


def stop():
    monitor.stop()