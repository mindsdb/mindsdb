import threading
import time
import requests

from mindsdb.api.mysql.mysql_proxy.controllers.session_controller import SessionController
from mindsdb.interfaces.storage import db
from mindsdb.utilities import log
from mindsdb.utilities.context import context as ctx
from mindsdb.interfaces.chatbot.chatbot_task import ChatBotTask
from mindsdb.interfaces.chatbot.chatbot_alerter import ChatbotAlerter


class ChatBotThread(threading.Thread):
    """A thread for polling style chatbots to operate."""
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
                ChatbotAlerter.send_slack_alert(
                'https://hooks.slack.com/services/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',
                "@here :robot_face: : The chatbot is unable to establish a connection",
                )

                log.logger.error(e)

            if self._to_stop:
                return
            log.logger.debug('running ' + self.name)
            time.sleep(7)

    def stop(self):
        self._to_stop = True
