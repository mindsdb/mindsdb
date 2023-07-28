import threading
import time

from mindsdb.api.mysql.mysql_proxy.controllers.session_controller import SessionController
from mindsdb.interfaces.storage import db
from mindsdb.utilities import log
from mindsdb.utilities.context import context as ctx
from mindsdb.interfaces.chatbot.chatbot_task import ChatBotTask


class ChatBotThread(threading.Thread):
    """A thread for polling style chatbots to operate."""
    def __init__(self, alerter, bot_record):
        threading.Thread.__init__(self)
        self.bot_record = bot_record
        self._to_stop = False
        self.alerter= alerter

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
                self.alerter.send_slack_alert(
                    "@here :robot_face: : The chatbot is unable to establish a connection",
                    [
                        {
                        "color": "#C80001",
                        "fields": [
                            {
                                "title": "Chatbot id",
                                "value": self._bot_record.id
                            }
                        ],
                        }
                    ]
                )

                log.logger.error(e)

            if self._to_stop:
                return
            log.logger.debug('running ' + self.name)
            time.sleep(7)

    def stop(self):
        self._to_stop = True
