import traceback
import threading

from mindsdb.utilities.context import context as ctx

from mindsdb.interfaces.triggers.trigger_task import TriggerTask
from mindsdb.interfaces.chatbot.chatbot_task import ChatBotTask
from mindsdb.utilities import log

from mindsdb.interfaces.storage import db


class TaskThread(threading.Thread):

    def __init__(self, task_record):
        threading.Thread.__init__(self)
        self.task_record = task_record
        self._stop_event = threading.Event()

    def run(self):
        # create context and session

        ctx.set_default()
        ctx.company_id = self.task_record.company_id
        if self.task_record.user_class is not None:
            ctx.user_class = self.task_record.user_class

        try:
            if self.task_record.object_type == 'trigger':
                trigger_id = self.task_record.object_id

                trigger = TriggerTask(trigger_id)
                trigger.run(self._stop_event)

            elif self.task_record.object_type == 'chatbot':
                bot_id = self.task_record.object_id

                bot = ChatBotTask(bot_id)
                bot.run(self._stop_event)

        except Exception:
            log.logger.error(traceback.format_exc())
            db.session.commit()
            pass

    def stop(self):
        self._stop_event.set()
