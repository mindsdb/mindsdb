import traceback
import threading
from mindsdb.utilities.context import context as ctx
from mindsdb.interfaces.storage import db
from mindsdb.utilities import log

from mindsdb.interfaces.triggers.trigger_task import TriggerTask
from mindsdb.interfaces.chatbot.chatbot_task import ChatBotTask
from mindsdb.interfaces.query_context.query_task import QueryTask

logger = log.getLogger(__name__)


class TaskThread(threading.Thread):

    def __init__(self, task_id):
        threading.Thread.__init__(self)
        self.task_id = task_id
        self._stop_event = threading.Event()
        self.object_type = None
        self.object_id = None

    def run(self):
        # create context and session

        task_record = db.Tasks.query.get(self.task_id)

        ctx.set_default()
        ctx.company_id = task_record.company_id
        if task_record.user_class is not None:
            ctx.user_class = task_record.user_class
        ctx.task_id = task_record.id

        self.object_type = task_record.object_type
        self.object_id = task_record.object_id

        logger.info(f'Task starting: {self.object_type}.{self.object_id}')
        try:
            if self.object_type == 'trigger':

                trigger = TriggerTask(self.task_id, self.object_id)
                trigger.run(self._stop_event)

            elif self.object_type == 'chatbot':
                bot = ChatBotTask(self.task_id, self.object_id)
                bot.run(self._stop_event)

            elif self.object_type == 'query':
                query = QueryTask(self.task_id, self.object_id)
                query.run(self._stop_event)

        except Exception:
            logger.error(traceback.format_exc())
            task_record.last_error = str(traceback.format_exc())

        db.session.commit()

    def stop(self):
        logger.info(f'Task stopping: {self.object_type}.{self.object_id}')

        self._stop_event.set()
