
import threading
from mindsdb.utilities.context import context as ctx
from mindsdb.interfaces.storage import db

from mindsdb.interfaces.triggers.trigger_task import TriggerTask


class TaskThread(threading.Thread):

    def __init__(self, task_id):
        threading.Thread.__init__(self)
        self.task_id = task_id
        self._stop_event = threading.Event()

    def run(self):
        # create context and session

        task_record = db.Tasks.query.get(self.task_id)

        ctx.set_default()
        ctx.company_id = task_record.company_id
        if task_record.user_class is not None:
            ctx.user_class = task_record.user_class

        object_type = task_record.object_type
        object_id = task_record.object_id

        if object_type == 'trigger':

            trigger = TriggerTask(object_id)
            trigger.run(self._stop_event)

        elif object_type == 'chatbot':
            # TODO
            ...
        db.session.rollback()

    def stop(self):

        self._stop_event.set()
