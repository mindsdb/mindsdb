from mindsdb.interfaces.storage import db


class BaseTask:

    def __init__(self, task_id, object_id):
        self.task_id = task_id
        self.object_id = object_id

    def run(self, stop_event):
        raise NotImplementedError

    def set_error(self, message):
        task_record = db.Tasks.query.get(self.task_id)
        task_record.last_error = str(message)
        db.session.commit()
