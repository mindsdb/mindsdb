from mindsdb.api.executor.sql_query import SQLQuery
from mindsdb.interfaces.query_context.context_controller import RunningQuery
from mindsdb.api.executor.controllers.session_controller import SessionController
from mindsdb.interfaces.tasks.task import BaseTask


class QueryTask(BaseTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.query_id = self.object_id

    def run(self, stop_event):

        # TODO database name
        session = SessionController()
        SQLQuery(query_id=self.query_id, session=session, stop_event=stop_event)

        # clear task
        RunningQuery(self.query_id).remove_from_tasks()
