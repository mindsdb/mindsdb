from mindsdb.api.executor.sql_query import SQLQuery
from mindsdb.interfaces.query_context.context_controller import query_context_controller
from mindsdb.api.executor.controllers.session_controller import SessionController
from mindsdb.interfaces.tasks.task import BaseTask


class QueryTask(BaseTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.query_id = self.object_id

    def run(self, stop_event):

        try:
            session = SessionController()
            SQLQuery(None, query_id=self.query_id, session=session, stop_event=stop_event)
        finally:
            # clear task
            query_context_controller.get_query(self.query_id).remove_from_task()
