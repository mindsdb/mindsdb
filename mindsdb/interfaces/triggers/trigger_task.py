import copy
import traceback
from mindsdb_sql_parser import parse_sql
from mindsdb_sql_parser.ast import Data, Identifier
from mindsdb.integrations.utilities.query_traversal import query_traversal

from mindsdb.interfaces.storage import db

from mindsdb.api.executor.controllers.session_controller import SessionController
from mindsdb.api.executor.command_executor import ExecuteCommands

from mindsdb.interfaces.database.projects import ProjectController
from mindsdb.utilities import log
from mindsdb.interfaces.tasks.task import BaseTask
from mindsdb.utilities.context import context as ctx

logger = log.getLogger(__name__)


class TriggerTask(BaseTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.command_executor = None
        self.query = None

        # callback might be without context
        self._ctx_dump = ctx.dump()

    def run(self, stop_event):
        trigger = db.Triggers.query.get(self.object_id)

        # parse query
        self.query = parse_sql(trigger.query_str)

        session = SessionController()

        # prepare executor
        project_controller = ProjectController()
        project = project_controller.get(trigger.project_id)

        session.database = project.name

        self.command_executor = ExecuteCommands(session)

        # subscribe
        database = session.integration_controller.get_by_id(trigger.database_id)
        data_handler = session.integration_controller.get_data_handler(database['name'])

        columns = trigger.columns
        if columns is not None:
            if columns == '':
                columns = None
            else:
                columns = columns.split('|')

        data_handler.subscribe(stop_event, self._callback, trigger.table_name, columns=columns)

    def _callback(self, row, key=None):
        logger.debug(f'trigger call: {row}, {key}')

        # set up environment
        ctx.load(self._ctx_dump)

        try:
            if key is not None:
                row.update(key)
            table = [
                row
            ]

            # inject data to query
            query = copy.deepcopy(self.query)

            def find_table(node, is_table, **kwargs):

                if is_table:
                    if (
                            isinstance(node, Identifier)
                            and len(node.parts) == 1
                            and node.parts[0] == 'TABLE_DELTA'
                    ):
                        # replace with data
                        return Data(table, alias=node.alias)

            query_traversal(query, find_table)

            # exec query
            ret = self.command_executor.execute_command(query)
            if ret.error_code is not None:
                self.set_error(ret.error_message)

        except Exception:
            self.set_error(str(traceback.format_exc()))

        db.session.commit()
