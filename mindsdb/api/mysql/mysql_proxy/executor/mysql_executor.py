from mindsdb_sql_parser import parse_sql
from mindsdb.api.executor.planner import utils as planner_utils

import mindsdb.utilities.profiler as profiler
from mindsdb.api.executor.sql_query.result_set import Column
from mindsdb.api.executor.sql_query import SQLQuery
from mindsdb.api.executor.data_types.answer import ExecuteAnswer
from mindsdb.api.executor.command_executor import ExecuteCommands
from mindsdb.api.mysql.mysql_proxy.utilities import ErSqlSyntaxError
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import MYSQL_DATA_TYPE
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class Executor:
    def __init__(self, session, sqlserver):
        self.session = session
        self.sqlserver = sqlserver

        self.query = None

        self.columns: list[Column] = []
        self.params: list[Column] = []
        self.data = None
        self.server_status = None
        self.is_executed = False
        self.error_message = None
        self.error_code = None
        self.executor_answer: ExecuteAnswer = None

        self.sql = ""
        self.sql_lower = ""

        context = {'connection_id': self.sqlserver.connection_id}
        self.command_executor = ExecuteCommands(self.session, context)

    def change_default_db(self, new_db):
        self.command_executor.change_default_db(new_db)

    def stmt_prepare(self, sql):

        self.parse(sql)

        # if not params
        params = planner_utils.get_query_params(self.query)
        if len(params) == 0:
            # execute immediately
            self.do_execute()

        else:
            # plan query
            # TODO less complex.
            #  planner is inside SQLQuery now.

            sqlquery = SQLQuery(self.query, session=self.session, execute=False)

            sqlquery.prepare_query()

            self.params = [Column(
                name=p.value,
                alias=p.value,
                type=MYSQL_DATA_TYPE.TEXT
            ) for p in params]

            # TODO:
            #   select * from mindsdb.models doesn't invoke prepare_steps and columns_list is empty
            self.columns = sqlquery.columns_list

    def stmt_execute(self, param_values):
        if self.is_executed:
            return

        # fill params
        self.query = planner_utils.fill_query_params(self.query, param_values)

        # execute query
        self.do_execute()

    @profiler.profile()
    def query_execute(self, sql):
        self.parse(sql)
        self.do_execute()

    @profiler.profile()
    def parse(self, sql):
        self.sql = sql
        sql_lower = sql.lower()
        self.sql_lower = sql_lower.replace("`", "")

        try:
            self.query = parse_sql(sql)
        except Exception as mdb_error:
            # not all statements are parsed by parse_sql
            logger.warning('Failed to parse SQL query')
            logger.debug(f'Query that cannot be parsed: {sql}')

            raise ErSqlSyntaxError(
                f"The SQL statement cannot be parsed - {sql}: {mdb_error}"
            ) from mdb_error

            # == a place for workarounds ==
            # or run sql in integration without parsing

    @profiler.profile()
    def do_execute(self):
        # it can be already run at prepare state
        if self.is_executed:
            return

        executor_answer: ExecuteAnswer = self.command_executor.execute_command(self.query)
        self.executor_answer = executor_answer

        self.is_executed = True
