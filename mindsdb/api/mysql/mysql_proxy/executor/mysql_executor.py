from mindsdb_sql import parse_sql
from mindsdb_sql.planner import utils as planner_utils

import mindsdb.utilities.profiler as profiler
from mindsdb.api.executor import Column, SQLQuery
from mindsdb.api.executor.command_executor import ExecuteCommands
from mindsdb.api.mysql.mysql_proxy.utilities import ErSqlSyntaxError
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class Executor:
    """This class stores initial and intermediate params
    between different steps of query execution. And it is also
    creates a separate instance of ExecuteCommands to execute the current
    query step.

    IMPORTANT: A public API of this class is a contract.
    And there are at least 2 classes strongly depend on it:
        ExecuctorClient
        ExecutorService.
    These classes do the same work as Executor when
    MindsDB works in 'modularity' mode.
    Thus please make sure that IF you change the API,
    you must update the API of these two classes as well!"""

    def __init__(self, session, sqlserver):
        self.session = session
        self.sqlserver = sqlserver

        self.query = None

        # returned values
        # all this attributes needs to be added in
        # self.json() method
        self.columns = []
        self.params = []
        self.data = None
        self.state_track = None
        self.server_status = None
        self.is_executed = False
        self.error_message = None
        self.error_code = None

        # self.predictor_metadata = {}

        self.sql = ""
        self.sql_lower = ""

        context = {'connection_id': self.sqlserver.connection_id}
        self.command_executor = ExecuteCommands(self.session, context)

    def change_default_db(self, new_db):
        self.command_executor.change_default_db(new_db)

    def stmt_prepare(self, sql):

        # resp = self.execute_external(sql)
        # if resp is not None:
        #     # is already executed
        #     self.is_executed = True
        #     return

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

            self.params = [
                Column(
                    alias=p.value,
                    type="str",
                    name=p.value,
                )
                for p in params
            ]

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
        logger.info("%s.query_execute: sql - %s", self.__class__.__name__, sql)
        # resp = self.execute_external(sql)
        # if resp is not None:
        #     # is already executed
        #     self.is_executed = True
        #     return

        self.parse(sql)
        self.do_execute()

    # for awesome Mongo API only
    # def binary_query_execute(self, sql):
    #     self.sql = sql.to_string()
    #     self.sql_lower = self.sql.lower()
    #
    #     ret = self.command_executor.execute_command(sql)
    #     self.error_code = ret.error_code
    #     self.error_message = ret.error_message
    #
    #     self.data = ret.data
    #     self.server_status = ret.status
    #     if ret.columns is not None:
    #         self.columns = ret.columns
    #
    #     self.state_track = ret.state_track

    # def execute_external(self, sql):
    #
    #     # not exec directly in integration
    #     return None

    @profiler.profile()
    def parse(self, sql):
        logger.info("%s.parse: sql - %s", self.__class__.__name__, sql)
        self.sql = sql
        sql_lower = sql.lower()
        self.sql_lower = sql_lower.replace("`", "")

        try:
            self.query = parse_sql(sql, dialect="mindsdb")
        except Exception as mdb_error:
            try:
                self.query = parse_sql(sql, dialect="mysql")
            except Exception:
                # not all statements are parsed by parse_sql
                logger.warning(f"SQL statement is not parsed by mindsdb_sql: {sql}")

                raise ErSqlSyntaxError(
                    f"SQL statement cannot be parsed by mindsdb_sql - {sql}: {mdb_error}"
                ) from mdb_error

                # == a place for workarounds ==
                # or run sql in integration without parsing

    @profiler.profile()
    def do_execute(self):
        # it can be already run at prepare state
        logger.info("%s.do_execute", self.__class__.__name__)
        if self.is_executed:
            return

        ret = self.command_executor.execute_command(self.query)
        self.error_code = ret.error_code
        self.error_message = ret.error_message

        self.is_executed = True

        self.data = ret.data
        self.server_status = ret.status
        if ret.columns is not None:
            self.columns = ret.columns

        self.state_track = ret.state_track
