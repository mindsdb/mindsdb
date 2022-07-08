import mindsdb_sql
from mindsdb_sql.parser.ast import (
    Insert,
    Set,
    Alter,
    Update
)
from mindsdb_sql import parse_sql
from mindsdb_sql.planner import utils as planner_utils

from mindsdb.api.mysql.mysql_proxy.classes.sql_query import (
    Column,
    SQLQuery
)
from mindsdb.api.mysql.mysql_proxy.classes.sql_statement_parser import SqlStatementParser
from mindsdb.api.mysql.mysql_proxy.utilities import (
    ErBadDbError,
    SqlApiException,
    log
)


from mindsdb.api.mysql.mysql_proxy.executor.executor_commands import ExecuteCommands


class Executor:

    def __init__(self, session, sqlserver):
        self.session = session
        self.sqlserver = sqlserver

        self.query = None

        # returns
        self.columns = []
        self.params = []
        self.data = None
        self.state_track = None
        self.server_status = None

        self.is_executed = False

        # self.predictor_metadata = {}

        self.sql = ''
        self.sql_lower = ''

        self.command_executor = ExecuteCommands(self.session, self)

    def stmt_prepare(self, sql):

        resp = self.execute_external(sql)
        if resp is not None:
            # is already executed
            self.is_executed = True
            return

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

            sqlquery = SQLQuery(
                self.query,
                session=self.session,
                execute=False
            )

            sqlquery.prepare_query()

            self.params = [
                Column(
                    alias=p.value,
                    type='str',
                    name=p.value,
                )
                for p in params
            ]

            # TODO:
            #   select * from mindsdb.predictors doesn't invoke prepare_steps and columns_list is empty
            self.columns = sqlquery.columns_list

    def stmt_execute(self, param_values):
        if self.is_executed:
            return

        # fill params
        self.query = planner_utils.fill_query_params(self.query, param_values)

        # execute query
        self.do_execute()

    def query_execute(self, sql):
        resp = self.execute_external(sql)
        if resp is not None:
            # is already executed
            self.is_executed = True
            return

        self.parse(sql)
        self.do_execute()

    def execute_external(self, sql):

        # not exec directly in integration
        return None

        # try exec in external integration
        if (
            isinstance(self.session.database, str)
            and len(self.session.database) > 0
            and self.session.database.lower() not in ('mindsdb',
                                                      'files',
                                                      'information_schema')
            and '@@' not in sql.lower()
            and (
                (
                    sql.lower().strip().startswith('select')
                    and 'from' in sql.lower()
                )
                or (
                    sql.lower().strip().startswith('show')
                    # and 'databases' in sql.lower()
                    and 'tables' in sql.lower()
                )
            )
        ):
            datanode = self.session.datahub.get(self.session.database)
            if datanode is None:
                raise ErBadDbError('Unknown database - %s' % self.session.database)

            # try parse or send raw sql
            try:
                sql = parse_sql(sql, dialect='mindsdb')
            except mindsdb_sql.exceptions.ParsingException:
                pass

            result, column_info = datanode.query(sql)
            columns = [
                Column(name=col['name'], type=col['type'])
                for col in column_info
            ]

            data = []
            if len(result) > 0:
                # columns = [{
                #     'table_name': '',
                #     'name': x,
                #     'type': TYPES.MYSQL_TYPE_VAR_STRING
                # } for x in result[0].keys()]

                data = [[str(value) for key, value in x.items()] for x in result]
            self.columns = columns
            self.data = data
            return True

    def parse(self, sql):
        self.sql = sql
        sql_lower = sql.lower()
        self.sql_lower = sql_lower.replace('`', '')

        try:
            try:
                self.query = parse_sql(sql, dialect='mindsdb')
            except Exception:
                self.query = parse_sql(sql, dialect='mysql')
        except Exception as e:
            # not all statements are parsed by parse_sql
            log.warning(f'SQL statement are not parsed by mindsdb_sql: {sql}')

            sql_list = [x for x in self.sql_lower.replace('\t', ' ').replace('\n', ' ').split(' ') if x not in ('', ' ')]
            if len(sql_list) > 1 and sql_list[0] == "show":
                raise SqlApiException(f"unknown command: {sql}")
            if len(sql_list) > 2 and " ".join(sql_list[:2]) == "create predictor":
                if 'predict' not in sql_list:
                    raise SqlApiException(f"'predict' field is mandatory: {sql}")
                # analyze predictor name
                if not sql_list[2][0].isalpha():
                    raise SqlApiException(f"predictor name must start from letter character: {sql}")

            # TODO
            if sql_lower == "set names 'utf8mb4' collate 'utf8mb4_general_ci'":
                self.query = Set()  # empty set command
            if sql_lower.startswith('alter table') and (
                    sql_lower.endswith('disable keys') or sql_lower.endswith('enable keys')):
                self.query = Set()  # empty set command

            st = SqlStatementParser(sql)
            keyword = st.keyword
            if keyword == 'set':
                self.query = Set()
            elif keyword == 'update':
                self.query = Update()
            elif keyword == 'insert':
                self.query = Insert(table='')
            elif keyword == 'alter':
                self.query = Alter()
            else:
                raise SqlApiException(f'SQL statement cannot be parsed by mindsdb_sql - {sql}: {e}') from e

            # not all statements are parse by parse_sql
            log.warning(f'SQL statement are not parsed by mindsdb_sql: {sql}')

            # == a place for workarounds ==
            # or run sql in integration without parsing

    def do_execute(self):
        # it can be already run at prepare state
        if self.is_executed:
            return

        ret = self.command_executor.execute_command(self.query)

        self.is_executed = True

        self.data = ret.data
        self.server_status = ret.status
        if ret.columns is not None:
            self.columns = ret.columns

        self.state_track = ret.state_track
