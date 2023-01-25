import mindsdb_sql
from numpy import dtype as np_dtype
from pandas.api import types as pd_types
from mindsdb_sql import parse_sql
from mindsdb_sql.planner import utils as planner_utils

from mindsdb.api.mysql.mysql_proxy.classes.sql_query import Column, SQLQuery
from mindsdb.api.mysql.mysql_proxy.utilities import (
    ErBadDbError,
    SqlApiException,
    logger,
)
from mindsdb.api.mysql.mysql_proxy.utilities.lightwood_dtype import dtype
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import (
    TYPES,
)

# import logging
# logger = logging.getLogger("mindsdb.main")


from mindsdb.api.mysql.mysql_proxy.executor.executor_commands import ExecuteCommands


class Executor:
    """This class stores initial and intermediatea params
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

        # self.predictor_metadata = {}

        self.sql = ""
        self.sql_lower = ""

        self.command_executor = ExecuteCommands(self.session, self)

    def change_default_db(self, new_db):
        self.command_executor.change_default_db(new_db)

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

    def query_execute(self, sql):
        logger.info("%s.query_execute: sql - %s", self.__class__.__name__, sql)
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
            and self.session.database.lower()
            not in ("mindsdb", "files", "information_schema")
            and "@@" not in sql.lower()
            and (
                (sql.lower().strip().startswith("select") and "from" in sql.lower())
                or (
                    sql.lower().strip().startswith("show")
                    # and 'databases' in sql.lower()
                    and "tables" in sql.lower()
                )
            )
        ):
            datanode = self.session.datahub.get(self.session.database)
            if datanode is None:
                raise ErBadDbError("Unknown database - %s" % self.session.database)

            # try parse or send raw sql
            try:
                sql = parse_sql(sql, dialect="mindsdb")
            except mindsdb_sql.exceptions.ParsingException:
                pass

            result, column_info = datanode.query(sql)
            columns = [
                Column(name=col["name"], type=col["type"]) for col in column_info
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

                raise SqlApiException(
                    f"SQL statement cannot be parsed by mindsdb_sql - {sql}: {mdb_error}"
                ) from mdb_error

                # == a place for workarounds ==
                # or run sql in integration without parsing

    def do_execute(self):
        # it can be already run at prepare state
        logger.info("%s.do_execute", self.__class__.__name__)
        if self.is_executed:
            return

        ret = self.command_executor.execute_command(self.query)

        self.is_executed = True

        self.data = ret.data
        self.server_status = ret.status
        if ret.columns is not None:
            self.columns = ret.columns

        self.state_track = ret.state_track

    def _to_json(self):
        params = {
            "columns": self.to_mysql_columns(self.columns),
            "params": self.to_mysql_columns(self.params),
            "data": self.data,
            "state_track": self.state_track,
            "server_status": self.server_status,
            "is_executed": self.is_executed,
        }
        return params

    def to_mysql_columns(self, columns):
        """Converts raw columns data into convinient format(list of lists) for the futher usage.
        Plus, it is also converts column types into internal ones."""

        result = []

        database = (
            None if self.session.database == "" else self.session.database.lower()
        )
        for column_record in columns:

            field_type = column_record.type

            column_type = TYPES.MYSQL_TYPE_VAR_STRING
            # is already in mysql protocol type?
            if isinstance(field_type, int):
                column_type = field_type
            # pandas checks
            elif isinstance(field_type, np_dtype):
                if pd_types.is_integer_dtype(field_type):
                    column_type = TYPES.MYSQL_TYPE_LONG
                elif pd_types.is_numeric_dtype(field_type):
                    column_type = TYPES.MYSQL_TYPE_DOUBLE
                elif pd_types.is_datetime64_any_dtype(field_type):
                    column_type = TYPES.MYSQL_TYPE_DATETIME
            # lightwood checks
            elif field_type == dtype.date:
                column_type = TYPES.MYSQL_TYPE_DATE
            elif field_type == dtype.datetime:
                column_type = TYPES.MYSQL_TYPE_DATETIME
            elif field_type == dtype.float:
                column_type = TYPES.MYSQL_TYPE_DOUBLE
            elif field_type == dtype.integer:
                column_type = TYPES.MYSQL_TYPE_LONG

            result.append(
                {
                    "database": column_record.database or database,
                    #  TODO add 'original_table'
                    "table_name": column_record.table_name,
                    "name": column_record.name,
                    "alias": column_record.alias or column_record.name,
                    # NOTE all work with text-type, but if/when wanted change types to real,
                    # it will need to check all types casts in BinaryResultsetRowPacket
                    "type": column_type,
                }
            )
        return result
