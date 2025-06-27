from typing import Union

from mindsdb_sql_parser import parse_sql
from mindsdb.api.executor.planner import utils as planner_utils

from numpy import dtype as np_dtype
from pandas.api import types as pd_types

from mindsdb.api.executor.sql_query import SQLQuery
from mindsdb.api.executor.sql_query.result_set import Column
from mindsdb.api.mysql.mysql_proxy.utilities.lightwood_dtype import dtype
from mindsdb.api.executor.command_executor import ExecuteCommands
from mindsdb.api.mysql.mysql_proxy.utilities import SqlApiException
from mindsdb.api.postgres.postgres_proxy.postgres_packets.postgres_fields import POSTGRES_TYPES
from mindsdb.utilities import log


class Executor:
    def __init__(self, session, proxy_server, charset=None):
        self.session = session
        self.server = proxy_server

        self.logger = log.getLogger(__name__)
        self.charset = charset or "utf8"
        self.query = None
        self.columns = []
        self.params = []
        self.data = None
        self.server_status = None
        self.state_track = None
        self.is_executed = False

        self.sql = ""
        self.sql_lower = ""

        self.command_executor = ExecuteCommands(self.session)

    def parse(self, sql: Union[str, bytes]):
        self.logger.info("%s.parse: sql - %s", self.__class__.__name__, sql)
        if type(sql) == bytes:
            sql = sql.decode(encoding=self.charset)
        self.sql = sql
        sql_lower = self.sql.lower()
        self.sql_lower = sql_lower.replace("`", "")

        try:
            self.query = parse_sql(sql)
        except Exception as mdb_error:
            # not all statements are parsed by parse_sql
            self.logger.warning('Failed to parse SQL query')
            self.logger.debug(f'Query that cannot be parsed: {sql}')

            raise SqlApiException(
                f"The SQL statement cannot be parsed - {sql}: {mdb_error}"
            ) from mdb_error

    def stmt_execute(self, param_values):
        if self.is_executed:
            return

        # fill params
        self.query = planner_utils.fill_query_params(self.query, param_values)

        # execute query
        self.do_execute()

    def execute_external(self, sql):
        return None

    def query_execute(self, sql):
        self.logger.info("%s.query_execute: sql - %s", self.__class__.__name__, sql)
        resp = self.execute_external(sql)
        if resp is not None:
            # is already executed
            self.is_executed = True
            return

        self.parse(sql)
        self.do_execute()

    def do_execute(self):
        self.logger.info("%s.do_execute", self.__class__.__name__)
        # it can be already run at prepare state
        if self.is_executed:
            return

        ret = self.command_executor.execute_command(self.query)

        self.is_executed = True

        if ret.data is not None:
            self.data = ret.data.to_lists()
            self.columns = ret.data.columns

        self.state_track = ret.state_track

    def _to_json(self):
        params = {
            "columns": self.to_postgres_columns(self.columns),
            "params": self.to_postgres_columns(self.params),
            "data": self.data,
            "state_track": self.state_track,
            "server_status": self.server_status,
            "is_executed": self.is_executed,
            "session": self.session.to_json()

        }
        return params

    def to_postgres_columns(self, columns):

        result = []

        database = (
            None if self.session.database == "" else self.session.database.lower()
        )
        for column_record in columns:

            field_type = column_record.type

            column_type = POSTGRES_TYPES.VARCHAR
            # is already in mysql protocol type?
            if isinstance(field_type, int):
                column_type = POSTGRES_TYPES.INT
            # pandas checks
            elif isinstance(field_type, np_dtype):
                if pd_types.is_integer_dtype(field_type):
                    column_type = POSTGRES_TYPES.LONG
                elif pd_types.is_numeric_dtype(field_type):
                    column_type = POSTGRES_TYPES.DOUBLE
                elif pd_types.is_datetime64_any_dtype(field_type):
                    column_type = POSTGRES_TYPES.DATETIME
            # lightwood checks
            elif field_type == dtype.date:
                column_type = POSTGRES_TYPES.DATE
            elif field_type == dtype.datetime:
                column_type = POSTGRES_TYPES.DATETIME
            elif field_type == dtype.float:
                column_type = POSTGRES_TYPES.DOUBLE
            elif field_type == dtype.integer:
                column_type = POSTGRES_TYPES.LONG

            if "()" in column_record.alias:
                column_record.alias = column_record.alias.strip("()")
            if "()" in column_record.name:
                column_record.name = column_record.name.strip("()")

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

    def change_default_db(self, new_db):
        self.command_executor.change_default_db(new_db)

    def stmt_prepare(self, sql):
        # Returns True if ready for query afterwards.
        # Check if execute external here
        self.parse(sql)
        params = planner_utils.get_query_params(self.query)
        if len(params) == 0:
            pass
        #    self.do_execute()
        #    return True
        else:
            # plan query

            sqlquery = SQLQuery(self.query, session=self.session, execute=False)

            sqlquery.prepare_query()

            self.params = [
                Column(
                    name=p.value,
                    alias=p.value,
                    type="str",
                )
                for p in params
            ]
            self.columns = sqlquery.columns_list
