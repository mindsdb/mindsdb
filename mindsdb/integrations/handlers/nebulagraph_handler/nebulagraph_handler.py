from collections import OrderedDict

from nebula3.Config import SessionPoolConfig
from nebula3.gclient.net.SessionPool import SessionPool
from nebula3.data.DataObject import Value, ValueWrapper
from nebula3.data.ResultSet import ResultSet

import pandas as pd

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from mindsdb.utilities import log

from typing import Dict, List, Optional

logger = log.getLogger(__name__)

DEFAULT_PORT = 9669
DEFAULT_SESSION_POOL_SIZE = 10
DEFAULT_USER = "root"
DEFAULT_PASSWORD = "nebula"

cast_as = {
    Value.NVAL: "as_null",
    Value.BVAL: "as_bool",
    Value.IVAL: "as_int",
    Value.FVAL: "as_double",
    Value.SVAL: "as_string",
    Value.LVAL: "as_list",
    Value.UVAL: "as_set",
    Value.MVAL: "as_map",
    Value.TVAL: "as_time",
    Value.DVAL: "as_date",
    Value.DTVAL: "as_datetime",
    Value.VVAL: "as_node",
    Value.EVAL: "as_relationship",
    Value.PVAL: "as_path",
    Value.GGVAL: "as_geography",
    Value.DUVAL: "as_duration",
}


def cast(val: ValueWrapper):
    _type = val._value.getType()
    if _type == Value.__EMPTY__:
        return None
    if _type in cast_as:
        return getattr(val, cast_as[_type])()
    if _type == Value.LVAL:
        return [cast(x) for x in val.as_list()]
    if _type == Value.UVAL:
        return {cast(x) for x in val.as_set()}
    if _type == Value.MVAL:
        return {k: cast(v) for k, v in val.as_map().items()}


class NebulaGraphHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the NebulaGraph statements.
    """

    name = "nebulagraph"

    def __init__(self, name: str, **kwargs):
        """constructor
        Args:
            name (str): The name of the handler
        """
        super().__init__(name)

        connection_data: dict = kwargs.get("connection_data", {})

        assert connection_data, "connection_data is required"

        self.connection: Optional[SessionPool] = None
        self.is_connected: bool = False

        self.host: str = connection_data.get("host") or "127.0.0.1"
        self.port: int = int(connection_data.get("port") or DEFAULT_PORT)
        self.graph_space: str = connection_data.get("graph_space")

        assert self.graph_space, "graph_space is required"

        self.user: str = connection_data.get("user") or DEFAULT_USER
        self.password: str = connection_data.get("password") or DEFAULT_PASSWORD
        self.session_pool_size: int = int(
            connection_data.get("session_pool_size") or DEFAULT_SESSION_POOL_SIZE
        )

    def connect(self) -> SessionPool:
        """
        Connect to the NebulaGraph database
        Returns:
            StatusResponse
        """
        try:
            if self.connection is not None:
                if self.connection.ping((self.host, self.port)):
                    self.is_connected = True
                    return self.connection

            config = SessionPoolConfig()
            config.min_size = 0
            config.max_size = self.session_pool_size
            config.idle_time = 0
            config.wait_timeout = 1000
            config.max_retry_times = 3

            connection = SessionPool(
                self.user, self.password, self.graph_space, [(self.host, self.port)]
            )

            connection.init(config)
            connection.ping((self.host, self.port))
            self.is_connected = True
            self.connection = connection
            return self.connection
        except Exception as e:
            logger.error(
                f"Failed to connect to NebulaGraph {self.host}:{self.port}@{self.graph_space}: {e}"
            )
            raise e

    def disconnect(self):
        """
        Disconnect from the NebulaGraph database
        """
        if self.connection is not None:
            self.connection.close()
            self.connection = None
        self.is_connected = False

    def check_connection(self) -> StatusResponse:
        """
        Check the connection of the NebulaGraph database
        :return: success status and error message if error occurs
        """
        try:
            if not self.is_connected:
                return StatusResponse(False, "Not connected")
            return StatusResponse(self.connection.ping((self.host, self.port)))
        except Exception as e:
            logger.error(
                f"Error connecting to NebulaGraph {self.host}:{self.port}@{self.graph_space}: {e}"
            )
            return StatusResponse(False, str(e))

    def native_query(self, query: str) -> Response:
        """
        Receive NebulaGraph query and runs it
        :param query: The nGQL query to run in NebulaGraph
        :return: returns the records from the current recordset
        """
        try:
            if not self.is_connected:
                self.connect()
            raw_result: ResultSet = self.connection.execute(query)
            columns = raw_result.keys()
            d: Dict[str, List] = {}
            for col_num in range(raw_result.col_size()):
                col_name = columns[col_num]
                col_list = raw_result.column_values(col_name)
                d[col_name] = [cast(x) for x in col_list]
            pd_result = pd.DataFrame(d)
            return Response(RESPONSE_TYPE.TABLE, pd_result)
        except Exception as e:
            logger.error(f"Error executing query {query}: {e}")
            return Response(RESPONSE_TYPE.ERROR, str(e))

    def get_tables(self) -> Response:
        """
        Get the list of tables in the NebulaGraph database, here the tables are TAGS and EDGES
        :return: returns the list of tables
        """
        try:
            if not self.is_connected:
                self.connect()
            query_tags = "SHOW TAGS;"
            query_edges = "SHOW EDGES;"
            tags: Response = self.native_query(query_tags)
            edges: Response = self.native_query(query_edges)
            if (
                tags.type == RESPONSE_TYPE.TABLE
                and edges.type == RESPONSE_TYPE.TABLE
            ):
                tags.data_frame["table_type"] = "TAG"
                edges.data_frame["table_type"] = "EDGE"
                combined_df = pd.concat([tags.data_frame, edges.data_frame])
                return Response(RESPONSE_TYPE.TABLE, combined_df)
            else:
                return Response(
                    RESPONSE_TYPE.ERROR,
                    f"Error getting tables: {tags.error_message} {edges.error_message}",
                )
        except Exception as e:
            logger.error(f"Error getting tables: {e}")
            return Response(RESPONSE_TYPE.ERROR, str(e))

    def get_columns(self, table_name) -> Response:
        """
        Get the list of columns in the NebulaGraph table
        :param table_name: The name of the table
        :return: returns the list of columns
        """
        try:
            if not self.is_connected:
                self.connect()
            tables = self.get_tables()
            if tables.type == RESPONSE_TYPE.TABLE:
                if table_name not in tables.data_frame["Name"].values:
                    return Response(
                        RESPONSE_TYPE.ERROR, f"Table {table_name} not found"
                    )
                table_type = tables.data_frame[
                    tables.data_frame["Name"] == table_name
                ]["table_type"].values[0]
            else:
                return Response(
                    RESPONSE_TYPE.ERROR,
                    f"Error getting columns for table {table_name}: {tables.error_message}",
                )
            query = f"DESC {table_type} {table_name};"

            return self.native_query(query)

        except Exception as e:
            logger.error(f"Error getting columns for table {table_name}: {e}")
            return Response(RESPONSE_TYPE.ERROR, str(e))


connection_args = OrderedDict(
    user={
        "type": ARG_TYPE.STR,
        "description": "The user name used to authenticate with the NebulaGraph.",
        "required": True,
        "label": "User",
    },
    password={
        "type": ARG_TYPE.PWD,
        "description": "The password to authenticate the user with the NebulaGraph.",
        "required": True,
        "label": "Password",
    },
    host={
        "type": ARG_TYPE.STR,
        "description": "The host name or IP address of the NebulaGraph server.",
        "required": True,
        "label": "Host",
    },
    port={
        "type": ARG_TYPE.INT,
        "description": "The TCP/IP port of the NebulaGraph server. Must be an integer. Default is 9669.",
        "required": False,
        "label": "Port",
    },
    graph_space={
        "type": ARG_TYPE.STR,
        "description": "The name of the space to use in NebulaGraph.",
        "required": True,
        "label": "Graph Space",
    },
    session_pool_size={
        "type": ARG_TYPE.INT,
        "description": "The size of the session pool for the NebulaGraph connection.",
        "required": False,
        "label": "Session Pool Size",
    },
)

connection_args_example = OrderedDict(
    host="127.0.0.1",
    port=DEFAULT_PORT,
    user=DEFAULT_USER,
    password=DEFAULT_PASSWORD,
    graph_space="basketballplayer",
)
