from collections import OrderedDict

from nebula3.Config import SessionPoolConfig
from nebula3.gclient.net import SessionPool
import pandas as pd

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from mindsdb.utilities import log

from typing import Dict, List, Optional, Any

logger = log.getLogger(__name__)


class NebulaGraphHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the NebulaGraph statements.
    """

    name = "nebulagraph"

    def __init__(self, name: str, connection_data: Optional[dict]):
        """constructor
        Args:
            name (str): The name of the handler
            connection_data (dict): The connection data for the handler
        """
        super().__init__(name)
        self.connection_data: Dict[str, Any] = connection_data
        self.session_pool = None

        self.host = self.connection_data.get("host")
        self.port = self.connection_data.get("port")
        self.graph_space = self.connection_data.get("graph_space")
        self.user = self.connection_data.get("user")
        self.password = self.connection_data.get("password")
        self.session_pool_size = self.connection_data.get("session_pool_size")

    def connect(self) -> StatusResponse:
        """
        Connect to the NebulaGraph database
        Returns:
            StatusResponse
        """
        if self.session_pool is not None and self.session_pool.ping(
            (self.host, self.port)
        ):
            return StatusResponse(True)

        config = SessionPoolConfig()
        config.min_size = 0
        config.max_size = self.session_pool_size
        config.idle_time = 0
        config.wait_timeout = 1000
        config.max_retry_times = 3

        self.session_pool = SessionPool(
            self.user, self.password, self.graph_space, [(self.host, self.port)]
        )

        if not self.session_pool.init(config):
            raise Exception("Failed to initialize session pool")

        return StatusResponse(self.session_pool.ping((self.host, self.port)))

    def disconnect(self):
        """
        Disconnect from the NebulaGraph database
        """
        if self.session_pool is not None:
            self.session_pool.close()
            self.session_pool = None

    def check_connection(self) -> StatusResponse:
        """
        Check the connection of the NebulaGraph database
        :return: success status and error message if error occurs
        """
        try:
            if self.session_pool is None:
                return StatusResponse(False, "Not connected")
            return StatusResponse(self.session_pool.ping((self.host, self.port)))
        except Exception as e:
            logger.error(
                f"Error connecting to NebulaGraph {self.host}:{self.port}@{self.graph_space}: {e}"
            )
            return StatusResponse(False, str(e))

    def native_query(self, query) -> Response:
        """
        Receive NebulaGraph query and runs it
        :param query: The nGQL query to run in NebulaGraph
        :return: returns the records from the current recordset
        """
        try:
            self.connect()
            raw_result = self.session_pool.execute(query)
            columns = raw_result.keys()
            d: Dict[str, List] = {}
            for col_num in range(raw_result.col_size()):
                col_name = columns[col_num]
                col_list = raw_result.column_values(col_name)
                d[col_name] = [x.cast() for x in col_list]
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
            query = f"SHOW {table_type} {table_name};"

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
        "default": 9669,
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
        "default": 10,
        "label": "Session Pool Size",
    },
)
