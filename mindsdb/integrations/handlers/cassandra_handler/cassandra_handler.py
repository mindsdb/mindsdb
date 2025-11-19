import tempfile

import pandas as pd

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.util import Date

from mindsdb_sql_parser import parse_sql
from mindsdb_sql_parser.ast.base import ASTNode
from mindsdb_sql_parser import ast
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender

from mindsdb.integrations.libs.base import MetaDatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class CassandraHandler(MetaDatabaseHandler):
    """
    This handler handles connection and execution of the Cassandra statements.
    """

    name = "cassandra"

    def __init__(self, name, **kwargs):
        super().__init__(name)
        connection_data = kwargs["connection_data"]
        self.parser = parse_sql
        self.session = None
        self.is_connected = False
        self.connection_args = connection_data
        self.keyspace = connection_data.get("keyspace")

    def connect(self):
        """
        Establishes connection to Cassandra.
        """

        if self.is_connected and self.session:
            return self.session

        if "host" not in self.connection_args:
            raise ValueError("'host' is required for Cassandra connection")
        if "port" not in self.connection_args:
            raise ValueError("'port' is required for Cassandra connection")

        # Setup authentication
        auth_provider = None
        if any(key in self.connection_args for key in ("user", "password")):
            if all(key in self.connection_args for key in ("user", "password")):
                auth_provider = PlainTextAuthProvider(
                    username=self.connection_args["user"],
                    password=self.connection_args["password"],
                )
            else:
                raise ValueError(
                    "Both 'user' and 'password' must be provided for authentication"
                )

        # Setup connection
        connection_props = {
            "auth_provider": auth_provider,
            "contact_points": [self.connection_args["host"]],
            "port": int(self.connection_args["port"]),
            "protocol_version": self.connection_args.get("protocol_version", 4),
        }

        try:
            self.cluster = Cluster(**connection_props)
            self.session = self.cluster.connect(self.keyspace)
            self.is_connected = True
            return self.session
        except Exception as e:
            self.is_connected = False
            logger.error(f"Failed to connect to Cassandra: {e}")
            raise

    def check_connection(self) -> StatusResponse:
        """
        Check the connection of the Cassandra database
        :return: success status and error message if error occurs
        """
        try:
            session = self.connect()
            session.execute("SELECT release_version FROM system.local")
            return StatusResponse(success=True)
        except Exception as e:
            return StatusResponse(success=False, error_message=str(e))

    def prepare_response(self, resp):
        # replace cassandra types
        data = []
        for row in resp:
            row2 = {}
            for k, v in row._asdict().items():
                if isinstance(v, Date):
                    v = v.date()
                row2[k] = v
            data.append(row2)
        return data

    def native_query(self, query: str) -> Response:
        """
        Receive SQL query and runs it
        :param query: The SQL query to run in MySQL
        :return: returns the records from the current recordset
        """
        session = self.connect()
        try:
            resp = session.execute(query).all()
            resp = self.prepare_response(resp)
            if resp:
                response = Response(RESPONSE_TYPE.TABLE, pd.DataFrame(resp))
            else:
                response = Response(RESPONSE_TYPE.OK)
        except Exception as e:
            logger.error(f"Error running query: {query} on {self.keyspace}!")
            response = Response(RESPONSE_TYPE.ERROR, error_message=str(e))
        return response

    def query(self, query: ASTNode) -> Response:
        """
        Retrieve the data from the SQL statement.
        """

        if isinstance(query, ast.Select):
            if (
                isinstance(query.from_table, ast.Identifier)
                and query.from_table.alias is not None
            ):
                query.from_table.alias = None

            # remove table name from fields
            table_name = query.from_table.parts[-1]

            for target in query.targets:
                if isinstance(target, ast.Identifier):
                    if target.parts[0] == table_name:
                        target.parts.pop(0)

        renderer = SqlalchemyRender("mysql")
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        Get a list of all tables in the current keyspace.
        """
        try:
            self.connect()

            keyspace_metadata = self.cluster.metadata.keyspaces.get(self.keyspace)

            if not keyspace_metadata:
                return Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=f"Keyspace {self.keyspace} not found",
                )

            data = []

            for table_name, table_meta in keyspace_metadata.tables.items():
                data.append(
                    {
                        "table_schema": self.keyspace,
                        "table_name": table_name,
                        "table_type": "BASE TABLE",
                    }
                )
            for view_name, view_meta in keyspace_metadata.views.items():
                data.append(
                    {
                        "table_schema": self.keyspace,
                        "table_name": view_name,
                        "table_type": "VIEW",
                    }
                )
            df = pd.DataFrame(data)
            if not df.empty:
                df = df[
                    ~df["table_schema"].str.contains("system", case=False, na=False)
                ]
                df = df.sort_values("table_name").reset_index(drop=True)

            return Response(RESPONSE_TYPE.TABLE, df)

        except Exception as e:
            logger.error(f"Error getting tables: {e}")
            return Response(RESPONSE_TYPE.ERROR, error_message=str(e))

    def get_columns(self, table_name: str) -> Response:
        """
        Return columns for a single table, minimal shape:
        - column_name
        - type
        """
        try:
            self.connect()

            keyspace_metadata = self.cluster.metadata.keyspaces.get(self.keyspace)

            if not keyspace_metadata:
                return Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=f"Keyspace {self.keyspace} not found",
                )

            table_metadata = keyspace_metadata.tables.get(table_name)

            if not table_metadata:
                return Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=f"Table {table_name} not found in keyspace {self.keyspace}",
                )

            data = []
            for col_name, col_meta in table_metadata.columns.items():
                data.append(
                    {
                        "column_name": col_name,
                        "type": str(col_meta.cql_type),
                    }
                )

            df = pd.DataFrame(data)
            return Response(RESPONSE_TYPE.TABLE, df)

        except Exception as e:
            logger.error(f"Error getting columns for table {table_name}: {e}")
            return Response(RESPONSE_TYPE.ERROR, error_message=str(e))

    # Metadata

    def meta_get_tables(self) -> Response:
        """
        Get a list with all of the tables in Cassandra
        """
        q = "SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{}';".format(
            self.connection_args["keyspace"]
        )
        return self.native_query(q)

    def meta_get_columns(self, table_name) -> Response:
        """
        Show details about the table
        """
        q = "SELECT column_name, kind, position, type FROM system_schema.columns WHERE keyspace_name = '{}' AND table_name = '{}';".format(
            self.connection_args["keyspace"], table_name
        )
        return self.native_query(q)

    def meta_get_column_statistics(self, table_name, column_name) -> Response:
        """
        Show basic statistics about a column in a table
        """
        # Cassandra does not maintain column statistics like some other databases.
        # Therefore, we will return an empty response or a message indicating that
        # TODO: Implement column statistics
        return Response(RESPONSE_TYPE.OK, pd.DataFrame())

    def meta_get_primary_keys(self, table_name) -> Response:
        """
        Get the primary keys of a table
        """
        q = "SELECT column_name FROM system_schema.columns WHERE keyspace_name = '{}' AND table_name = '{}' AND kind = 'partition_key';".format(
            self.connection_args["keyspace"], table_name
        )
        return self.native_query(q)

    def meta_get_foreign_keys(self, table_name) -> Response:
        """
        Get the foreign keys of a table
        """
        # Cassandra does not support foreign keys.
        return Response(RESPONSE_TYPE.OK, pd.DataFrame())
