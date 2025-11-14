import tempfile

import pandas as pd
import requests

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

    def download_secure_bundle(self, url, max_size=10 * 1024 * 1024):
        """
        Downloads the secure bundle from a given URL and stores it in a temporary file.

        :param url: URL of the secure bundle to be downloaded.
        :param max_size: Maximum allowable size of the bundle in bytes. Defaults to 10MB.
        :return: Path to the downloaded secure bundle saved as a temporary file.
        :raises ValueError: If the secure bundle size exceeds the allowed `max_size`.
        """
        response = requests.get(url, stream=True, timeout=10)
        response.raise_for_status()

        content_length = int(response.headers.get("content-length", 0))
        if content_length > max_size:
            raise ValueError("Secure bundle is larger than the allowed size!")

        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            size_downloaded = 0
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    temp_file.write(chunk)
                    size_downloaded += len(chunk)
                    if size_downloaded > max_size:
                        raise ValueError(
                            "Secure bundle is larger than the allowed size!"
                        )
            return temp_file.name

    def connect(self):
        """
        Handles the connection to a Cassandra keystore.
        """
        if self.is_connected is True:
            return self.session
        auth_provider = None
        if any(key in self.connection_args for key in ("user", "password")):
            if all(key in self.connection_args for key in ("user", "password")):
                auth_provider = PlainTextAuthProvider(
                    username=self.connection_args["user"],
                    password=self.connection_args["password"],
                )
            else:
                raise ValueError(
                    "If authentication is required, both 'user' and 'password' must be provided!"
                )

        connection_props = {"auth_provider": auth_provider}
        connection_props["protocol_version"] = self.connection_args.get(
            "protocol_version", 4
        )
        secure_connect_bundle = self.connection_args.get("secure_connect_bundle")

        if secure_connect_bundle:
            if secure_connect_bundle.startswith(("http://", "https://")):
                secure_connect_bundle = self.download_secure_bundle(
                    secure_connect_bundle
                )
            connection_props["cloud"] = {"secure_connect_bundle": secure_connect_bundle}
        else:
            connection_props["contact_points"] = [self.connection_args["host"]]
            connection_props["port"] = int(self.connection_args["port"])

        cluster = Cluster(**connection_props)
        session = cluster.connect(self.connection_args.get("keyspace"))

        self.is_connected = True
        self.session = session
        return self.session

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

        query = """
            SELECT
                keyspace_name AS table_schema,
                table_name
            FROM system_schema.tables
        """
        if self.keyspace:
            query += " WHERE keyspace_name = '{}';".format(self.keyspace)

        result = self.native_query(query)
        # Filter out system keyspaces in Python
        if result.type == RESPONSE_TYPE.TABLE:
            df = result.data_frame
            df = df[~df["table_schema"].str.contains("system", case=False, na=False)]
            # add table_type column
            df["table_type"] = "BASE TABLE"
            result.data_frame = df

        views_query = """
            SELECT
                keyspace_name AS table_schema,
                table_name
            FROM system_schema.views
        """
        if self.keyspace:
            views_query += " WHERE keyspace_name = '{}';".format(self.keyspace)

        views_result = self.native_query(views_query)
        if views_result.type == RESPONSE_TYPE.TABLE:
            views_df = views_result.data_frame
            views_df = views_df[
                ~views_df["table_schema"].str.contains("system", case=False, na=False)
            ]
            views_df["table_type"] = "VIEW"
            # concatenate tables and views
            result.data_frame = pd.concat(
                [result.data_frame, views_df], ignore_index=True
            )

        return result

    def get_columns(self, table_name: str) -> Response:
        """
        Return columns for a single table, minimal shape:
        - column_name
        - type
        """
        q = f"DESCRIBE {table_name};"
        return self.native_query(q)

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
