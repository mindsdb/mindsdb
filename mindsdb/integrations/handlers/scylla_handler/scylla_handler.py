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

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class ScyllaHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Scylla statements.
    """
    name = 'scylla'

    def __init__(self, name=None, **kwargs):
        super().__init__(name)
        self.parser = parse_sql
        self.connection_args = kwargs.get('connection_data')
        self.session = None
        self.is_connected = False

    def download_secure_bundle(self, url, max_size=10 * 1024 * 1024):
        """
        Downloads the secure bundle from a given URL and stores it in a temporary file.

        :param url: URL of the secure bundle to be downloaded.
        :param max_size: Maximum allowable size of the bundle in bytes. Defaults to 10MB.
        :return: Path to the downloaded secure bundle saved as a temporary file.
        :raises ValueError: If the secure bundle size exceeds the allowed `max_size`.

        TODO:
        - Find a way to periodically clean up or delete the temporary files
        after they have been used to prevent filling up storage over time.
        """
        response = requests.get(url, stream=True, timeout=10)
        response.raise_for_status()

        content_length = int(response.headers.get('content-length', 0))
        if content_length > max_size:
            raise ValueError("Secure bundle is larger than the allowed size!")

        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            size_downloaded = 0
            for chunk in response.iter_content(chunk_size=8192):
                size_downloaded += len(chunk)
                if size_downloaded > max_size:
                    raise ValueError("Secure bundle is larger than the allowed size!")
                temp_file.write(chunk)
        return temp_file.name

    def connect(self):
        """
        Handles the connection to a Scylla keystore.
        """
        if self.is_connected is True:
            return self.session

        auth_provider = None
        if any(key in self.connection_args for key in ('user', 'password')):
            if all(key in self.connection_args for key in ('user', 'password')):
                auth_provider = PlainTextAuthProvider(
                    username=self.connection_args['user'], password=self.connection_args['password']
                )
            else:
                raise ValueError("If authentication is required, both 'user' and 'password' must be provided!")

        connection_props = {
            'auth_provider': auth_provider
        }
        connection_props['protocol_version'] = self.connection_args.get('protocol_version', 4)
        secure_connect_bundle = self.connection_args.get('secure_connect_bundle')

        if secure_connect_bundle:
            # Check if the secure bundle is a URL
            if secure_connect_bundle.startswith(('http://', 'https://')):
                secure_connect_bundle = self.download_secure_bundle(secure_connect_bundle)
            connection_props['cloud'] = {
                'secure_connect_bundle': secure_connect_bundle
            }
        else:
            connection_props['contact_points'] = [self.connection_args['host']]
            connection_props['port'] = int(self.connection_args['port'])

        cluster = Cluster(**connection_props)
        session = cluster.connect(self.connection_args.get('keyspace'))

        self.is_connected = True
        self.session = session
        return self.session

    def check_connection(self) -> StatusResponse:
        """
        Check the connection of the Scylla database
        :return: success status and error message if error occurs
        """
        response = StatusResponse(False)

        try:
            session = self.connect()
            # TODO: change the healthcheck
            session.execute('SELECT release_version FROM system.local').one()
            response.success = True
        except Exception as e:
            logger.error(f'Error connecting to Scylla {self.connection_args["keyspace"]}, {e}!')
            response.error_message = e

        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

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
                response = Response(
                    RESPONSE_TYPE.TABLE,
                    pd.DataFrame(
                        resp
                    )
                )
            else:
                response = Response(RESPONSE_TYPE.OK)
        except Exception as e:
            logger.error(f'Error running query: {query} on {self.connection_args["keyspace"]}!')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )
        return response

    def query(self, query: ASTNode) -> Response:
        """
        Retrieve the data from the SQL statement.
        """

        # remove table alias because Cassandra Query Language doesn't support it
        if isinstance(query, ast.Select):
            if isinstance(query.from_table, ast.Identifier) and query.from_table.alias is not None:
                query.from_table.alias = None

            # remove table name from fields
            table_name = query.from_table.parts[-1]

            for target in query.targets:
                if isinstance(target, ast.Identifier):
                    if target.parts[0] == table_name:
                        target.parts.pop(0)

        renderer = SqlalchemyRender('mysql')
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        Get a list with all of the tabels in MySQL
        """
        q = "DESCRIBE TABLES;"
        result = self.native_query(q)
        df = result.data_frame
        result.data_frame = df.rename(columns={df.columns[0]: 'table_name'})
        return result

    def get_columns(self, table_name) -> Response:
        """
        Show details about the table
        """
        q = f"DESCRIBE {table_name};"
        result = self.native_query(q)
        return result
