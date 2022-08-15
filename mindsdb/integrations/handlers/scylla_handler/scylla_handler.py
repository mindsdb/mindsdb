import os
from mindsdb.integrations.libs.base_handler import DatabaseHandler
from mindsdb_sql import parse_sql
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.utilities.log import log
import pandas as pd
from mindsdb_sql.parser.ast.base import ASTNode


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

    def connect(self):
        """
        Handles the connection to a Scylla keystore.
        """
        if self.is_connected is True:
            return self.session

        auth_provider = PlainTextAuthProvider(
            username=self.connection_args['user'], password=self.connection_args['password']
        )

        connection_props = {
            'auth_provider': auth_provider
        }

        if self.connection_args['protocol_version'] is not None:
            connection_props['protocol_version'] = self.connection_args['protocol_version']

        secure_connect_bundle = self.connection_args.get('secure_connect_bundle')

        if secure_connect_bundle is not None:
            if os.path.isfile(secure_connect_bundle) is False:
                raise Exception("Secure_connect_bundle' must be path to the file")
            connection_props['cloud'] = {
                'secure_connect_bundle': secure_connect_bundle
            }
        else:
            connection_props['contact_points'] = [self.connection_args['host']]
            connection_props['port'] = int(self.connection_args['port'])

        cluster = Cluster(**connection_props)
        session = cluster.connect(self.connection_args['keyspace'])

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
            log.error(f'Error connecting to Scylla {self.connection_args["keyspace"]}, {e}!')
            response.error_message = e

        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def native_query(self, query: str) -> Response:
        """
        Receive SQL query and runs it
        :param query: The SQL query to run in MySQL
        :return: returns the records from the current recordset
        """
        session = self.connect()
        try:
            resp = session.execute(query).all()
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
            log.error(f'Error running query: {query} on {self.connection_args["keyspace"]}!')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )
        return response

    def query(self, query: ASTNode) -> Response:
        """
        Retrieve the data from the SQL statement.
        """
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
