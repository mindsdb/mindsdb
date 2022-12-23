import re
from typing import Dict
import pandas as pd
from pyhive import (trino, sqlalchemy_trino)
from mindsdb_sql import parse_sql, ASTNode
from trino.auth import KerberosAuthentication, BasicAuthentication
from trino.dbapi import connect
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from .trino_config_provider import TrinoConfigProvider


class TrinoHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Trino statements

    kerberos is not implemented yet
    """

    name = 'trino'

    def __init__(self, name, connection_data, **kwargs):
        super().__init__(name)
        self.parser = parse_sql
        self.connection_data = connection_data
        '''
        service_name = kwargs.get('service_name')
        self.config_file_name = kwargs.get('config_file_name')
        self.trino_config_provider = TrinoConfigProvider(config_file_name=self.config_file_name)
        self.kerberos_config = self.trino_config_provider.get_trino_kerberos_config()
        self.http_scheme = self.kerberos_config['http_scheme']
        self.dialect = self.kerberos_config['dialect']
        config = self.kerberos_config['config']
        hostname_override = self.kerberos_config['hostname_override']
        principal = f"{kwargs.get('user')}@{hostname_override}"
        ca_bundle = self.kerberos_config['ca_bundle']
        self.auth_config = KerberosAuthentication(config=config,
                                                  service_name=service_name,
                                                  principal=principal,
                                                  ca_bundle=ca_bundle,
                                                  hostname_override=hostname_override)
        '''
        self.connection = None
        self.is_connected = False
        self.with_clause = ""

    def connect(self):
        """"
        Handles the connection to a Trino instance.
        """
        if self.is_connected is True:
            return self.connection

        # option configuration
        http_scheme='http'
        auth=None
        auth_config=None
        password=None

        if 'auth' in self.connection_data:
            auth=self.connection_data['auth']
        if 'password' in self.connection_data:
            password=self.connection_data['password']
        if 'http_scheme' in self.connection_data:
           http_scheme=self.connection_data['http_scheme']
        if 'with' in self.connection_data:
           self.with_clause=self.connection_data['with']

        if password and auth=='kerberos':
            raise Exception("Kerberos authorization doesn't support password.")
        elif password:
            auth_config=BasicAuthentication(self.connection_data['user'], password)

        if auth:
            conn = connect(
                host=self.connection_data['host'],
                port=self.connection_data['port'],
                user=self.connection_data['user'],
                catalog=self.connection_data['catalog'],
                schema=self.connection_data['schema'],
                http_scheme=http_scheme,
                auth=auth_config)
        else:
            conn = connect(
                host=self.connection_data['host'],
                port=self.connection_data['port'],
                user=self.connection_data['user'],
                catalog=self.connection_data['catalog'],
                schema=self.connection_data['schema'])

        self.is_connected = True
        self.connection = conn
        return conn

    def check_connection(self) -> StatusResponse:
        """
        Check the connection of the Trino instance
        :return: success status and error message if error occurs
        """
        response = StatusResponse(False)

        try:
            connection = self.connect()
            cur = connection.cursor()
            cur.execute("SELECT 1")
            response.success = True
        except Exception as e:
            log.logger.error(f'Error connecting to Trino {self.connection_data["schema"]}, {e}!')
            response.error_message = str(e)

        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def native_query(self, query: str) -> Response:
        """
        Receive SQL query and runs it
        :param query: The SQL query to run in Trino
        :return: returns the records from the current recordset
        """
        try:
            connection = self.connect()
            cur = connection.cursor()
            result = cur.execute(query)
            if result:
                response = Response(
                    RESPONSE_TYPE.TABLE,
                    data_frame=pd.DataFrame(
                        result,
                        columns=[x[0] for x in cur.description]
                    )
                )
            else:
                response = Response(RESPONSE_TYPE.OK)
            connection.commit()
        except Exception as e:
            log.logger.error(f'Error connecting to Trino {self.connection_data["schema"]}, {e}!')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )
        return response

    def query(self, query: ASTNode) -> Response:
        # Utilize trino dialect from sqlalchemy
        # implement WITH clause as default for all table
        # in future, this behavior should be changed to support more detail
        # level
        # also, for simple the current implement is using rendered query string
        # another method that directly manipulate ASTNOde is prefered
        renderer = SqlalchemyRender(sqlalchemy_trino.TrinoDialect)
        query_str = renderer.get_string(query, with_failback=True)
        modified_query_str = re.sub(
            "(?is)(CREATE.+TABLE.+\(.*\))",
            f"\\1 {self.with_clause}",
            query_str
        )
        return self.native_query(modified_query_str)

    def get_tables(self) -> Response:
        """
        List all tables in Trino
        :return: list of all tables
        """
        query = "SHOW TABLES"
        response = self.native_query(query)
        df = response.data_frame
        response.data_frame = df.rename(columns={df.columns[0]: 'table_name'})
        return response

    def get_columns(self, table_name: str) -> Dict:
        query = f'DESCRIBE "{table_name}"'
        response = self.native_query(query)
        return response
