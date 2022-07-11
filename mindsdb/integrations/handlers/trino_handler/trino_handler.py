from typing import List, Dict

import pandas as pd
from mindsdb_sql import parse_sql, ASTNode
from trino.auth import KerberosAuthentication
from trino.dbapi import connect

from mindsdb.integrations.libs.base_handler import DatabaseHandler
from mindsdb.utilities.log import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from .trino_config_provider import TrinoConfigProvider


class TrinoHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Trino statements
    using kerberos authentication
    """

    name = 'trino'

    def __init__(self, name, **kwargs):
        super().__init__(name)
        self.parser = parse_sql
        self.connection_args = kwargs
        self.user = kwargs.get('user')
        self.password = kwargs.get('password')
        self.host = kwargs.get('host')
        self.port = kwargs.get('port')
        self.catalog = kwargs.get('catalog')
        self.schema = kwargs.get('schema')
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
        self.connection = None
        self.is_connected = False

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self, **kwargs):
        if self.is_connected is True:
            return self.connection

        self.connection = self.__connect()
        self.is_connected = True
        return self.connection

    def disconnect(self):
        if self.is_connected is True:
            self.disconnect()

    def __connect(self):
        """"
        Handles the connection to a Trino instance.
        """
        conn = connect(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            schema=self.schema,
            http_scheme=self.http_scheme,
            auth=self.auth_config
        )
        return conn

    def check_connection(self) -> StatusResponse:
        """
        Check the connection of the Trino instance
        :return: success status and error message if error occurs
        """
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            connection = self.connect()
            cur = connection.cursor()
            cur.execute("SELECT * FROM system.runtime.nodes")
            rows = cur.fetchall()
            print('trino nodes: ', rows)
            response.success = True
        except Exception as e:
            log.error(f'Error connecting to Trino {self.schema}, {e}!')
            response.error_message = str(e)
        finally:
            cur.close()
            if need_to_close is True:
                connection.close()
        return response

    def native_query(self, query: str) -> Response:
        """
        Receive SQL query and runs it
        :param query: The SQL query to run in Trino
        :return: returns the records from the current recordset
        """
        need_to_close = self.is_connected is False

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
            log.error(f'Error connecting to Trino {self.schema}, {e}!')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )
        finally:
            cur.close()
            if need_to_close is True:
                self.disconnect()
            connection.rollback()
        return response

    # TODO: complete the implementations
    def query(self, query: ASTNode) -> dict:
        pass

    def get_tables(self) -> Response:
        """
        List all tables in Trino
        :return: list of all tables
        """
        query = "SHOW TABLES"
        response = self.native_query(query)
        return response

    def get_columns(self, table_name: str) -> Dict:
        query = f'DESCRIBE "{table_name}"'
        response = self.native_query(query)
        return response
