import re
from typing import Dict
import pandas as pd
from pyhive import (trino, sqlalchemy_trino)
from mindsdb_sql import parse_sql, ASTNode
from trino.auth import KerberosAuthentication, BasicAuthentication, JWTAuthentication, OAuth2Authentication, CertificateAuthentication
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

    def _generate_connection_arguments(self):
        """
        Generates arguments that need to create a connection
        """
        # required config passed in trino connect method
        required_config = {
            'host': self.connection_data.get('host', ''),
            'port': self.connection_data.get('port', ''),
            'user': self.connection_data.get('user', ''),
        }
        # TODO: password in required here? (def not for kerberos)
        # optional config passed in trino connect method
        optional_config = {'http_scheme': 'http'}
        if 'catalog' in self.connection_data:
            optional_config['catalog'] = self.connection_data['catalog']
        if 'schema' in self.connection_data:
            optional_config['schema'] = self.connection_data['schema']
        if 'http_scheme' in self.connection_data:
            optional_config['http_scheme'] = self.connection_data['http_scheme']
        # create a config object
        config =  {
            **required_config,
            **optional_config
        }
        # generate auth config
        if 'auth' in self.connection_data:
            auth_method_params = []
            # NOTE: the order of parameters in required and optional corresponds to the init methods in authenticator class
            auth_method = { 
                'basic': {
                    'call': BasicAuthentication,
                    'required': ['user', 'password'],
                    'optional': []
                },
                'jwt': {
                    'call': JWTAuthentication,
                    'required': ['jwt_token'],
                    'optional': []
                },
                'cert': {
                    'call': CertificateAuthentication,
                    'required': ['cert_path', 'key_cert'],
                    'optional': []
                },
                'oauth2': { # TODO: will this work?
                    'call': OAuth2Authentication,
                    'required': [],
                    'optional': []
                },
                'kerberos': {
                    'call': KerberosAuthentication,
                    'required': [],
                    'optional': ['config', 'service_name', 'mutual_authentication', 'force_preemptive', 'hostname_override', 'sanitize_mutual_error_response', 'principal', 'delegate', 'ca_bundle']
                }
            }
            # required config passed in authenticator class
            for req_param in auth_method[self.connection_data['auth']]['required']:
                auth_method_params.append(self.connection_data[req_param])
            # optional config passed in authenticator class
            for opt_param in auth_method[self.connection_data['auth']]['optional']:
                auth_method_params.append(self.connection_data[opt_param])
            config['auth'] = auth_method[self.connection_data.get['auth']]['call'](*auth_method_params)
        return config

    def connect(self):
        """"
        Handles the connection to a Trino instance.
        """
        if self.is_connected is True:
            return self.connection

        config = self._generate_connection_arguments()

        if 'with' in self.connection_data:
           self.with_clause=self.connection_data['with']
        # TODO: raise an error if password with kerberos?

        self.connection = connect(**config)
        self.is_connected = True

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
            if result and cur.description:
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
