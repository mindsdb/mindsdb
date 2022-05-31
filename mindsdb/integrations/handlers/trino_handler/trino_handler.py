from typing import List, Optional, Dict

import pandas as pd
from mindsdb_sql import parse_sql, ASTNode
from trino.auth import KerberosAuthentication
from trino.dbapi import connect

from mindsdb.api.mysql.mysql_proxy.mysql_proxy import RESPONSE_TYPE
from mindsdb.integrations.libs.base_handler import DatabaseHandler
from mindsdb.integrations.trino_handler.trino_config_provider import TrinoConfigProvider
from mindsdb.utilities.log import log


class TrinoHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Trino statements
    using kerberos authentication
    """

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

    def connect(self, **kwargs) -> Dict[str, int]:
        conn_status = self.check_status()
        if conn_status.get('success'):
            return {'status': 200}
        return {'status': 503,
                'error': conn_status.get('error')}

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

    def check_status(self):
        """
        Check the connection of the Trino instance
        :return: success status and error message if error occurs
        """
        status = {
            'success': False
        }
        try:
            conn = self.__connect()
            cur = conn.cursor()
            cur.execute("SELECT * FROM system.runtime.nodes")
            rows = cur.fetchall()
            print('trino nodes: ', rows)
            status['success'] = True
        except Exception as e:
            log.error(f'Error connecting to Trino {self.schema}, {e}!')
            status['error'] = e
        finally:
            cur.close()
            conn.close()
        return status

    def native_query(self, query):
        """
        Receive SQL query and runs it
        :param query: The SQL query to run in Trino
        :return: returns the records from the current recordset
        """
        try:
            conn = self.__connect()
            cur = conn.cursor()
            result = cur.execute(query)
            if result:
                response = {
                    'type': RESPONSE_TYPE.TABLE,
                    'data_frame': pd.DataFrame(
                        result,
                        columns=[x[0] for x in cur.description]
                    )
                }
            else:
                response = {
                    'type': RESPONSE_TYPE.OK
                }
        except Exception as e:
            log.error(f'Error connecting to Trino {self.schema}, {e}!')
            response = {
                'type': RESPONSE_TYPE.ERROR,
                'error_code': 0,
                'error_message': str(e)
            }
        finally:
            cur.close()
            conn.close()
        return response

    def get_tables(self) -> List:
        """
        List all tables in Trino
        :return: list of all tables
        """
        query = "SHOW TABLES"
        res_tables = self.native_query(query)
        tables = res_tables.get('data_frame')['Table'].tolist()
        log.info(f'tables: {tables}')
        return tables

    def describe_table(self, table_name: str) -> Dict:
        query = f'DESCRIBE "{table_name}"'
        res = self.native_query(query)
        return res

    # TODO: complete the implementations
    def query(self, query: ASTNode) -> dict:
        pass

    def join(self, stmt, data_handler, into: Optional[str]) -> pd.DataFrame:
        pass

    def get_views(self) -> List:
        pass

    def select_into(self, table: str, dataframe: pd.DataFrame):
        pass
