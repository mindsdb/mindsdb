from typing import Optional
from contextlib import closing

import pandas as pd
import mysql.connector
from sqlalchemy import create_engine

from mindsdb_sql import parse_sql
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.integrations.libs.base_handler import DatabaseHandler
from mindsdb.utilities.log import log
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE


class MySQLHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the MySQL statements.
    """

    name = 'mysql'

    def __init__(self, name, **kwargs):
        super().__init__(name)
        self.mysql_url = None
        self.parser = parse_sql
        self.dialect = 'mysql'
        connection_data = kwargs.get('connection_data')
        self.host = connection_data.get('host')
        self.port = connection_data.get('port')
        self.user = connection_data.get('user')
        self.database = connection_data.get('database')  # todo: may want a method to change active DB
        self.password = connection_data.get('password')
        self.ssl = connection_data.get('ssl')
        self.ssl_ca = connection_data.get('ssl_ca')
        self.ssl_cert = connection_data.get('ssl_cert')
        self.ssl_key = connection_data.get('ssl_key')

    def __connect(self):
        config = {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password
        }
        if self.ssl is True:
            config['client_flags'] = [mysql.connector.constants.ClientFlag.SSL]
            if self.ssl_ca is not None:
                config["ssl_ca"] = self.ssl_ca
            if self.ssl_cert is not None:
                config["ssl_cert"] = self.ssl_cert
            if self.ssl_key is not None:
                config["ssl_key"] = self.ssl_key

        connection = mysql.connector.connect(**config)
        return connection

    def check_status(self):
        """
        Check the connection of the MySQL database
        :return: success status and error message if error occurs
        """
        status = {
            'success': False
        }
        try:
            con = self.__connect()
            with closing(con) as con:
                status['success'] = con.is_connected()
        except Exception as e:
            log.error(f'Error connecting to MySQL {self.database}, {e}!')
            status['error'] = e
        return status

    def native_query(self, query):
        """
        Receive SQL query and runs it
        :param query: The SQL query to run in MySQL
        :return: returns the records from the current recordset
        """
        con = self.__connect()
        with closing(con) as con:
            with con.cursor(dictionary=True, buffered=True) as cur:
                try:
                    cur.execute(f"USE {self.database};")
                    cur.execute(query)
                    if cur.with_rows:
                        result = cur.fetchall()
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
                    log.error(f'Error running query: {query} on {self.database}!')
                    response = {
                        'type': RESPONSE_TYPE.ERROR,
                        'error_code': 0,
                        'error_message': str(e)
                    }
        return response

    def get_tables(self):
        """
        Get a list with all of the tabels in MySQL
        """
        q = "SHOW TABLES;"
        result = self.native_query(q)
        return result

    def get_views(self):
        """
        Get more information about specific database views
        """
        q = f"SHOW FULL TABLES IN {self.database} WHERE TABLE_TYPE LIKE 'VIEW';"
        result = self.native_query(q)
        return result

    def describe_table(self, table_name):
        """
        Show details about the table
        """
        q = f"DESCRIBE {table_name};"
        result = self.native_query(q)
        return result

    def query(self, query: ASTNode):
        """
        Retrieve the data from the SQL statement.
        """
        renderer = SqlalchemyRender('mysql')
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def select_into(self, table, dataframe: pd.DataFrame):
        """
        TODO: Update this
        """
        try:
            con = create_engine(f'mysql://{self.host}:{self.port}/{self.database}', echo=False)
            dataframe.to_sql(table, con=con, if_exists='append', index=False)
            return True
        except Exception as e:
            print(e)
            raise Exception(f"Could not select into table {table}, aborting.")

    def join(self, stmt, data_handler, into: Optional[str] = None) -> pd.DataFrame:
        """
        TODO: Update this
        """
        local_result = self.select_query(stmt.targets, stmt.from_table.left, stmt.where)  # should check it's actually on the left
        external_result = data_handler.select_query(stmt.targets, stmt.from_table.right, stmt.where)  # should check it's actually on the right

        local_df = pd.DataFrame.from_records(local_result)
        external_df = pd.DataFrame.from_records(external_result)
        df = local_df.join(external_df, on=[str(t) for t in stmt.targets], lsuffix='_left', rsuffix='_right')

        if into:
            self.select_into(into, df)
        return df
