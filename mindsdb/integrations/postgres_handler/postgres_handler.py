from mindsdb.integrations.libs.base_handler import DatabaseHandler
import psycopg
from mindsdb.utilities.log import log
from contextlib import closing
from mindsdb_sql import parse_sql

class PostgresHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the PostgreSQL statements. 
    """

    def __init__(self, name, **kwargs):
        super().__init__(name)
        self.parser = parse_sql
        self.dialect = 'postgresql'
        self.database = kwargs.get('database')
        self.host = kwargs.get('host')
        self.port = kwargs.get('port')
        self.user = kwargs.get('user')
        self.password = kwargs.get('password')

    def __connect(self):
        """
        Handles the connection to a PostgreSQL database insance.
        """
        #TODO: Check psycopg_pool
        connection = psycopg.connect(f'host={self.host} port={self.port} dbname={self.database} user={self.user} password={self.password}', connect_timeout=10)
        return connection

    def check_status(self):
        """
        Check the connection of the PostgreSQL database
        """
        connected = False
        try:   
            con = self.__connect()
            with closing(con) as con:
                with con.cursor() as cur:
                    cur.execute('select 1;')
            connected = True
        except Exception as e:
            log.error(f'Error connecting to PostgreSQL {self.database} on {self.host}!')
            pass
        return connected
    
    def run_native_query(self, query_str):
        """
        Receive SQL query and runs it
        :param query_str: The SQL query to run in PostgreSQL
        :return: returns the records from the current recordset
        """
        con = self.__connect()
        with closing(con) as con:
            with con.cursor() as cur:
                res = True
                try:
                    cur.execute(query_str)
                    res = cur.fetchall()
                except psycopg.Error as e:
                    log.error(f'Error running {query_str} on {self.database}!')
                    pass
        return res
    
    def get_tables(self):
        """
        List all tabels in PostgreSQL without the system tables information_schema and pg_catalog
        """
        query = f"SELECT * FROM information_schema.tables WHERE \
                 table_schema NOT IN ('information_schema', 'pg_catalog')"
        res = self.run_native_query(query)
        return res
    
    def get_views(self):
        """
        List all views in PostgreSQL without the system views information_schema and pg_catalog
        """
        query = f"SELECT * FROM information_schema.views WHERE table_schema NOT IN ('information_schema', 'pg_catalog')"
        result = self.run_native_query(query)
        return result

    def describe_table(self, table_name):
        """
        List names and data types about the table coulmns
        """
        query = f"SELECT table_name, column_name, data_type FROM \
              information_schema.columns WHERE table_name='{table_name}';"
        result = self.run_native_query(query)
        return result

  