from mindsdb.integrations.libs.base_handler import DatabaseHandler
import psycopg
from mindsdb.utilities.log import log
from contextlib import closing
from mindsdb_sql import parse_sql
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender

class PostgresHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the PostgreSQL statements. 
    """

    def __init__(self, name, **kwargs):
        super().__init__(name)
        self.parser = parse_sql
        self.connection_args = kwargs
        del self.connection_args['database']
        self.dialect = 'postgresql'
        self.database = kwargs.get('database')

    def __connect(self):
        """
        Handles the connection to a PostgreSQL database insance.
        """
        #TODO: Check psycopg_pool
        self.connection_args['dbname'] = self.database
        connection = psycopg.connect(**self.connection_args, connect_timeout=10)
        return connection

    def check_status(self):
        """
        Check the connection of the PostgreSQL database
        :return: success status and error message if error occurs
        """
        status = {
            'success': False
        }
        try:   
            con = self.__connect()
            with closing(con) as con:
                with con.cursor() as cur:
                    cur.execute('select 1;')
            status['success'] = True
        except psycopg.Error as e:
            log.error(f'Error connecting to PostgreSQL {self.database}, {e}!')
            status['error'] = e
        return status
    
    def native_query(self, query_str):
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
        res = self.native_query(query)
        return res
    
    def get_views(self):
        """
        List all views in PostgreSQL without the system views information_schema and pg_catalog
        """
        query = f"SELECT * FROM information_schema.views WHERE table_schema NOT IN ('information_schema', 'pg_catalog')"
        result = self.native_query(query)
        return result

    def describe_table(self, table_name):
        """
        List names and data types about the table coulmns
        """
        query = f"SELECT table_name, column_name, data_type FROM \
              information_schema.columns WHERE table_name='{table_name}';"
        result = self.native_query(query)
        return result

    def select_query(self, query):
        """
        Retrieve the data from the SQL statement with eliminated rows that dont satisfy the WHERE condition
        """
        renderer = SqlalchemyRender('postgres')
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    #TODO: JOIN, SELECT INTO