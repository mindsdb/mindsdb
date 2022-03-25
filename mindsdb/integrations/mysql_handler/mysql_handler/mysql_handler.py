from typing import Union, List
import mysql.connector
from contextlib import closing

from mindsdb_sql import parse_sql
from mindsdb.integrations.libs.base_handler import DatabaseHandler


class MySQLHandler(DatabaseHandler):

    def __init__(self, name, **kwargs):
        super().__init__(name)
        self.connection = None
        self.mysql_url = None
        self.parser = parse_sql
        self.dialect = 'mysql'
        self.host = kwargs.get('host')
        self.port = kwargs.get('port')
        self.user = kwargs.get('user')
        self.database = kwargs.get('database')  # todo: may want a method to change active DB
        self.password = kwargs.get('password')
        self.ssl = kwargs.get('ssl')
        self.ssl_ca = kwargs.get('ssl_ca')
        self.ssl_cert = kwargs.get('ssl_cert')
        self.ssl_key = kwargs.get('ssl_key')
        self.connect()

    def connect(self):
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

        self.connection = mysql.connector.connect(**config)
        return self.connection

    def check_status(self):
        try:
            con = self.connection
            with closing(con) as con:
                connected = con.is_connected()
        except Exception:
            connected = False
        return connected

    def run_native_query(self, query_str):
        if not self.check_status():
            self.connect()
        try:
            with closing(self.connection) as con:
                cur = con.cursor(dictionary=True, buffered=True)
                cur.execute(f"USE {self.database};")
                cur.execute(query_str)
                res = True
                try:
                    res = cur.fetchall()
                except Exception:
                    pass
                con.commit()
        except Exception as e:
            raise Exception(f"Error: {e}. Please check and retry!")
        return res

    def get_tables(self):
        q = "SHOW TABLES;"
        result = self.run_native_query(q)
        return result

    def get_views(self):
        q = f"SHOW FULL TABLES IN {self.database} WHERE TABLE_TYPE LIKE 'VIEW';"
        result = self.run_native_query(q)
        return result

    def describe_table(self, table_name):
        q = f"DESCRIBE {table_name};"
        result = self.run_native_query(q)
        return result

    def select_query(self, targets, from_stmt, where_stmt):
        query = f"SELECT {','.join([t.__str__() for t in targets])} FROM {from_stmt.parts[0]}"
        if where_stmt:
            query += f" WHERE {str(where_stmt)}"

        result = self.run_native_query(query)
        return result

    def select_into(self, table_name, select_query):
        # todo: implement new signature
        query = f"CREATE TABLE {self.database}.{table_name} AS ({select_query})"
        result = self.run_native_query(query)

    # def join(self, stmt, data_handler, into: Optional[str]) -> pd.DataFrame:  # todo: adapt to base signature
    def join(self, table, left_integration: str, on: str, left_where: Union[None, str] = None, into: Optional[str] = None):
        """
        for now assumes
            - left_integration to be a table in the same DB, but should get to a point where it can be a different handler
            - single column to join on
        """
        if not on:
            on = '*'
        query = f"SELECT * FROM {self.database}.{table} JOIN {self.database}.{left_integration} ON {table}.{on}"
        if left_where is not None:
            query += f'WHERE {left_where}'
        result = self.run_native_query(query)

        if into:
            self.select_into(table, pd.DataFrame(list(result)))

        return result


if __name__ == '__main__':
    kwargs = {
        "host": "localhost",
        "port": "3306",
        "user": "root",
        "password": "root",
        "database": "test",
        "ssl": False
    }
    handler = MySQLHandler('test_handler', **kwargs)
    assert handler.check_status()

    dbs = handler.run_native_query("SHOW DATABASES;")
    assert isinstance(dbs, list)

    tbls = handler.get_tables()
    assert isinstance(tbls, list)

    views = handler.get_views()
    assert isinstance(views, list)

    try:
        result = handler.run_native_query("DROP TABLE test_mdb")
    except:
        pass
    try:
        handler.run_native_query("CREATE TABLE test_mdb (test_col INT)")
    except Exception:
        pass

    described = handler.describe_table("test_mdb")
    assert isinstance(described, list)

    query = "SELECT * FROM test_mdb WHERE 'id'='a'"
    parsed = handler.parser(query, dialect=handler.dialect)
    targets = parsed.targets
    from_stmt = parsed.from_table
    where_stmt = parsed.where
    result = handler.select_query(targets, from_stmt, where_stmt)

    try:
        result = handler.run_native_query("DROP TABLE test_mdb2")
    except:
        pass
    result = handler.select_into('test_mdb2', "SELECT * FROM test_mdb")

    tbls = handler.get_tables()
    assert 'test_mdb2' in [item['Tables_in_test'] for item in tbls]

    handler.run_native_query("INSERT INTO test_mdb(test_col) VALUES (1)")
    handler.run_native_query("INSERT INTO test_mdb2(test_col) VALUES (1)")
    result = handler.join('test_mdb', 'test_mdb2', 'test_col', left_where=None)

    # to build a DF with results
    import pandas as pd
    result_df = pd.DataFrame.from_records(handler.run_native_query(query))

    # todo: test `select_into` and `join` with `into`