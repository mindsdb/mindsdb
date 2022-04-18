from typing import List, Optional
from contextlib import closing

import pandas as pd
import mysql.connector
from sqlalchemy import create_engine

from mindsdb_sql import parse_sql
from mindsdb.integrations.libs.base_handler import DatabaseHandler
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender


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
        connected = False
        try:
            con = self.connect()
            with closing(con) as con:
                connected = con.is_connected()
        except Exception:
            pass
        return connected

    def native_query(self, query_str):
        try:
            with closing(self.connect()) as con:
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
        result = self.native_query(q)
        return result

    def get_views(self):
        q = f"SHOW FULL TABLES IN {self.database} WHERE TABLE_TYPE LIKE 'VIEW';"
        result = self.native_query(q)
        return result

    def describe_table(self, table_name):
        q = f"DESCRIBE {table_name};"
        result = self.native_query(q)
        return result

    def select_query(self, query):
        renderer = SqlalchemyRender('mysql')
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def select_into(self, table, dataframe: pd.DataFrame):
        try:
            con = create_engine(f'mysql://{self.host}:{self.port}/{self.database}', echo=False)
            dataframe.to_sql(table, con=con, if_exists='append', index=False)
            return True
        except Exception as e:
            print(e)
            raise Exception(f"Could not select into table {table}, aborting.")

    def join(self, stmt, data_handler, into: Optional[str] = None) -> pd.DataFrame:
        local_result = self.select_query(stmt.targets, stmt.from_table.left, stmt.where)  # should check it's actually on the left
        external_result = data_handler.select_query(stmt.targets, stmt.from_table.right, stmt.where)  # should check it's actually on the right

        local_df = pd.DataFrame.from_records(local_result)
        external_df = pd.DataFrame.from_records(external_result)
        df = local_df.join(external_df, on=[str(t) for t in stmt.targets], lsuffix='_left', rsuffix='_right')

        if into:
            self.select_into(into, df)
        return df
