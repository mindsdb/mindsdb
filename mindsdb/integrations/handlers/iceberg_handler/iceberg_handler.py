from collections import OrderedDict
from typing import Optional

import pandas as pd
from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from pyiceberg.catalog import load_catalog

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from mindsdb.integrations.libs.response import RESPONSE_TYPE
from mindsdb.integrations.libs.response import HandlerResponse as Response
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse
from mindsdb.utilities import log


class IcebergHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Iceberg SQL statements.
    """

    name = 'iceberg'

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        super().__init__(name)
        self.parser = parse_sql
        self.dialect = 'iceberg'
        self.renderer = SqlalchemyRender('postgres')
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self):
        if self.is_connected is True:
            return self.connection

        user = self.connection_data.get('user')
        password = self.connection_data.get('password')
        db = self.connection_data.get('database')
        name = self.connection_data.get('name', 'default')
        namespace = self.connection_data.get('namespace')
        table = self.connection_data.get('table')

        config = {
            "type": self.connection_data.get('type', "sql"),
            "uri": f"postgresql+psycopg2://{user}:{password}@localhost/{db}",
        }

        catalog = load_catalog(name, **config)

        try:
            _ = catalog.list_namespaces()
        except Exception:
            catalog.create_tables()
            raise Exception(
                "You need to configure a table schema first. "
                + "Read the pyiceberg documentation for more information: "
                + "https://py.iceberg.apache.org/api/"
            )

        self.is_connected = True

        table_cat = catalog.load_table((namespace, table))
        self.connection = table_cat.scan().to_duckdb(table_name=table)
        query = f"""
        INSTALL postgres_scanner;
        LOAD postgres_scanner;
        CALL postgres_attach('host=localhost dbname={db} user={user} password={password}');
        """
        self.connection.execute(query)
        return self.connection

    def disconnect(self):
        if self.is_connected is False:
            return
        self.connection.close()
        self.is_connected = False
        return

    def check_connection(self) -> StatusResponse:
        """
        Check the connection of the Iceberg database
        :return: success status and error message if error occurs
        """

        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self.connect()
            response.success = True
        except Exception as e:
            log.logger.error(
                f'Error connecting to Iceberg {self.connection_data["database"]}, {e}!'
            )
            response.error_message = str(e)

        if response.success is True and need_to_close:
            self.disconnect()

        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def native_query(self, query: ASTNode) -> Response:
        """
        Execute the Iceberg query
        :param query: Iceberg query
        :return: query result
        """

        need_to_close = self.is_connected is False
        connection = self.connect()
        cursor = connection.cursor()

        try:
            cursor.execute(query)

            result = cursor.fetchall()
            if result:
                response = Response(
                    RESPONSE_TYPE.TABLE,
                    data_frame=pd.DataFrame(
                        result, columns=[x[0] for x in cursor.description]
                    ),
                )
            else:
                connection.commit()
                response = Response(RESPONSE_TYPE.OK)
        except Exception as e:
            log.logger.error(
                f'Error running query: {query} on {self.connection_data["database"]}!'
            )
            response = Response(RESPONSE_TYPE.ERROR, error_message=str(e))

        cursor.close()
        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: str) -> Response:
        """
        Execute a SQL query.

        Args:
            query (str): The SQL query to execute.

        Returns:
            Response: The query result.
        """
        query_str = self.renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        Get a list of all tables in the database.

        Returns:
            Response: The names of the tables in the database.
        """

        q = "SHOW TABLES;"
        result = self.native_query(q)
        df = result.data_frame
        result.data_frame = df.rename(columns={df.columns[0]: 'table_name'})
        return result

    def get_columns(self, table_name: str) -> Response:
        """Get details about a table.

        Args:
            table_name (str): Name of the table to retrieve details of.

        Returns:
            Response: Details of the table.
        """

        query = f'DESCRIBE {table_name};'
        return self.native_query(query)


connection_args = OrderedDict(
    user={
        'name': 'user',
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the database.',
        'required': True,
    },
    password={
        'name': 'password',
        'type': ARG_TYPE.STR,
        'description': 'The password used to authenticate with the database.',
        'required': True,
    },
    database={
        'name': 'database',
        'type': ARG_TYPE.STR,
        'description': 'The database name.',
        'required': True,
    },
    table={
        'name': 'table',
        'type': ARG_TYPE.STR,
        'description': 'The table name.',
        'required': True,
    },
    type={
        'name': 'type',
        'type': ARG_TYPE.STR,
        'description': 'The type of the database.',
        'required': False,
    },
    name={
        'name': 'name',
        'type': ARG_TYPE.STR,
        'description': 'The name of the catalog.',
        'required': True,
    },
    namespace={
        'name': 'namespace',
        'type': ARG_TYPE.STR,
        'description': 'The namespace in the catalog.',
        'required': True,
    },
)

connection_args_example = OrderedDict(
    user='postgres',
    password='password',
    database='database',
    table='table',
    type='sql',
    name='default',
    namespace='docs_example',
)
