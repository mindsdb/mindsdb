from collections import OrderedDict

from google.cloud import spanner_dbapi
from google.cloud.spanner_dbapi import Connection

import pandas as pd
from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.const import (
    HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE,
)
from mindsdb.integrations.libs.response import RESPONSE_TYPE
from mindsdb.integrations.libs.response import HandlerResponse as Response
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.utilities import log

logger = log.getLogger(__name__)

class CloudSpannerHandler(DatabaseHandler):
    """This handler handles connection and execution of the Cloud Spanner statements."""

    name = 'cloud_spanner'

    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        self.parser = parse_sql
        self.dialect = 'googlesql'
        self.connection_data = kwargs.get('connection_data')

        self.connection = None
        self.is_connected = False

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self) -> Connection:
        """Connect to a Cloud Spanner database.

        Returns:
            Connection: The database connection.
        """

        if self.is_connected is True:
            return self.connection

        args = {
            'database_id': self.connection_data.get('database_id'),
            'instance_id': self.connection_data.get('instance_id'),
            'project': self.connection_data.get('project'),
            'credentials': self.connection_data.get('crendentials'),
        }

        self.connection = spanner_dbapi.connect(**args)
        self.is_connected = True

        return self.connection

    def disconnect(self):
        """Close the database connection."""

        if self.is_connected is False:
            return

        self.connection.close()
        self.is_connected = False

    def check_connection(self) -> StatusResponse:
        """Check the connection to the Cloud Spanner database.

        Returns:
            StatusResponse: Connection success status and error message if an error occurs.
        """

        response = StatusResponse(False)

        try:
            self.connect()
            response.success = True
        except Exception as e:
            logger.error(
                f'Error connecting to Cloud Spanner {self.connection_data["database_id"]}, {e}!'
            )
            response.error_message = str(e)
        finally:
            if response.success is True and self.is_connected:
                self.disconnect()
            if response.success is False and self.is_connected:
                self.is_connected = False

        return response

    def native_query(self, query: str) -> Response:
        """Execute a SQL query.

        Args:
            query (str): The SQL query to execute.

        Returns:
            Response: The query result.
        """

        connection = self.connect()
        cursor = connection.cursor()

        try:
            cursor.execute(query)

            # The cursor description check indicates if there are any results.
            # This is required as spanner_dbapi will fail on a fetchall() call on an empty cursor.
            if cursor.description:
                result = cursor.fetchall()
                response = Response(
                    RESPONSE_TYPE.TABLE,
                    data_frame=pd.DataFrame(
                        result, columns=[x[0] for x in cursor.description]
                    ),
                )
            else:
                response = Response(RESPONSE_TYPE.OK)

            connection.commit()
        except Exception as e:
            logger.error(
                f'Error running query: {query} on {self.connection_data["database_id"]}!'
            )
            response = Response(RESPONSE_TYPE.ERROR, error_message=str(e))

        cursor.close()
        if self.is_connected:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> Response:
        """Render and execute a SQL query.

        Args:
            query (ASTNode): The SQL query.

        Returns:
            Response: The query result.
        """
        if isinstance(query, ASTNode):
            query_str = query.to_string()
        else:
            query_str = str(query)

        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """Get a list of all the tables in the database.

        Returns:
            Response: Names of the tables in the database.
        """

        query = '''
            SELECT
              t.table_name
            FROM
              information_schema.tables AS t
            WHERE
              t.table_schema = ''
        '''
        result = self.native_query(query)
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

        query = f'''
            SELECT
              t.column_name,
              t.spanner_type,
              t.is_nullable
            FROM
              information_schema.columns AS t
            WHERE
              t.table_name = '{table_name}'
        '''
        return self.native_query(query)


connection_args = OrderedDict(
    instance_id={
        'type': ARG_TYPE.STR,
        'description': 'The Cloud Spanner instance identifier.',
    },
    database_id={
        'type': ARG_TYPE.STR,
        'description': 'The Cloud Spanner database indentifier.',
    },
    project={
        'type': ARG_TYPE.STR,
        'description': 'The Cloud Spanner project indentifier.',
    },
    credentials={
        'type': ARG_TYPE.STR,
        'description': 'The Google Cloud Platform service account key in the JSON format.',
    },
)

connection_args_example = OrderedDict(
    instance_id='test-instance', datbase_id='example-db', project='your-project-id'
)
