from typing import List, Optional, Dict
from arango import ArangoClient
from arango.exceptions import ArangoError 

from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.utilities import log
import pandas as pd

logger = log.getLogger(__name__)

class ArangoDBHandler(APIHandler):
    """
    This handler handles connection and execution of the ArangoDB statements.
    """

    name = 'arangodb'

    def __init__(self, name: str, connection_data: Optional[Dict], **kwargs):
        super().__init__(name)
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.client = None
        self.db = None
        self.is_connected = False

    def connect(self):
        """
        Establishes a connection to the ArangoDB database.
        """
        if self.is_connected:
            return self.db

        hosts = self.connection_data.get('hosts', 'http://127.0.0.1:8529')
        database = self.connection_data.get('database', '_system')
        username = self.connection_data.get('username')
        password = self.connection_data.get('password')

        try:
            self.client = ArangoClient(hosts=hosts)
            self.db = self.client.db(database, username=username, password=password)
            # Verify connection by fetching version
            self.db.version()
            self.is_connected = True
        except ArangoError as e:
            logger.error(f'Error connecting to ArangoDB: {e}')
            raise e
        except Exception as e:
            logger.error(f'Unexpected error connecting to ArangoDB: {e}')
            raise e

        return self.db

    def check_connection(self) -> StatusResponse:
        response = StatusResponse(False)

        try:
            self.connect()
            response.success = True
        except ArangoError as e:
            logger.error(f'Error connecting to ArangoDB: {e}')
            response.error_message = str(e)
        except Exception as e:
            logger.error(f'Unexpected error connecting to ArangoDB: {e}')
            response.error_message = str(e)

        return response

    def native_query(self, query: str) -> Response:
        """
        Executes a raw AQL query.
        """
        db = self.connect()
        try:
            cursor = db.aql.execute(query)
            # cursor returns a generator of dicts
            data = list(cursor)
            df = pd.DataFrame(data)
            return Response(RESPONSE_TYPE.TABLE, data_frame=df)
        except ArangoError as e:
            logger.error(f'Error executing query: {query}, {e}')
            return Response(RESPONSE_TYPE.ERROR, error_message=str(e))
        except Exception as e:
            logger.error(f'Unexpected error executing query: {query}, {e}')
            return Response(RESPONSE_TYPE.ERROR, error_message=str(e))

    def get_tables(self) -> Response:
        """
        Lists all non-system collections as tables.
        """
        db = self.connect()
        try:
            collections = db.collections()
            # Filter system collections
            tables = [c['name'] for c in collections if not c['system']]
            df = pd.DataFrame(tables, columns=['table_name'])
            return Response(RESPONSE_TYPE.TABLE, data_frame=df)
        except ArangoError as e:
            logger.error(f'Error listing tables: {e}')
            return Response(RESPONSE_TYPE.ERROR, error_message=str(e))
        except Exception as e:
            logger.error(f'Unexpected error listing tables: {e}')
            return Response(RESPONSE_TYPE.ERROR, error_message=str(e))
            
    def get_columns(self, table_name: str) -> Response:
        """
        Infers columns from the first 5 document in the collection.
        This is a schema-less DB, so this is a best-effort guess.
        """
        db = self.connect()
        try:
            collection = db.collection(table_name)
            cursor = collection.all(skip=0, limit=5)
            documents = list(cursor)
            
            if not documents:
                 return Response(RESPONSE_TYPE.TABLE, data_frame=pd.DataFrame(columns=['column_name', 'data_type']))

            # Infer schema from pandas
            df_sample = pd.DataFrame(documents)
            columns = df_sample.columns
            types = [str(t) for t in df_sample.dtypes]
            
            df_cols = pd.DataFrame({
                'column_name': columns,
                'data_type': types
            })
            return Response(RESPONSE_TYPE.TABLE, data_frame=df_cols)

        except ArangoError as e:
            logger.error(f'Error getting columns for {table_name}: {e}')
            return Response(RESPONSE_TYPE.ERROR, error_message=str(e))
        except Exception as e:
            logger.error(f'Unexpected error getting columns for {table_name}: {e}')
            return Response(RESPONSE_TYPE.ERROR, error_message=str(e))

