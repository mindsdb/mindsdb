from collections import OrderedDict
from typing import Optional

import pandas as pd
from google.cloud import firestore
from google.oauth2 import service_account

from mindsdb_sql import parse_sql, ASTNode

from mindsdb.integrations.handlers.firestore_handler.utils.firestore_render import FirestoreRender
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)

from mindsdb.utilities.log import get_log

logger = get_log(__name__)


class FirestoreHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Firestore statements.
    """

    name = 'firestore'

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        """
        Initialize the handler.
        Args:
            name (str): name of particular handler instance
            connection_data (dict): parameters for connecting to the database
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)
        self.parser = parse_sql
        self.dialect = 'firestore'

        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

    def connect(self) -> StatusResponse:
        """
        Set up the connection required by the handler.
        Returns:
            HandlerStatusResponse
        """

        if self.is_connected is True:
            return self.connection

        self.connection = firestore.Client(
            project=self.connection_data['project'],
            credentials=(
                service_account.Credentials.from_service_account_file(
                    self.connection_data['credentials']
                )
            ),
            database=self.connection_data.get('database'),
        )
        self.is_connected = True

        return self.connection

    def disconnect(self):
        """
        Close any existing connections.
        """

        if self.is_connected is False:
            return

        self.connection.close()
        self.is_connected = False
        return self.is_connected

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler.
        Returns:
            HandlerStatusResponse
        """

        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self.connect()
            response.success = True
        except Exception as e:
            logger.error(f'Error connecting to Firestore, {e}!')
            response.error_message = str(e)
        finally:
            if response.success is True and need_to_close:
                self.disconnect()
            if response.success is False and self.is_connected is True:
                self.is_connected = False

        return response

    def native_query(self, query) -> Response:
        try:
            columns = list(self.get_columns(query['collection']).data_frame.Field)
            query_result = query['query'].stream()

            if query.get('update_values') is not None:
                for row in query_result:
                    row.reference.update(query.get('update_values'))
                return Response(
                    RESPONSE_TYPE.OK,
                )
            else:
                result = [row.to_dict() for row in query_result]
                if len(result) > 0:
                    if query.get('requested_columns') is not None:
                        result = [{col: row.get(col) for col in query.get('requested_columns')} for row in result]
                    else:
                        result = [{col: row.get(col, None) for col in columns} for row in result]
                    df = pd.DataFrame(result)
                else:
                    df = pd.DataFrame([], columns=columns)

                return Response(
                    RESPONSE_TYPE.TABLE,
                    df
                )

        except Exception as e:
            logger.error(f'Error running query: {query} on {query["collection"]}!')
            return Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )

    def query(self, query: ASTNode) -> Response:
        """
        Retrieve the data from the SQL statement.
        """
        renderer = FirestoreRender(connection=self.connection)
        firestore_query = renderer.to_firestore_query(query)
        return self.native_query(firestore_query)

    def get_tables(self) -> Response:
        """
        Return list of entities that will be accessible as tables.
        Returns:
            HandlerResponse
        """

        collections = {collection.id for collection in self.connection.collections()}

        df = pd.DataFrame(
            data=collections,
            columns=['table_name']
        )
        response = Response(
            RESPONSE_TYPE.TABLE,
            df
        )

        return response

    def get_columns(self, collection: str) -> Response:
        """
        Return list of columns for particular collection.
        Note: this only uses the first document to define the columns
        Args:
            collection (str): name of the collection
        Returns:
            HandlerResponse
        """
        document_stream = self.connection.collection(collection).list_documents()
        document_ref = next(document_stream)
        document = document_ref.get().to_dict()

        columns = []
        if document is not None:
            for key, value in document.items():
                columns.append([key, type(value).__name__])

        df = pd.DataFrame(
            data=columns,
            columns=['Field', 'Type']
        )
        response = Response(
            RESPONSE_TYPE.TABLE,
            df
        )

        return response


connection_args = OrderedDict(
    project={
        'type': ARG_TYPE.STR,
        'description': 'The project id for the firestore database',
        'required': True,
        'label': 'Project',
    },
    credentials={
        'type': ARG_TYPE.STR,
        'description': 'Path to the service account credentials file',
        'required': True,
        'label': 'Credentials',
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'Database name',
        'required': False,
        'label': 'Database',
    },
)

connection_args_example = OrderedDict(
    project="firestore_project",
    credentials="secret_dir/key.json",
    database="region_east",
)
