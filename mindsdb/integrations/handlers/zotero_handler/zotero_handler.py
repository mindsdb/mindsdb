import os
import ast

from pyzotero import zotero
import pandas as pd

from mindsdb.utilities import log
from mindsdb.utilities.config import Config

from mindsdb.integrations.libs.api_handler import APIHandler, APITable, FuncParser
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)

logger = log.getLogger(__name__)

class AnnotationsTable(APITable):
    """Represents a table of annotations in Zotero."""

    def select(self, query: ast.Select) -> Response:
        """Select annotations based on the provided query.

        Parameters
        ----------
        query : ast.Select
            AST (Abstract Syntax Tree) representation of the SQL query.

        Returns
        -------
        Response
            Response object containing the selected annotations as a DataFrame.
        """
        conditions = extract_comparison_conditions(query.where)

        params = {}
        method_name = 'items'  # Default method name
        for op, arg1, arg2 in conditions:

            if op in {'or', 'and'}:
                raise NotImplementedError(f'OR and AND are not supported')
            if arg1 == 'item_id':
                if op == '=':
                    params['item_id'] = arg2
                    method_name = 'item'
                else:
                    NotImplementedError('Only  "item_id=" is implemented')
            if arg1 == 'parent_item_id':
                if op == '=':
                    params['parent_item_id'] = arg2
                    method_name = 'children'
                else:
                    NotImplementedError('Only  "parent_item_id=" is implemented')
            
        params.update({'itemType': 'annotation'})  # Add item type to params

        annotations_data = self.handler.call_zotero_api(method_name, params)
        annotations = [annotation['data'] for annotation in annotations_data]
        df = pd.DataFrame(annotations)
        
        # Get the columns of the annotations table
        columns = self.get_columns()

        # Filter the DataFrame by columns
        df = df[columns]

        return Response(RESPONSE_TYPE.TABLE, data_frame=df)
    
    def get_columns(self):
        """Get the columns of the annotations table.

        Returns
        -------
        list
            List of column names.
        """
        return [
            'annotationColor',
            'annotationComment',
            'annotationPageLabel',
            'annotationText',
            'annotationType',
            'dateAdded',
            'dateModified',
            'key',
            'parentItem',
            'relations',
            'tags',
            'version'
        ]

class ZoteroHandler(APIHandler):
    """Handles communication with the Zotero API."""

    def __init__(self, name=None, **kwargs):
        """Initialize the Zotero handler.

        Parameters
        ----------
        name : str
            Name of the handler instance.

        Other Parameters
        ----------------
        connection_data : dict
            Dictionary containing connection data such as 'library_id', 'library_type', and 'api_key'.
            If not provided, will attempt to fetch from environment variables or configuration file.
        """

        super().__init__(name)

        args = kwargs.get('connection_data', {})
        self.connection_args = {}

        handler_config = Config().get('zotero_handler', {})
        for k in ['library_id', 'library_type', 'api_key']:
            if k in args:
                self.connection_args[k] = args[k]
            elif f'ZOTERO_{k.upper()}' in os.environ:
                self.connection_args[k] = os.environ[f'ZOTERO_{k.upper()}']
            elif k in handler_config:
                self.connection_args[k] = handler_config[k]
            
        self.is_connected = False
        self.api = None

        annotations_table = AnnotationsTable(self)
        self._register_table('annotations', annotations_table)

    def connect(self) -> StatusResponse :
        """Connect to the Zotero API.

        Returns
        -------
        StatusResponse
            Status of the connection attempt.
        """

        if self.is_connected is True:
            return self.api

        self.api = zotero.Zotero(self.connection_args['library_id'],
                                  self.connection_args['library_type'], 
                                  self.connection_args['api_key'])

        self.is_connected = True
        return self.api
    
    def check_connection(self) -> StatusResponse:
        """Check the connection status to the Zotero API.

        Returns
        -------
        StatusResponse
            Status of the connection.
        """

        response = StatusResponse(False)

        try:
            self.connect()
            response.success = True

        except Exception as e:
            response.error_message = f'Error connecting to Zotero API: {str(e)}. Check credentials.'
            logger.error(response.error_message)

        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def native_query(self, query_string: str = None) -> Response:
        """Execute a native query against the Zotero API.

        Parameters
        ----------
        query_string : str
            The query string to execute, formatted as required by the Zotero API.

        Returns
        -------
        Response
            Response object containing the result of the query.
        """

        method_name, params = FuncParser().from_string(query_string)

        df = self.call_zotero_api(method_name, params)

        return Response(
            RESPONSE_TYPE.TABLE,
            data_frame=df
        )
    
    def call_zotero_api(self, method_name: str = None, params: dict = None) -> pd.DataFrame:
        """Call a method in the Zotero API.

        Parameters
        ----------
        method_name : str
            The name of the method to call in the Zotero API.
        params : dict
            Parameters to pass to the method.

        Returns
        -------
        pd.DataFrame
            DataFrame containing the result of the API call.
        """

        if self.is_connected is False:
            self.connect()

        method = getattr(self.api, method_name)

        try:
            result = method(**params)
        except Exception as e:
            error = f"Error calling method '{method_name}' with params '{params}': {e}"
            logger.error(error)
            raise e
        return pd.DataFrame(result)

