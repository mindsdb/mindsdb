import os
import pandas as pd
from pyzotero import zotero
from mindsdb_sql.parser import ast
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

    def select(self, query: ast.Select):
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

        if query.where is None:  # Handle case for SELECT * FROM annotations
            method_name = 'items'
            params = {'itemType': 'annotation'}
            df = self.handler.call_find_annotations_zotero_api(method_name, params)
            return df[self.get_columns()]

        conditions = extract_comparison_conditions(query.where)
        params = {'itemType': 'annotation'}
        method_name = 'items'
        supported = False  # Flag to check if the query is supported

        for op, arg1, arg2 in conditions:
            if op in {'or', 'and'}:
                raise NotImplementedError('OR and AND are not supported')
            if arg1 == 'item_id' and op == '=':
                params[arg1] = arg2
                method_name = 'item'
                supported = True
            elif arg1 == 'parent_item_id' and op == '=':
                params[arg1] = arg2
                method_name = 'children'
                supported = True

        if not supported:
            raise NotImplementedError('Only "item_id=" and "parent_item_id=" conditions are implemented')

        df = self.handler.call_find_annotations_zotero_api(method_name, params)
        return df[self.get_columns()]

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
        self.connection_args = self._get_connection_args(kwargs.get('connection_data', {}))
        self.is_connected = False
        self.api = None
        self._register_table('annotations', AnnotationsTable(self))

    def _get_connection_args(self, args):
        """Fetch connection arguments from parameters, environment variables, or configuration.

        Parameters
        ----------
        args
            Dictionary containing connection data.

        Returns
        -------
        connection_args
            Connection data list
        """

        handler_config = Config().get('zotero_handler', {})
        connection_args = {}
        for k in ['library_id', 'library_type', 'api_key']:
            connection_args[k] = args.get(k) or os.getenv(f'ZOTERO_{k.upper()}') or handler_config.get(k)
        return connection_args

    def connect(self) -> StatusResponse:
        """Connect to the Zotero API.

        Returns
        -------
        StatusResponse
            Status of the connection attempt.
        """

        if not self.is_connected:
            self.api = zotero.Zotero(
                self.connection_args['library_id'],
                self.connection_args['library_type'],
                self.connection_args['api_key']
            )
            self.is_connected = True
        return StatusResponse(True)

    def check_connection(self) -> StatusResponse:
        """Check the connection status to the Zotero API.

        Returns
        -------
        StatusResponse
            Status of the connection.
        """

        try:
            self.connect()
            return StatusResponse(True)
        except Exception as e:
            error_message = f'Error connecting to Zotero API: {str(e)}. Check credentials.'
            logger.error(error_message)
            self.is_connected = False
            return StatusResponse(False, error_message=error_message)

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
        df = self.call_find_annotations_zotero_api(method_name, params)
        return Response(RESPONSE_TYPE.TABLE, data_frame=df)

    def call_find_annotations_zotero_api(self, method_name: str = None, params: dict = None) -> pd.DataFrame:
        """Call a method in the Zotero API.
        Specifically made for annotation related method calling.

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

        if not self.is_connected:
            self.connect()

        try:
            method = getattr(self.api, method_name)
            item_id = params.pop('item_id', None)
            parent_id = params.pop('parent_item_id', None)
            result = method(item_id or parent_id, **params) if item_id or parent_id else method(**params)

            if isinstance(result, dict):
                return pd.DataFrame([result.get('data', {})])
            if isinstance(result, list) and all(isinstance(item, dict) for item in result):
                data_list = [item.get('data', {}) for item in result]
                return pd.DataFrame(data_list)

        except Exception as e:
            logger.error(f"Error calling method '{method_name}' with params '{params}': {e}")
            raise e

        return pd.DataFrame()
