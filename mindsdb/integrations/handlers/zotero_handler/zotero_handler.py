import os
from pyzotero import zotero
from mindsdb.utilities import log
from mindsdb.utilities.config import Config
from mindsdb.integrations.libs.api_handler import APIHandler, FuncParser

from mindsdb.integrations.handlers.zotero_handler.zotero_tables import AnnotationsTable

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)

logger = log.getLogger(__name__)


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

    def native_query(self, query_string: str = None):
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
        df = self._call_find_annotations_zotero_api(method_name, params)
        return Response(RESPONSE_TYPE.TABLE, data_frame=df)
