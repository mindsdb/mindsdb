import duckdb
import pandas as pd
import dropbox
import requests

from typing import Dict, Optional

from duckdb import DuckDBPyConnection

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb_sql.parser.ast import Select, Identifier
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)


class DropboxHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the SQL statements on files from Dropbox.
    """

    name = "dropbox"
    supported_file_formats = ["csv", "tsv", "json"]

    def __init__(self, name: str, connection_args: Optional[Dict], **kwargs):
        """
        Initializes the handler.

        Args:
            name (Text): The name of the handler instance.
            connection_args (Dict): The connection data required to connect to the Dropbox account.
            kwargs: Arbitrary keyword arguments.
        """
        super().__init__(name)
        self.connection_args = connection_args
        self.kwargs = kwargs
        self.logger = log.getLogger(__name__)

        self.connection = None
        self.is_connected = False

    def connect(self):
        """
        Establishes a connection to the Dropbox account.

        Raises:
            ValueError: If the required connection parameters are not provided.

        Returns:
            dropbox: An object to the Dropbox account.
        """

    def disconnect(self):
        """
        Closes the connection to the Dropbox account if it's currently open.
        """
        if not self.is_connected:
            return
        self.is_connected = False

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the S3 bucket.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self.connect()
            response.success = True
        except (requests.exceptions.ConnectionError, ValueError) as e:
            self.logger.error(
                f"Error connecting to Dropbox with the given credentials, {e}!"
            )
            response.error_message = str(e)

        if response.success and need_to_close:
            self.disconnect()

        elif not response.success and self.is_connected:
            self.is_connected = False

        return response
