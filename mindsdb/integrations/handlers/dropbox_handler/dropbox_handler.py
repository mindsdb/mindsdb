import duckdb
import pandas as pd
import dropbox

from dropbox.exceptions import AuthError, ApiError

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
        self.dbx = None

    def connect(self):
        """
        Establishes a connection to the Dropbox account.

        Raises:
            ValueError: If the required connection parameters are not provided.

        Returns:
            dropbox: An object to the Dropbox account.
        """
        if self.is_connected:
            return self.dbx
        if "access_token" not in self.connection_args:
            raise ValueError("Access token must be provided.")
        self.dbx = dropbox.Dropbox(self.connection_args["access_token"])
        self.logger.info(
            f"Connected to the Dropbox by {self.dbx.users_get_current_account()}"
        )

    def disconnect(self):
        """
        Closes the connection to the Dropbox account if it's currently open.
        """
        if not self.is_connected:
            return
        self.is_connected = False
        self.dbx.close()
        self.dbx = None

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
        except (ApiError, ValueError) as e:
            self.logger.error(
                f"Error connecting to Dropbox with the given credentials, {e}!"
            )
            response.error_message = str(e)
        except AuthError as e:
            self.logger.error(
                f"Error connecting to Dropbox because of the wrong credentials, {e}!"
            )
            response.error_message = str(e)
        except Exception as e:
            self.logger.error(f"Error has occured: {e}")

        if response.success and need_to_close:
            self.disconnect()

        elif not response.success and self.is_connected:
            self.is_connected = False

        return response

    def get_tables(self) -> Response:
        """
        Retrieves a list of tables (supported files) in the Dropbox.

        Eachs supported file is considered a table.

        Returns:
            Response: A response object containing the list of tables and views, formatted as per the `Response` class.
        """
        files = self.dbx.file_requests_list_v2()
        supported_files = []

        response = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=pd.DataFrame(supported_files, columns=["table_name"]),
        )

        return response
