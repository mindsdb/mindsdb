import io
import duckdb
import pandas as pd
import dropbox

from dropbox.exceptions import AuthError, ApiError

from typing import Any, Dict, Optional, Text

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

supported_file_formats = ["csv", "tsv", "json"]


class DropboxHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the SQL statements on files from Dropbox.
    """

    name = "dropbox"

    def __init__(self, name: Text, connection_data: Optional[Dict], **kwargs):
        """
        Initializes the handler.

        Args:
            name (Text): The name of the handler instance.
            connection_args (Dict): The connection data required to connect to the Dropbox account.
            kwargs: Arbitrary keyword arguments.
        """
        super().__init__(name)
        self.connection_args = connection_data
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
        self.logger.info(f"Connecting to Dropbox...")
        if self.is_connected:
            return self.dbx
        if "access_token" not in self.connection_args:
            raise ValueError("Access token must be provided.")
        self.dbx = dropbox.Dropbox(self.connection_args["access_token"])
        self.logger.info(
            f"Connected to Dropbox by {self.dbx.users_get_current_account()}"
        )

    def disconnect(self):
        """
        Closes the connection to the Dropbox account if it's currently open.
        """
        self.logger.info(f"Disconnecting from Dropbox...")
        if not self.is_connected:
            return
        self.is_connected = False
        self.dbx.close()
        self.dbx = None
        self.logger.info(f"Disconnected from Dropbox")

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the Dropbox.

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
        self.connect()
        self.logger.info(f"Getting list of tables...")
        try:
            files = self._list_files()
            table_names = [file["name"] for file in files]
        except TimeoutError as e:
            self.logger.info(f"Timeout getting list of tables from dropbox: {e}")

        response = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=pd.DataFrame(table_names, columns=["table_name"]),
        )
        self.logger.info(f"Retrieved all the tables: {table_names}")

        return response

    def _list_files(self, path=""):
        """
        List all files in the Dropbox account starting from the given path.

        Args:
            path (str): The path to start listing files from.

        Returns:
            List[Dict]: A list of files with their metadata.
        """
        self.connect()
        files = []
        result = self.dbx.files_list_folder(path, recursive=True)
        while True:
            for entry in result.entries:
                if isinstance(entry, dropbox.files.FileMetadata):
                    extension = entry.name.split(".")[-1].lower()
                    if extension in self.supported_file_formats:
                        files.append(
                            {
                                "path": entry.path_lower,
                                "name": entry.name,
                                "extension": extension,
                            }
                        )
            if result.has_more:
                result = self.dbx.files_list_folder_continue(result.cursor)
            else:
                break

        return files

    def _read_file(self, path) -> pd.DataFrame:
        """
        Reads a file from Dropbox and returns it as a Pandas DataFrame.

        Args:
            path (str): The path to the file in Dropbox.

        Returns:
            pd.DataFrame: The data from the file as a DataFrame.
        """
        self.connect()
        _, res = self.dbx.files_download(path)
        content = res.content
        extension = path.split(".")[-1].lower()
        if extension == "csv":
            df = pd.read_csv(io.BytesIO(content))
        elif extension == "tsv":
            df = pd.read_csv(io.BytesIO(content), sep="\t")
        elif extension == "json":
            df = pd.read_json(io.BytesIO(content))
        elif extension == "parquet":
            df = pd.read_parquet(io.BytesIO(content))
        else:
            raise ValueError(f"Unsupported file format: {extension}")
        return df
    
    def query(self, query: ASTNode) -> Response:
        """Receive query as AST (abstract syntax tree) and act upon it somehow.
        Args:
            query (ASTNode): sql query represented as AST. May be any kind
                of query: SELECT, INSERT, DELETE, etc
        Returns:
            HandlerResponse
        """
        pass



    def native_query(self, query: Any) -> Response:
        """Receive raw query and act upon it somehow.
        Args:
            query (Any): query in native format (str for sql databases,
                dict for mongo, etc)
        Returns:
            HandlerResponse
        """
        pass

