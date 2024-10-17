import io
import pandas as pd
import dropbox
import duckdb

from dropbox.exceptions import AuthError, ApiError
from typing import Dict, Optional, Text

from mindsdb_sql.parser.ast import ASTNode
from mindsdb.utilities import log
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)


class DropboxHandler(DatabaseHandler):
    """
    This handler handles connection and execution of SQL statements on files from Dropbox.
    """

    name = "dropbox"
    supported_file_formats = ["csv", "tsv", "json", "parquet"]

    def __init__(self, name: Text, connection_data: Optional[Dict], **kwargs):
        """
        Initialize the handler.
        Args:
            name (Text): The name of the handler instance.
            connection_data (Dict): The connection data required to connect to the Dropbox account.
            kwargs: Arbitrary keyword arguments.
        """
        super().__init__(name)
        self.connection_data = connection_data
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
        """
        if self.is_connected:
            return self.dbx
        if "access_token" not in self.connection_data:
            raise ValueError("Access token must be provided.")
        self.dbx = dropbox.Dropbox(self.connection_data["access_token"])
        self.is_connected = True
        self.logger.info(
            f"Connected to Dropbox as {self.dbx.users_get_current_account().email}"
        )

    def disconnect(self):
        """
        Closes the connection to the Dropbox account if it's currently open.
        """
        if not self.is_connected:
            return
        self.dbx = None
        self.is_connected = False
        self.logger.info("Disconnected from Dropbox")

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to Dropbox.
        Returns:
            HandlerStatusResponse
        """
        response = StatusResponse(False)
        need_to_close = not self.is_connected

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
            self.logger.error(f"Error connecting to Dropbox: {e}")
            response.error_message = str(e)
        finally:
            if response.success and need_to_close:
                self.disconnect()
            elif not response.success and self.is_connected:
                self.is_connected = False

        return response

    def _load_tables(self):
        """
        Load files from Dropbox and register them as tables in DuckDB.
        """
        self.connect()
        self.connection = duckdb.connect(database=":memory:")
        files = self._list_files()

        for file in files:
            df = self._read_file(file["path"])
            table_name = file["name"]
            self.connection.register(table_name, df)

    def _list_files(self, path=""):
        """
        List all files in the Dropbox account starting from the given path.

        Args:
            path (str): The path to start listing files from.

        Returns:
            List[Dict]: A list of files with their metadata.
        """
        files = []

        result = self.dbx.files_list_folder(path, recursive=True)

        files.extend(self._process_entries(result.entries))

        while result.has_more:
            result = self.dbx.files_list_folder_continue(result.cursor)
            files.extend(self._process_entries(result.entries))

        return files

    def _process_entries(self, entries):
        """
        Process a list of entries from Dropbox and filter supported file formats.

        Args:
            entries (List): List of entries from Dropbox API.

        Returns:
            List[Dict]: A list of files with their metadata.
        """
        files = []
        for entry in entries:
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
        return files

    def _read_file(self, path) -> pd.DataFrame:
        """
        Reads a file from Dropbox and returns it as a Pandas DataFrame.

        Args:
            path (str): The path to the file in Dropbox.

        Returns:
            pd.DataFrame: The data from the file as a DataFrame.
        """
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

    def native_query(self, query: str) -> Response:
        """
        Execute a raw SQL query using DuckDB.

        Args:
            query (str): The SQL query to execute.

        Returns:
            HandlerResponse
        """
        need_to_close = not self.is_connected

        self.connect()
        self._load_tables()
        try:
            result_df = self.connection.execute(query).fetchdf()
            response = Response(RESPONSE_TYPE.TABLE, data_frame=result_df)
        except Exception as e:
            self.logger.error(f"Error executing query: {e}")
            response = Response(RESPONSE_TYPE.ERROR, error_message=str(e))
        finally:
            self.connection.close()
            if need_to_close:
                self.disconnect()
        return response

    def query(self, query: ASTNode) -> Response:
        """
        Execute a SQL query represented as an ASTNode.

        Args:
            query (ASTNode): The query AST.

        Returns:
            HandlerResponse
        """
        query_str = query.to_string().replace("`", '"')
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        Return list of entities that will be accessible as tables.

        Returns:
            HandlerResponse
        """
        self.connect()
        files = self._list_files()
        table_names = [file["name"] for file in files]

        response = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=pd.DataFrame(table_names, columns=["table_name"]),
        )

        return response

    def get_columns(self, table_name: Text) -> Response:
        """
        Returns a list of columns for the specified table.

        Args:
            table_name (str): The name of the table.

        Returns:
            HandlerResponse
        """
        self.connect()
        self._load_tables()

        table_name_quoted = table_name.replace("`", '"')

        try:
            result = self.connection.execute(f"DESCRIBE {table_name_quoted}").fetchdf()
            response = Response(
                RESPONSE_TYPE.TABLE,
                data_frame=pd.DataFrame(
                    {
                        "column_name": result["column_name"],
                        "data_type": result["column_type"],
                    }
                ),
            )
        except Exception as e:
            self.logger.error(f"Error retrieving columns for table {table_name}: {e}")
            response = Response(RESPONSE_TYPE.ERROR, error_message=str(e))
        finally:
            self.connection.close()
            self.disconnect()

        return response
