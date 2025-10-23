from typing import Any, Dict, Text, Optional, List
import time
import json
import hashlib
from datetime import datetime

import msal
from mindsdb_sql_parser.ast.base import ASTNode
from mindsdb_sql_parser.ast import Constant, Identifier, Select, Star
from mindsdb_sql_parser import parse_sql
import pandas as pd
from requests.exceptions import RequestException

from mindsdb.integrations.handlers.ms_one_drive_handler.ms_graph_api_one_drive_client import MSGraphAPIOneDriveClient
from mindsdb.integrations.handlers.ms_one_drive_handler.ms_one_drive_tables import (
    FileTable,
    ListFilesTable,
    ConnectionMetadataTable,
    DeltaStateTable,
    SyncStatsTable
)
from mindsdb.integrations.utilities.handlers.auth_utilities.microsoft import MSGraphAPIDelegatedPermissionsManager
from mindsdb.integrations.utilities.handlers.auth_utilities.exceptions import AuthException
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    HandlerStatusResponse as StatusResponse,
    RESPONSE_TYPE
)
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class MSOneDriveHandler(APIHandler):
    """
    This handler handles the connection and execution of SQL statements on Microsoft OneDrive.

    Supports both legacy code-based auth and modern token injection for multi-tenant operations.
    """

    name = 'one_drive'
    supported_file_formats = ['csv', 'tsv', 'json', 'parquet', 'pdf', 'txt']

    # Token refresh settings
    TOKEN_REFRESH_BUFFER_SECONDS = 300  # Refresh if expiring within 5 minutes

    # Scope requirements
    MINIMUM_REQUIRED_SCOPES = ['Files.Read', 'offline_access']

    def __init__(self, name: Text, connection_data: Dict, **kwargs: Any) -> None:
        """
        Initializes the handler.

        Args:
            name (Text): The name of the handler instance.
            connection_data (Dict): The connection data required to connect to the Microsoft Graph API.
                Supported parameters:
                    - Legacy: client_id, client_secret, tenant_id, code
                    - Modern: access_token, refresh_token, expires_at, tenant_id, account_id
                    - Optional: authority (default: 'common'), scopes, enable_files_read_all
                    - File Picker: selected_items (list of item objects from file picker)
                      Note: scope_type is auto-set to 'specific' when selected_items is provided
            kwargs: Arbitrary keyword arguments.
        """
        super().__init__(name)
        self.connection_data = connection_data
        self.handler_storage = kwargs['handler_storage']
        self.kwargs = kwargs

        # Generate unique connection ID for storage isolation
        self.connection_id = self._generate_connection_id()

        self.connection = None
        self.is_connected = False
        self._token_metadata = None

        # Parse file picker parameters
        # Auto-detect scope type based on selected_items presence
        self.selected_items = connection_data.get('selected_items', [])
        self.scope_type = 'specific' if self.selected_items else connection_data.get('scope_type', 'all')

    def _generate_connection_id(self) -> str:
        """
        Generates a unique connection ID based on connection parameters for storage isolation.

        Returns:
            str: A unique connection identifier
        """
        # Use name + tenant_id/client_id combination for uniqueness
        key_parts = [
            self.kwargs.get('name', 'default'),
            self.connection_data.get('tenant_id', ''),
            self.connection_data.get('account_id', ''),
            self.connection_data.get('client_id', '')
        ]
        key_string = '_'.join(filter(None, key_parts))
        return hashlib.sha256(key_string.encode()).hexdigest()[:16]

    def _parse_expires_at(self, expires_at: Any) -> float:
        """
        Parse expires_at value to Unix timestamp.

        Args:
            expires_at: Can be Unix timestamp (int/float) or datetime string

        Returns:
            float: Unix timestamp
        """
        if expires_at is None:
            # Default to 1 hour from now
            return time.time() + 3600

        # If already a number (Unix timestamp), return it
        if isinstance(expires_at, (int, float)):
            return float(expires_at)

        # If it's a string, parse it
        if isinstance(expires_at, str):
            try:
                # Try parsing different datetime formats
                # Format: "2025-10-23 21:41:33.683305 +0000"
                dt = datetime.strptime(expires_at.split('.')[0] + ' +0000', '%Y-%m-%d %H:%M:%S %z')
                return dt.timestamp()
            except ValueError:
                try:
                    # Try ISO format
                    dt = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
                    return dt.timestamp()
                except ValueError:
                    logger.warning(f"Could not parse expires_at: {expires_at}, defaulting to 1 hour")
                    return time.time() + 3600

        # Fallback
        return time.time() + 3600

    def _get_token_cache_key(self) -> str:
        """Returns the storage key for per-connection token cache."""
        return f"token_cache_{self.connection_id}.bin"

    def _load_token_metadata(self) -> Optional[Dict]:
        """Load stored token metadata from handler storage."""
        try:
            metadata_key = f"token_metadata_{self.connection_id}.json"
            metadata_content = self.handler_storage.file_get(metadata_key)
            if metadata_content:
                return json.loads(metadata_content.decode('utf-8'))
        except FileNotFoundError:
            pass
        return None

    def _save_token_metadata(self, metadata: Dict) -> None:
        """Save token metadata to handler storage."""
        metadata_key = f"token_metadata_{self.connection_id}.json"
        self.handler_storage.file_set(
            metadata_key,
            json.dumps(metadata).encode('utf-8')
        )

    def _needs_token_refresh(self) -> bool:
        """
        Check if token needs refresh based on expiry time.

        Returns:
            bool: True if token needs refresh
        """
        if not self._token_metadata:
            return False

        expires_at = self._token_metadata.get('expires_at')
        if not expires_at:
            return False

        # Refresh if expiring within buffer window
        return time.time() >= (expires_at - self.TOKEN_REFRESH_BUFFER_SECONDS)

    def _refresh_access_token(self) -> Optional[str]:
        """
        Refresh the access token using the refresh token.

        Returns:
            Optional[str]: New access token if successful, None otherwise
        """
        refresh_token = self.connection_data.get('refresh_token')
        if not refresh_token:
            logger.warning("No refresh token available for token refresh")
            return None

        try:
            # Use MSAL to refresh token
            cache = msal.SerializableTokenCache()
            authority = self.connection_data.get('authority', 'common')
            tenant_id = self.connection_data.get('tenant_id', authority)

            msal_app = msal.ConfidentialClientApplication(
                self.connection_data['client_id'],
                authority=f"https://login.microsoftonline.com/{tenant_id}",
                client_credential=self.connection_data.get('client_secret', ''),
                token_cache=cache
            )

            scopes = self.connection_data.get('scopes', ['https://graph.microsoft.com/.default'])
            if isinstance(scopes, str):
                scopes = scopes.split(',')

            result = msal_app.acquire_token_by_refresh_token(
                refresh_token,
                scopes=scopes
            )

            if 'access_token' in result:
                # Update stored metadata
                self._token_metadata = {
                    'access_token': result['access_token'],
                    'refresh_token': result.get('refresh_token', refresh_token),
                    'expires_at': time.time() + result.get('expires_in', 3600)
                }
                self._save_token_metadata(self._token_metadata)

                # Update connection data for current session
                self.connection_data['access_token'] = result['access_token']
                if 'refresh_token' in result:
                    self.connection_data['refresh_token'] = result['refresh_token']

                logger.info(f"Successfully refreshed access token for connection {self.connection_id}")
                return result['access_token']
            else:
                logger.error(f"Token refresh failed: {result.get('error_description', 'Unknown error')}")
                return None

        except Exception as e:
            logger.error(f"Exception during token refresh: {e}")
            return None

    def _validate_scopes(self, granted_scopes: List[str]) -> None:
        """
        Validate that granted scopes include minimum required scopes.

        Args:
            granted_scopes: List of granted scope strings

        Raises:
            ValueError: If required scopes are missing
        """
        # Check for Files.Read.All if explicitly enabled
        if self.connection_data.get('enable_files_read_all'):
            if 'Files.Read.All' not in granted_scopes:
                logger.warning("Files.Read.All requested but not granted")

        # Check minimum scopes
        missing_scopes = [
            scope for scope in self.MINIMUM_REQUIRED_SCOPES
            if scope not in granted_scopes and not any(s in granted_scopes for s in ['Files.Read.All', '.default'])
        ]

        if missing_scopes:
            raise ValueError(
                f"Missing required scopes: {', '.join(missing_scopes)}. "
                f"Please ensure your app registration includes these permissions."
            )

    def _use_token_injection_path(self) -> bool:
        """Determine if we should use token injection (modern) or legacy code exchange."""
        return 'access_token' in self.connection_data or 'refresh_token' in self.connection_data

    def connect(self):
        """
        Establishes a connection to Microsoft OneDrive via the Microsoft Graph API.

        Supports both modern token injection and legacy code-based authentication.

        Raises:
            ValueError: If the required connection parameters are not provided.
            AuthenticationError: If an error occurs during the authentication process.

        Returns:
            MSGraphAPIOneDriveClient: An instance of the Microsoft Graph API client for Microsoft OneDrive.
        """
        if self.is_connected and self.connection and self.connection.check_connection():
            return self.connection

        # Choose authentication path
        if self._use_token_injection_path():
            # Modern path: Token injection
            access_token = self._connect_with_token_injection()
        else:
            # Legacy path: Code exchange
            logger.warning("Using legacy code-based authentication. Consider migrating to token injection.")
            access_token = self._connect_with_code_exchange()

        # Create the Graph API client with automatic token refresh callback
        self.connection = MSGraphAPIOneDriveClient(
            access_token=access_token,
            refresh_callback=self._on_token_refresh_needed,
            scope_type=self.scope_type,
            selected_items=self.selected_items
        )

        self.is_connected = True

        # Fetch and persist connection identity on first connect
        self._fetch_and_store_identity()

        logger.info(f"Connected to OneDrive for connection {self.connection_id}")
        return self.connection

    def _connect_with_token_injection(self) -> str:
        """
        Modern authentication path: Use injected tokens from backend.

        Returns:
            str: Access token

        Raises:
            ValueError: If required token parameters are missing
        """
        # Load existing metadata if available
        self._token_metadata = self._load_token_metadata()

        # Check if we have a valid access token
        access_token = self.connection_data.get('access_token')
        expires_at = self.connection_data.get('expires_at')

        # Initialize metadata if not present
        if not self._token_metadata:
            if not access_token:
                raise ValueError("access_token is required for token injection authentication")

            # Parse expires_at to Unix timestamp
            expires_at_timestamp = self._parse_expires_at(expires_at)

            self._token_metadata = {
                'access_token': access_token,
                'refresh_token': self.connection_data.get('refresh_token'),
                'expires_at': expires_at_timestamp,
                'tenant_id': self.connection_data.get('tenant_id'),
                'account_id': self.connection_data.get('account_id')
            }
            self._save_token_metadata(self._token_metadata)

        # Check if token needs refresh
        if self._needs_token_refresh():
            logger.info(f"Access token expiring soon for connection {self.connection_id}, refreshing...")
            refreshed_token = self._refresh_access_token()
            if refreshed_token:
                return refreshed_token
            else:
                # Fall back to provided token if refresh fails
                logger.warning("Token refresh failed, using provided access_token")
                if not access_token:
                    raise ValueError("Token refresh failed and no valid access_token available")
                return access_token

        # Use metadata token if available, otherwise use provided
        return self._token_metadata.get('access_token') or access_token

    def _connect_with_code_exchange(self) -> str:
        """
        Legacy authentication path: Exchange authorization code for tokens.

        Returns:
            str: Access token

        Raises:
            ValueError: If required legacy parameters are missing
        """
        # Validate legacy parameters
        if not all(key in self.connection_data for key in ['client_id', 'client_secret', 'tenant_id']):
            raise ValueError("Required parameters (client_id, client_secret, tenant_id) must be provided.")

        # Initialize per-connection token cache
        cache = msal.SerializableTokenCache()

        # Load the cache from file if it exists
        cache_file = self._get_token_cache_key()
        try:
            cache_content = self.handler_storage.file_get(cache_file)
            if cache_content:
                cache.deserialize(cache_content)
        except FileNotFoundError:
            pass

        # Initialize the Microsoft Authentication Library (MSAL) app
        permissions_manager = MSGraphAPIDelegatedPermissionsManager(
            client_id=self.connection_data['client_id'],
            client_secret=self.connection_data['client_secret'],
            tenant_id=self.connection_data['tenant_id'],
            cache=cache,
            code=self.connection_data.get('code', None)
        )

        access_token = permissions_manager.get_access_token()

        # Save the cache back to file if it has changed
        if cache.has_state_changed:
            self.handler_storage.file_set(cache_file, cache.serialize().encode('utf-8'))

        return access_token

    def _on_token_refresh_needed(self) -> Optional[str]:
        """
        Callback invoked by the client when it receives a 401 Unauthorized.

        Returns:
            Optional[str]: New access token if refresh successful, None otherwise
        """
        logger.info(f"Token refresh requested by client for connection {self.connection_id}")
        new_token = self._refresh_access_token()
        if new_token and self.connection:
            # Update the client's token
            self.connection.update_access_token(new_token)
        return new_token

    def _fetch_and_store_identity(self) -> None:
        """
        Fetch user identity from Graph API and store in connection metadata table.
        This is optional metadata and failure here does not prevent the handler from working.
        """
        try:
            if not self.connection:
                return

            # Fetch user identity
            user_info = self.connection.get_user_info()
            drive_info = self.connection.get_drive_info()

            # Store in metadata (will be persisted by ConnectionMetadataTable)
            identity_data = {
                'connection_id': self.connection_id,
                'tenant_id': self.connection_data.get('tenant_id', ''),
                'account_id': user_info.get('id', ''),
                'display_name': user_info.get('displayName', ''),
                'email': user_info.get('mail') or user_info.get('userPrincipalName', ''),
                'drive_id': drive_info.get('id', ''),
                'created_at': time.time(),
                'updated_at': time.time()
            }

            # Save to storage
            identity_key = f"connection_identity_{self.connection_id}.json"
            self.handler_storage.file_set(
                identity_key,
                json.dumps(identity_data).encode('utf-8')
            )

            logger.info(f"Stored identity for connection {self.connection_id}: {identity_data.get('email')}")

        except RequestException as e:
            # API error - log details but don't fail the connection
            logger.warning(f"Failed to fetch identity metadata (API error): {e}. This does not affect file operations.")
        except Exception as e:
            # Other errors - log but don't fail the connection
            logger.warning(f"Failed to fetch and store identity: {e}. This does not affect file operations.")

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the Microsoft Graph API for Microsoft OneDrive.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)

        try:
            connection = self.connect()
            if connection.check_connection():
                response.success = True
                response.copy_storage = True
            else:
                raise RequestException("Connection check failed!")
        except (ValueError, RequestException) as known_error:
            logger.error(f'Connection check to Microsoft OneDrive failed, {known_error}!')
            response.error_message = str(known_error)
        except AuthException as error:
            response.error_message = str(error)
            response.redirect_url = error.auth_url
            return response
        except Exception as unknown_error:
            logger.error(f'Connection check to Microsoft OneDrive failed due to an unknown error, {unknown_error}!')
            response.error_message = str(unknown_error)

        self.is_connected = response.success

        return response

    def query(self, query: ASTNode) -> Response:
        """
        Executes a SQL query represented by an ASTNode and retrieves the data.

        Args:
            query (ASTNode): An ASTNode representing the SQL query to be executed.

        Raises:
            ValueError: If the file format is not supported.
            NotImplementedError: If the query type is not supported.

        Returns:
            Response: A response object containing the result of the query or an error message.
        """
        if isinstance(query, Select):
            table_name = query.from_table.parts[-1]

            # System tables for metadata and diagnostics
            if table_name == "files":
                table = ListFilesTable(self)
                df = table.select(query)
            elif table_name == "onedrive_connections":
                table = ConnectionMetadataTable(self)
                df = table.select(query)
            elif table_name == "onedrive_delta_state":
                table = DeltaStateTable(self)
                df = table.select(query)
            elif table_name == "onedrive_sync_stats":
                table = SyncStatsTable(self)
                df = table.select(query)
            # For any other table name, query the file content via the 'FileTable' class.
            # Only the supported file formats can be queried.
            else:
                extension = table_name.split('.')[-1]
                if extension not in self.supported_file_formats:
                    logger.error(f'The file format {extension} is not supported!')
                    raise ValueError(f'The file format {extension} is not supported!')

                table = FileTable(self, table_name=table_name)
                df = table.select(query)

            return Response(
                RESPONSE_TYPE.TABLE,
                data_frame=df
            )

        else:
            raise NotImplementedError(
                "Only SELECT queries are supported by the Microsoft OneDrive handler."
            )

    def native_query(self, query: Text) -> Response:
        """
        Executes a SQL query and returns the result.

        Args:
            query (str): The SQL query to be executed.

        Returns:
            Response: A response object containing the result of the query or an error message.
        """
        query_ast = parse_sql(query)
        return self.query(query_ast)

    def get_tables(self) -> Response:
        """
        Retrieves a list of tables (files) in the user's Microsoft OneDrive.
        Each file is considered a table. Only the supported file formats are included in the list.

        Returns:
            Response: A response object containing the list of tables and views, formatted as per the `Response` class.
        """
        connection = self.connect()

        # Get only the supported file formats.
        # Wrap the file names with backticks to prevent SQL syntax errors.
        supported_files = [
            f"`{file['path']}`"
            for file in connection.get_all_items()
            if file['path'].split('.')[-1] in self.supported_file_formats
        ]

        # Add the 'files' table to the list of supported tables.
        supported_files.insert(0, 'files')

        response = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=pd.DataFrame(
                supported_files,
                columns=['table_name']
            )
        )

        return response

    def get_columns(self, table_name: str) -> Response:
        """
        Retrieves column details for a specified table (file) in the user's Microsoft OneDrive.

        Args:
            table_name (Text): The name of the table for which to retrieve column information.

        Returns:
            Response: A response object containing the column details, formatted as per the `Response` class.
        """
        # Get the columns (and their data types) by querying a single row from the table.
        query = Select(
            targets=[Star()],
            from_table=Identifier(parts=[table_name]),
            limit=Constant(1)
        )

        result = self.query(query)

        response = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=pd.DataFrame(
                {
                    'column_name': result.data_frame.columns,
                    'data_type': [data_type if data_type != 'object' else 'string' for data_type in result.data_frame.dtypes]
                }
            )
        )

        return response
