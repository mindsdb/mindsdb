from mindsdb_sql_parser import parse_sql
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.utilities import log
from mindsdb.integrations.handlers.google_analytics_handler.google_analytics_tables import ConversionEventsTable
from mindsdb.integrations.handlers.google_analytics_handler.google_analytics_data_tables import (
    ReportsTable,
    RealtimeReportsTable,
    MetadataTable,
)
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
)

import json
import os

from google.analytics.admin_v1beta import AnalyticsAdminServiceClient
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials as OAuthCredentials
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError
from mindsdb.integrations.utilities.handlers.auth_utilities.google import GoogleUserOAuth2Manager
from mindsdb.integrations.utilities.handlers.auth_utilities.exceptions import AuthException

DEFAULT_SCOPES = ['https://www.googleapis.com/auth/analytics.readonly',
                  'https://www.googleapis.com/auth/analytics.edit',
                  'https://www.googleapis.com/auth/analytics'
                  ]

logger = log.getLogger(__name__)


class GoogleAnalyticsHandler(APIHandler):
    """A class for handling connections and interactions with the Google Analytics Admin API and Data API.

    This handler supports both the Admin API (for managing conversion events) and the Data API (for
    running reports and accessing analytics data).

    Authentication Methods:
        1. Service Account (JSON credentials):
            - credentials_file: Path to service account JSON file
            - credentials_json: Service account credentials as dictionary

        2. OAuth2 User Authentication:
            - Option A: Direct refresh token method
                - client_id: OAuth client ID
                - client_secret: OAuth client secret
                - refresh_token: User refresh token
                - token_uri: Token URI (optional, defaults to Google's token URI)

            - Option B: Authorization code flow
                - credentials_file or credentials_url: Path/URL to OAuth client secrets
                - code: Authorization code (obtained after user consent)

    Attributes:
        property_id (str): The Google Analytics 4 property ID.
        scopes (List[str], Optional): The scopes to use when authenticating with the Google Analytics API.

    Tables:
        Admin API:
            - conversion_events: Manage conversion events (SELECT, INSERT, UPDATE, DELETE)

        Data API:
            - reports: Run standard GA4 reports with dimensions and metrics (SELECT)
            - realtime_reports: Run realtime reports for current user activity (SELECT)
            - metadata: Fetch available dimensions and metrics (SELECT)
    """

    name = 'google_analytics'

    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        self.page_size = 500
        self.connection_args = kwargs.get('connection_data', {})
        self.handler_storage = kwargs.get('handler_storage')
        self.property_id = self.connection_args['property_id']
        if self.connection_args.get('credentials'):
            self.credentials_file = self.connection_args.pop('credentials')

        # Handle scopes - can be string or list
        scopes = self.connection_args.get('scopes', DEFAULT_SCOPES)
        if isinstance(scopes, str):
            # Convert comma-separated string to list
            self.scopes = [s.strip() for s in scopes.split(',')]
        else:
            self.scopes = scopes

        # OAuth parameters
        self.credentials_url = self.connection_args.get('credentials_url')
        self.code = self.connection_args.get('code')
        self.client_id = self.connection_args.get('client_id')
        self.client_secret = self.connection_args.get('client_secret')
        self.refresh_token = self.connection_args.get('refresh_token')
        self.token_uri = self.connection_args.get('token_uri', 'https://oauth2.googleapis.com/token')

        self.service = None  # Admin API client (for backward compatibility)
        self.admin_service = None  # Admin API client
        self.data_service = None  # Data API client
        self.is_connected = False

        # Metadata cache (shared across all tables)
        self._metadata_cache = None  # Will store: {'metrics': set(), 'dimensions': set()}
        self._metadata_cache_timestamp = None
        self._metadata_cache_ttl = 3600  # 1 hour in seconds

        # Register Admin API tables
        conversion_events = ConversionEventsTable(self)
        self.conversion_events = conversion_events
        self._register_table('conversion_events', conversion_events)

        # Register Data API tables
        reports = ReportsTable(self)
        self.reports = reports
        self._register_table('reports', reports)

        realtime_reports = RealtimeReportsTable(self)
        self.realtime_reports = realtime_reports
        self._register_table('realtime_reports', realtime_reports)

        metadata = MetadataTable(self)
        self.metadata = metadata
        self._register_table('metadata', metadata)

    def _store_credentials(self, credentials_data: dict) -> None:
        """
        Store credentials securely in encrypted storage

        Args:
            credentials_data: Service account credentials as dictionary
        """
        if not hasattr(self, 'handler_storage') or not self.handler_storage:
            return

        try:
            self.handler_storage.encrypted_json_set("ga_credentials", credentials_data)
        except Exception as e:
            logger.warning(f"Failed to store credentials: {e}")

    def _load_stored_credentials(self) -> dict:
        """
        Load stored credentials from encrypted storage

        Returns:
            dict: Stored credentials or None if not found
        """
        if not hasattr(self, 'handler_storage') or not self.handler_storage:
            return None

        try:
            return self.handler_storage.encrypted_json_get("ga_credentials")
        except Exception as e:
            logger.debug(f"No stored credentials found: {e}")
            return None

    def _get_creds_json(self):
        # First, try to load from encrypted storage
        stored_creds = self._load_stored_credentials()
        if stored_creds:
            return stored_creds

        # If not in storage, load from connection args and store securely
        if 'credentials_file' in self.connection_args:
            if os.path.isfile(self.connection_args['credentials_file']) is False:
                raise Exception("credentials_file must be a file path")
            with open(self.connection_args['credentials_file']) as source:
                info = json.load(source)
            # Store credentials for future use
            self._store_credentials(info)
            return info
        elif 'credentials_json' in self.connection_args:
            info = json.loads(self.connection_args['credentials_json'])
            if not isinstance(info, dict):
                raise Exception("credentials_json has to be dict")
            info['private_key'] = info['private_key'].replace('\\n', '\n')
            # Store credentials for future use
            self._store_credentials(info)
            return info
        else:
            raise Exception('Connection args have to content ether credentials_file or credentials_json')

    def create_connection(self):
        """
        Create connection to Google Analytics using either OAuth2 or Service Account authentication.

        OAuth2 is attempted first if refresh_token is provided, then authorization code flow,
        and finally falls back to service account credentials.

        Returns:
            tuple: (admin_client, data_client) - Admin API and Data API clients

        Raises:
            AuthException: When OAuth authorization is required (user needs to authorize)
            Exception: For other authentication errors
        """
        creds = None

        # Try OAuth2 with refresh token (direct method)
        if self.refresh_token:
            logger.info("Authenticating with OAuth2 using refresh token")
            try:
                # Note: scopes are not passed here as they are already embedded in the refresh token
                creds = OAuthCredentials(
                    token=None,
                    refresh_token=self.refresh_token,
                    token_uri=self.token_uri,
                    client_id=self.client_id,
                    client_secret=self.client_secret
                )
                # Refresh to get access token
                creds.refresh(Request())
                logger.info("OAuth2 authentication successful with refresh token")
            except Exception as e:
                logger.error(f"OAuth2 authentication with refresh token failed: {e}")
                raise Exception(f"OAuth2 authentication failed: {e}")

        # Try OAuth2 with authorization code flow
        elif self.credentials_url or (hasattr(self, 'credentials_file') and self.credentials_file and not self._is_service_account_file()):
            logger.info("Authenticating with OAuth2 using authorization code flow")
            try:
                google_oauth2_manager = GoogleUserOAuth2Manager(
                    self.handler_storage,
                    self.scopes,
                    getattr(self, 'credentials_file', None),
                    self.credentials_url,
                    self.code
                )
                creds = google_oauth2_manager.get_oauth2_credentials()
                logger.info("OAuth2 authentication successful with authorization code flow")
            except AuthException:
                # Re-raise AuthException so it can be caught in check_connection
                raise
            except Exception as e:
                logger.error(f"OAuth2 authentication with code flow failed: {e}")
                raise Exception(f"OAuth2 authentication failed: {e}")

        # Fall back to service account authentication
        else:
            logger.info("Authenticating with Service Account")
            try:
                info = self._get_creds_json()
                creds = service_account.Credentials.from_service_account_info(info=info, scopes=self.scopes)

                if not creds or not creds.valid:
                    if creds and creds.expired and creds.refresh_token:
                        creds.refresh(Request())

                logger.info("Service Account authentication successful")
            except Exception as e:
                logger.error(f"Service Account authentication failed: {e}")
                raise Exception(f"Service Account authentication failed: {e}")

        # Ensure we have valid credentials
        if not creds:
            raise Exception("Failed to create credentials: no authentication method succeeded")

        # Create both Admin API and Data API clients with same credentials
        admin_client = AnalyticsAdminServiceClient(credentials=creds)
        data_client = BetaAnalyticsDataClient(credentials=creds)

        return admin_client, data_client

    def _is_service_account_file(self):
        """
        Check if the credentials_file contains service account credentials.

        Returns:
            bool: True if service account, False if OAuth client secrets
        """
        if not hasattr(self, 'credentials_file') or not self.credentials_file:
            return False

        try:
            with open(self.credentials_file, 'r') as f:
                data = json.load(f)
                # Service account files have 'type': 'service_account'
                # OAuth client secrets have 'installed' or 'web' keys
                return data.get('type') == 'service_account'
        except Exception as e:
            logger.warning(f"Could not determine credentials file type: {e}")
            return True  # Assume service account if can't determine

    def connect(self):
        """
        Authenticate with the Google Analytics Admin API and Data API using the credential file.

        Returns
        -------
        service: object
            The authenticated Google Analytics Admin API service object (for backward compatibility).
        """
        if self.is_connected is True:
            return self.service

        self.admin_service, self.data_service = self.create_connection()
        self.service = self.admin_service  # For backward compatibility
        self.is_connected = True

        return self.service

    def get_metadata_cache(self) -> dict:
        """
        Get cached metadata about dimensions and metrics.
        Caches for 1 hour to avoid repeated API calls.

        Returns:
            dict: {
                'metrics': set of metric api_names (API format with colons),
                'dimensions': set of dimension api_names (API format with colons),
                'column_to_api': dict mapping sanitized column names to API names,
                'api_to_column': dict mapping API names to sanitized column names
            }
        """
        import time
        from google.analytics.data_v1beta.types import GetMetadataRequest

        # Check if cache is valid
        current_time = time.time()
        if (self._metadata_cache is not None and
            self._metadata_cache_timestamp is not None and
            current_time - self._metadata_cache_timestamp < self._metadata_cache_ttl):
            return self._metadata_cache

        # Fetch fresh metadata
        try:
            self.connect()
            request = GetMetadataRequest(
                name=f"properties/{self.property_id}/metadata"
            )
            response = self.data_service.get_metadata(request)

            # Build cache with clean API names and bidirectional mappings
            metrics = set()
            dimensions = set()
            column_to_api = {}
            api_to_column = {}

            # Process metrics
            for metric in response.metrics:
                api_name = metric.api_name
                column_name = api_name.replace(':', '_')  # Sanitize for SQL

                metrics.add(api_name)  # Store API name only (clean set)
                column_to_api[column_name] = api_name
                api_to_column[api_name] = column_name

            # Process dimensions
            for dimension in response.dimensions:
                api_name = dimension.api_name
                column_name = api_name.replace(':', '_')  # Sanitize for SQL

                dimensions.add(api_name)  # Store API name only (clean set)
                column_to_api[column_name] = api_name
                api_to_column[api_name] = column_name

            self._metadata_cache = {
                'metrics': metrics,
                'dimensions': dimensions,
                'column_to_api': column_to_api,
                'api_to_column': api_to_column
            }
            self._metadata_cache_timestamp = current_time

            logger.info(
                f"Metadata cache refreshed: {len(metrics)} metrics, "
                f"{len(dimensions)} dimensions, {len(column_to_api)} mappings"
            )
            return self._metadata_cache

        except Exception as e:
            logger.warning(f"Failed to fetch metadata for cache: {e}")
            # Return empty cache on error
            return {
                'metrics': set(),
                'dimensions': set(),
                'column_to_api': {},
                'api_to_column': {}
            }

    def invalidate_metadata_cache(self):
        """
        Invalidate the metadata cache to force refresh on next access.
        Useful when property configuration changes (new custom dimensions/metrics added).
        """
        self._metadata_cache = None
        self._metadata_cache_timestamp = None
        logger.info("Metadata cache invalidated")

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler.

        Returns
        -------
        response
            Status confirmation with optional auth_url for OAuth flow
        """
        response = StatusResponse(False)

        try:
            # Call the Google Analytics API
            service = self.connect()
            result = service.list_conversion_events(parent=f'properties/{self.property_id}')

            if result is not None:
                response.success = True
        except AuthException as e:
            # OAuth authorization required - return auth URL to user
            response.error_message = str(e)
            logger.info(f"OAuth authorization required: {e}")
        except HttpError as error:
            response.error_message = f'Error connecting to Google Analytics api: {error}.'
            logger.error(response.error_message)
        except Exception as error:
            response.error_message = f'Error connecting to Google Analytics: {error}.'
            logger.error(response.error_message)

        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def native_query(self, query_string: str = None) -> Response:
        ast = parse_sql(query_string)

        return self.query(ast)

    def get_api_url(self, endpoint):
        return f'{endpoint}/{self.property_id}'
