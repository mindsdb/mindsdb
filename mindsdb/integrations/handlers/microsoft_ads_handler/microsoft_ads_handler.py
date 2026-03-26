from mindsdb_sql_parser import parse_sql

from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
)
from mindsdb.utilities import log

from mindsdb.integrations.handlers.microsoft_ads_handler.microsoft_ads_tables import (
    CampaignsTable,
    AdGroupsTable,
    AdsTable,
    KeywordsTable,
    CampaignPerformanceTable,
    SearchTermsTable,
)

logger = log.getLogger(__name__)


class MicrosoftAdsHandler(APIHandler):
    """Handler for Microsoft Advertising API.

    Uses the bingads Python SDK (v13) which wraps the SOAP-based API.
    Authentication is via Azure AD OAuth with refresh token.
    """

    name = 'microsoft_ads'

    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        self.connection_args = kwargs.get('connection_data', {})
        self.handler_storage = kwargs.get('handler_storage')

        self.developer_token = self.connection_args['developer_token']
        self.account_id = self.connection_args['account_id']
        self.customer_id = self.connection_args['customer_id']
        self.client_id = self.connection_args['client_id']
        self.client_secret = self.connection_args['client_secret']
        self.refresh_token = self.connection_args['refresh_token']
        self.environment = self.connection_args.get('environment', 'production')

        self.authorization_data = None
        self.campaign_service = None
        self.reporting_service = None
        self.is_connected = False

        # Register entity tables
        self._register_table('campaigns', CampaignsTable(self))
        self._register_table('ad_groups', AdGroupsTable(self))
        self._register_table('ads', AdsTable(self))
        self._register_table('keywords', KeywordsTable(self))

        # Register report tables
        self._register_table('campaign_performance', CampaignPerformanceTable(self))
        self._register_table('search_terms', SearchTermsTable(self))

    def _get_refresh_token(self):
        """Get the latest refresh token — from storage if previously refreshed, else from connection_args."""
        if self.handler_storage:
            try:
                stored = self.handler_storage.encrypted_json_get('microsoft_ads_tokens')
                if stored and stored.get('refresh_token'):
                    return stored['refresh_token']
            except Exception:
                pass
        return self.refresh_token

    def _build_authorization_data(self):
        """Refresh OAuth tokens and build AuthorizationData for the bingads SDK."""
        from bingads import AuthorizationData, OAuthDesktopMobileAuthCodeGrant

        authentication = OAuthDesktopMobileAuthCodeGrant(
            client_id=self.client_id,
            client_secret=self.client_secret,
            env=self.environment,
        )

        refresh_token = self._get_refresh_token()
        authentication.request_oauth_tokens_by_refresh_token(refresh_token)

        # Persist the new refresh token for future use
        if self.handler_storage:
            try:
                self.handler_storage.encrypted_json_set('microsoft_ads_tokens', {
                    'refresh_token': authentication.oauth_tokens.refresh_token,
                })
            except Exception as e:
                logger.warning(f"Failed to persist refresh token: {e}")

        authorization_data = AuthorizationData(
            account_id=self.account_id,
            customer_id=self.customer_id,
            developer_token=self.developer_token,
            authentication=authentication,
        )
        return authorization_data

    def connect(self):
        """Authenticate and create service clients."""
        if self.is_connected:
            return self.campaign_service

        from bingads import ServiceClient

        self.authorization_data = self._build_authorization_data()

        self.campaign_service = ServiceClient(
            service='CampaignManagementService',
            version=13,
            authorization_data=self.authorization_data,
            environment=self.environment,
        )

        self.reporting_service = ServiceClient(
            service='ReportingService',
            version=13,
            authorization_data=self.authorization_data,
            environment=self.environment,
        )

        self.is_connected = True
        return self.campaign_service

    def check_connection(self) -> StatusResponse:
        """Verify credentials by calling GetUser."""
        response = StatusResponse(False)
        try:
            self.connect()
            self.campaign_service.GetUser(UserId=None)
            response.success = True
        except Exception as e:
            response.error_message = f'Error connecting to Microsoft Advertising: {e}'
            logger.error(response.error_message)
            if self.is_connected:
                self.is_connected = False
        return response

    def native_query(self, query_string: str = None) -> Response:
        ast = parse_sql(query_string)
        return self.query(ast)
