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
        self.redirect_uri = self.connection_args['redirect_uri']
        self.refresh_token = self.connection_args['refresh_token']
        self.environment = self.connection_args.get('environment', 'production')
        self.auth_type = self.connection_args.get('auth_type', 'microsoft')

        self.authorization_data = None
        self.campaign_service = None
        self.customer_service = None
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
        """Refresh OAuth tokens and build AuthorizationData for the bingads SDK.

        Supports two auth flows:
        - 'microsoft': Azure AD OAuth — token endpoint is login.microsoftonline.com
        - 'google': Google OAuth (Microsoft Ads accounts signed in via Google) —
          token endpoint is oauth2.googleapis.com; Google does not always return a
          new refresh token on refresh, so the original is kept when absent.
        """
        import requests as http_requests
        from bingads import AuthorizationData, OAuthTokens
        from bingads import GoogleOAuthWebAuthCodeGrant, OAuthWebAuthCodeGrant

        if self.auth_type == 'google':
            token_url = 'https://oauth2.googleapis.com/token'
            post_data = {
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'grant_type': 'refresh_token',
                'refresh_token': self._get_refresh_token(),
            }
        else:
            token_url = (
                'https://login.windows-ppe.net/consumers/oauth2/v2.0/token'
                if self.environment == 'sandbox'
                else 'https://login.microsoftonline.com/common/oauth2/v2.0/token'
            )
            post_data = {
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'grant_type': 'refresh_token',
                'refresh_token': self._get_refresh_token(),
                'redirect_uri': self.redirect_uri,
                'scope': 'https://ads.microsoft.com/msads.manage offline_access',
            }

        resp = http_requests.post(token_url, data=post_data)
        if not resp.ok:
            err = resp.json()
            raise Exception(
                f"error_code: {err.get('error')}, "
                f"error_description: {err.get('error_description')}"
            )
        token_data = resp.json()
        # Google does not always issue a new refresh token — keep the original when absent
        new_refresh_token = token_data.get('refresh_token') or self._get_refresh_token()

        if self.handler_storage and token_data.get('refresh_token'):
            try:
                self.handler_storage.encrypted_json_set('microsoft_ads_tokens', {
                    'refresh_token': new_refresh_token,
                })
            except Exception as e:
                logger.warning(f"Failed to persist refresh token: {e}")

        if self.auth_type == 'google':
            authentication = GoogleOAuthWebAuthCodeGrant(
                client_id=self.client_id,
                client_secret=self.client_secret,
                redirect_url=self.redirect_uri,
            )
        else:
            authentication = OAuthWebAuthCodeGrant(
                client_id=self.client_id,
                client_secret=self.client_secret,
                redirection_uri=self.redirect_uri,
                env=self.environment,
            )
        authentication._oauth_tokens = OAuthTokens(
            access_token=token_data['access_token'],
            access_token_expires_in_seconds=int(token_data['expires_in']),
            refresh_token=new_refresh_token,
        )

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

        self.customer_service = ServiceClient(
            service='CustomerManagementService',
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
            self.customer_service.GetUser(UserId=None)
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
