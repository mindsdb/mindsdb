from mindsdb_sql_parser import parse_sql
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
)

import json
import os

from google.oauth2 import service_account
from google.oauth2.credentials import Credentials as OAuthCredentials
from google.auth.transport.requests import Request
from mindsdb.integrations.utilities.handlers.auth_utilities.google import GoogleUserOAuth2Manager
from mindsdb.integrations.utilities.handlers.auth_utilities.exceptions import AuthException

DEFAULT_SCOPES = ['https://www.googleapis.com/auth/adwords']

logger = log.getLogger(__name__)


class GoogleAdsHandler(APIHandler):
    """Handler for the Google Ads API.

    Supports OAuth2 (refresh token or authorization code flow) and service account auth.
    All tables use Pattern A: fetch raw data via GAQL, return full DataFrame, DuckDB handles
    complex expressions (CASE WHEN, aggregations, arithmetic, etc.).

    Connection args:
        customer_id       (required) Google Ads customer ID (123-456-7890 or 1234567890)
        developer_token   (required) From Google Ads UI > Tools & Settings > API Center
        login_customer_id (optional) MCC manager account ID
        client_id / client_secret / refresh_token  — OAuth direct method
        credentials_file / credentials_url / code  — OAuth code flow
        credentials_file / credentials_json        — Service account

    Tables:
        campaigns                    Entity: campaigns in the account
        ad_groups                    Entity: ad groups within campaigns
        ads                          Entity: ads within ad groups
        keywords                     Entity: keywords within ad groups
        campaign_performance         Report: daily metrics per campaign (requires start_date, end_date)
        search_terms                 Report: search term performance (requires start_date, end_date)
        languages                    Lookup: language criterion IDs (from language_constant API)
        geo_targets                  Lookup: geo target criterion IDs (from geo_target_constant API)
        keyword_ideas                Keyword Planner: generate keyword ideas (requires keywords or url)
        keyword_historical_metrics   Keyword Planner: historical metrics for keywords (requires keywords)
        keyword_forecast_metrics     Keyword Planner: forecast metrics (requires keywords + max_cpc_bid_micros)
    """

    name = 'google_ads'

    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        self.connection_args = kwargs.get('connection_data', {})
        self.handler_storage = kwargs.get('handler_storage')

        # Normalize customer_id: strip dashes so the API always gets digits only
        raw_customer_id = self.connection_args['customer_id']
        self.customer_id = raw_customer_id.replace('-', '').strip()

        self.developer_token = self.connection_args['developer_token']
        self.login_customer_id = self.connection_args.get('login_customer_id')
        if self.login_customer_id:
            self.login_customer_id = self.login_customer_id.replace('-', '').strip()

        if self.connection_args.get('credentials'):
            self.credentials_file = self.connection_args.pop('credentials')

        scopes = self.connection_args.get('scopes', DEFAULT_SCOPES)
        if isinstance(scopes, str):
            self.scopes = [s.strip() for s in scopes.split(',')]
        else:
            self.scopes = scopes

        self.credentials_url = self.connection_args.get('credentials_url')
        self.code = self.connection_args.get('code')
        self.client_id = self.connection_args.get('client_id')
        self.client_secret = self.connection_args.get('client_secret')
        self.refresh_token = self.connection_args.get('refresh_token')
        self.token_uri = self.connection_args.get('token_uri', 'https://oauth2.googleapis.com/token')

        self.client = None
        self.is_connected = False

        # Lookup caches (shared across tables, lazy-loaded with 24h TTL)
        self._languages_cache = None
        self._languages_cache_timestamp = None
        self._geo_targets_cache = None
        self._geo_targets_cache_timestamp = None
        self._lookup_cache_ttl = 86400  # 24 hours — these rarely change

        # Import here so the handler can be loaded even if google-ads isn't installed yet
        # (import_error in __init__.py will catch it at registration time)
        from mindsdb.integrations.handlers.google_ads_handler.google_ads_tables import (
            CampaignsTable,
            AdGroupsTable,
            AdsTable,
            KeywordsTable,
            CampaignPerformanceTable,
            SearchTermsTable,
            LanguagesTable,
            GeoTargetsTable,
            KeywordIdeasTable,
            KeywordHistoricalMetricsTable,
            KeywordForecastMetricsTable,
        )

        self._register_table('campaigns', CampaignsTable(self))
        self._register_table('ad_groups', AdGroupsTable(self))
        self._register_table('ads', AdsTable(self))
        self._register_table('keywords', KeywordsTable(self))
        self._register_table('campaign_performance', CampaignPerformanceTable(self))
        self._register_table('search_terms', SearchTermsTable(self))
        self._register_table('languages', LanguagesTable(self))
        self._register_table('geo_targets', GeoTargetsTable(self))
        self._register_table('keyword_ideas', KeywordIdeasTable(self))
        self._register_table('keyword_historical_metrics', KeywordHistoricalMetricsTable(self))
        self._register_table('keyword_forecast_metrics', KeywordForecastMetricsTable(self))

    def _store_credentials(self, credentials_data: dict) -> None:
        if not hasattr(self, 'handler_storage') or not self.handler_storage:
            return
        try:
            self.handler_storage.encrypted_json_set('google_ads_credentials', credentials_data)
        except Exception as e:
            logger.warning(f"Failed to store credentials: {e}")

    def _load_stored_credentials(self) -> dict:
        if not hasattr(self, 'handler_storage') or not self.handler_storage:
            return None
        try:
            return self.handler_storage.encrypted_json_get('google_ads_credentials')
        except Exception as e:
            logger.debug(f"No stored credentials found: {e}")
            return None

    def _get_creds_json(self):
        stored_creds = self._load_stored_credentials()
        if stored_creds:
            return stored_creds

        if 'credentials_file' in self.connection_args:
            if not os.path.isfile(self.connection_args['credentials_file']):
                raise Exception("credentials_file must be a file path")
            with open(self.connection_args['credentials_file']) as f:
                info = json.load(f)
            self._store_credentials(info)
            return info
        elif 'credentials_json' in self.connection_args:
            info = json.loads(self.connection_args['credentials_json'])
            if not isinstance(info, dict):
                raise Exception("credentials_json must be a dict")
            info['private_key'] = info['private_key'].replace('\\n', '\n')
            self._store_credentials(info)
            return info
        else:
            raise Exception('Connection args must contain credentials_file or credentials_json')

    def _is_service_account_file(self):
        if not hasattr(self, 'credentials_file') or not self.credentials_file:
            return False
        try:
            with open(self.credentials_file, 'r') as f:
                data = json.load(f)
                return data.get('type') == 'service_account'
        except Exception as e:
            logger.warning(f"Could not determine credentials file type: {e}")
            return True

    def create_connection(self):
        """Build OAuth/service-account credentials and return a GoogleAdsClient.

        Priority:
          1. refresh_token  → OAuthCredentials direct
          2. credentials_url / OAuth client secrets file + code  → GoogleUserOAuth2Manager
          3. credentials_file / credentials_json  → service account
        """
        from google.ads.googleads.client import GoogleAdsClient

        creds = None

        if self.refresh_token:
            logger.info("Authenticating with OAuth2 using refresh token")
            try:
                creds = OAuthCredentials(
                    token=None,
                    refresh_token=self.refresh_token,
                    token_uri=self.token_uri,
                    client_id=self.client_id,
                    client_secret=self.client_secret,
                )
                creds.refresh(Request())
                logger.info("OAuth2 authentication successful with refresh token")
            except Exception as e:
                logger.error(f"OAuth2 refresh token auth failed: {e}")
                raise Exception(f"OAuth2 authentication failed: {e}")

        elif self.credentials_url or (
            hasattr(self, 'credentials_file')
            and self.credentials_file
            and not self._is_service_account_file()
        ):
            logger.info("Authenticating with OAuth2 using authorization code flow")
            try:
                google_oauth2_manager = GoogleUserOAuth2Manager(
                    self.handler_storage,
                    self.scopes,
                    getattr(self, 'credentials_file', None),
                    self.credentials_url,
                    self.code,
                )
                creds = google_oauth2_manager.get_oauth2_credentials()
                logger.info("OAuth2 authentication successful with authorization code flow")
            except AuthException:
                raise
            except Exception as e:
                logger.error(f"OAuth2 code flow auth failed: {e}")
                raise Exception(f"OAuth2 authentication failed: {e}")

        else:
            logger.info("Authenticating with Service Account")
            try:
                info = self._get_creds_json()
                creds = service_account.Credentials.from_service_account_info(
                    info=info, scopes=self.scopes
                )
                if not creds.valid:
                    if creds.expired and creds.refresh_token:
                        creds.refresh(Request())
                logger.info("Service Account authentication successful")
            except Exception as e:
                logger.error(f"Service Account authentication failed: {e}")
                raise Exception(f"Service Account authentication failed: {e}")

        if not creds:
            raise Exception("Failed to create credentials: no authentication method succeeded")

        client = GoogleAdsClient(
            credentials=creds,
            developer_token=self.developer_token,
            login_customer_id=self.login_customer_id,
        )
        return client

    def connect(self):
        if self.is_connected:
            return self.client

        self.client = self.create_connection()
        self.is_connected = True
        return self.client

    def check_connection(self) -> StatusResponse:
        response = StatusResponse(False)

        try:
            client = self.connect()
            ga_service = client.get_service("GoogleAdsService")
            result = ga_service.search(
                customer_id=self.customer_id,
                query="SELECT customer.id FROM customer LIMIT 1",
            )
            # Consume the first row to confirm connectivity
            next(iter(result), None)
            response.success = True
        except AuthException as e:
            response.error_message = str(e)
            logger.info(f"OAuth authorization required: {e}")
        except Exception as error:
            response.error_message = f'Error connecting to Google Ads: {error}.'
            logger.error(response.error_message)

        if not response.success and self.is_connected:
            self.is_connected = False

        return response

    # ------------------------------------------------------------------
    # Lookup caches (languages / geo targets)
    # ------------------------------------------------------------------

    def get_languages_cache(self):
        """Return cached languages DataFrame, refreshing if stale (24h TTL)."""
        import time

        current_time = time.time()
        if (self._languages_cache is not None
                and self._languages_cache_timestamp is not None
                and current_time - self._languages_cache_timestamp < self._lookup_cache_ttl):
            return self._languages_cache

        try:
            self.connect()
            ga_service = self.client.get_service("GoogleAdsService")
            response = ga_service.search(
                customer_id=self.customer_id,
                query=(
                    "SELECT language_constant.id, language_constant.name, "
                    "language_constant.code, language_constant.targetable "
                    "FROM language_constant"
                ),
            )
            import pandas as pd
            rows = []
            for row in response:
                lc = row.language_constant
                rows.append({
                    'id': str(lc.id),
                    'name': lc.name,
                    'code': lc.code,
                    'targetable': lc.targetable,
                })
            self._languages_cache = pd.DataFrame(
                rows, columns=['id', 'name', 'code', 'targetable']
            ) if rows else pd.DataFrame(columns=['id', 'name', 'code', 'targetable'])
            self._languages_cache_timestamp = current_time
            logger.info(f"Languages cache refreshed: {len(rows)} entries")
        except Exception as e:
            logger.warning(f"Failed to refresh languages cache: {e}")
            if self._languages_cache is None:
                import pandas as pd
                self._languages_cache = pd.DataFrame(
                    columns=['id', 'name', 'code', 'targetable']
                )

        return self._languages_cache

    def get_geo_targets_cache(self):
        """Return cached geo targets DataFrame, refreshing if stale (24h TTL)."""
        import time

        current_time = time.time()
        if (self._geo_targets_cache is not None
                and self._geo_targets_cache_timestamp is not None
                and current_time - self._geo_targets_cache_timestamp < self._lookup_cache_ttl):
            return self._geo_targets_cache

        try:
            self.connect()
            from mindsdb.integrations.handlers.google_ads_handler.google_ads_tables import _enum_name
            ga_service = self.client.get_service("GoogleAdsService")
            response = ga_service.search(
                customer_id=self.customer_id,
                query=(
                    "SELECT geo_target_constant.id, geo_target_constant.name, "
                    "geo_target_constant.canonical_name, geo_target_constant.country_code, "
                    "geo_target_constant.target_type, geo_target_constant.status "
                    "FROM geo_target_constant"
                ),
            )
            import pandas as pd
            cols = ['id', 'name', 'canonical_name', 'country_code', 'target_type', 'status']
            rows = []
            for row in response:
                gt = row.geo_target_constant
                rows.append({
                    'id': str(gt.id),
                    'name': gt.name,
                    'canonical_name': gt.canonical_name,
                    'country_code': gt.country_code,
                    'target_type': gt.target_type,
                    'status': _enum_name(gt.status),
                })
            self._geo_targets_cache = pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)
            self._geo_targets_cache_timestamp = current_time
            logger.info(f"Geo targets cache refreshed: {len(rows)} entries")
        except Exception as e:
            logger.warning(f"Failed to refresh geo targets cache: {e}")
            if self._geo_targets_cache is None:
                import pandas as pd
                cols = ['id', 'name', 'canonical_name', 'country_code', 'target_type', 'status']
                self._geo_targets_cache = pd.DataFrame(columns=cols)

        return self._geo_targets_cache

    def invalidate_lookup_caches(self):
        """Force refresh on next access."""
        self._languages_cache = None
        self._languages_cache_timestamp = None
        self._geo_targets_cache = None
        self._geo_targets_cache_timestamp = None
        logger.info("Lookup caches invalidated")

    def native_query(self, query_string: str = None) -> Response:
        ast = parse_sql(query_string)
        return self.query(ast)
