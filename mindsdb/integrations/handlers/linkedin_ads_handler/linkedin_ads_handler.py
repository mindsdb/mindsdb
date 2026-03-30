from __future__ import annotations

from typing import Any

from mindsdb_sql_parser import parse_sql

from mindsdb.integrations.handlers.linkedin_ads_handler.linkedin_ads_auth import LinkedInAdsAuthManager
from mindsdb.integrations.handlers.linkedin_ads_handler.linkedin_ads_client import LinkedInAdsClient
from mindsdb.integrations.handlers.linkedin_ads_handler import linkedin_ads_utils as utils
from mindsdb.integrations.handlers.linkedin_ads_handler.tables import (
    CampaignAnalyticsTable,
    CampaignGroupsTable,
    CampaignsTable,
    CreativesTable,
)
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import HandlerResponse as Response
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse
from mindsdb.integrations.utilities.sql_utils import FilterCondition, SortColumn
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class LinkedInAdsHandler(APIHandler):
    """Handler for LinkedIn Ads account-scoped discovery and metadata tables.

    Delegates responsibilities to:
    - LinkedInAdsAuthManager: Token lifecycle management
    - LinkedInAdsClient: API communication and pagination
    - linkedin_ads_utils: Data normalization and helper functions
    """

    name = "linkedin_ads"

    # API Endpoints
    AD_ACCOUNTS_ENDPOINT = "https://api.linkedin.com/rest/adAccounts"
    CAMPAIGNS_ENDPOINT = "https://api.linkedin.com/rest/adAccounts/{account_id}/adCampaigns"
    CAMPAIGN_GROUPS_ENDPOINT = "https://api.linkedin.com/rest/adAccounts/{account_id}/adCampaignGroups"
    CREATIVES_ENDPOINT = "https://api.linkedin.com/rest/adAccounts/{account_id}/creatives"
    AD_ANALYTICS_ENDPOINT = "https://api.linkedin.com/rest/adAnalytics"

    # Configuration
    DEFAULT_API_VERSION = "202602"
    DEFAULT_ANALYTICS_LOOKBACK_DAYS = 30
    SUPPORTED_TIME_GRANULARITIES = {"ALL", "DAILY", "MONTHLY"}
    CAMPAIGN_ANALYTICS_FIELDS = (
        "dateRange",
        "pivotValues",
        "impressions",
        "clicks",
        "landingPageClicks",
        "likes",
        "shares",
        "costInLocalCurrency",
        "externalWebsiteConversions",
    )

    # Filter mappings
    CAMPAIGN_SEARCH_FILTERS = {
        "id": "id",
        "status": "status",
        "name": "name",
        "test": "test",
    }
    CAMPAIGN_GROUP_SEARCH_FILTERS = {
        "id": "id",
        "status": "status",
        "name": "name",
        "test": "test",
    }
    CREATIVE_CRITERIA_FILTERS = {
        "id": "creatives",
        "creative_id": "creatives",
        "campaign_urn": "campaigns",
        "campaign_id": "campaigns",
        "content_reference": "contentReferences",
        "content_reference_id": "contentReferences",
        "intended_status": "intendedStatuses",
        "is_test": "isTestAccount",
    }

    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        self.connection_data = kwargs.get("connection_data", {})
        self.handler_storage = kwargs.get("handler_storage")

        # Extract connection parameters
        self.account_id = self.connection_data.get("account_id")
        access_token = self.connection_data.get("access_token")
        refresh_token = self.connection_data.get("refresh_token")
        client_id = self.connection_data.get("client_id")
        client_secret = self.connection_data.get("client_secret")
        api_version = self.connection_data.get("api_version", self.DEFAULT_API_VERSION)

        # Initialize auth manager and client
        self.auth_manager = LinkedInAdsAuthManager(
            handler_storage=self.handler_storage,
            client_id=client_id,
            client_secret=client_secret,
            access_token=access_token,
            refresh_token=refresh_token,
        )
        self.client = LinkedInAdsClient(
            auth_manager=self.auth_manager,
            api_version=api_version,
        )

        # Register tables
        self._register_table("campaigns", CampaignsTable(self))
        self._register_table("campaign_groups", CampaignGroupsTable(self))
        self._register_table("creatives", CreativesTable(self))
        self._register_table("campaign_analytics", CampaignAnalyticsTable(self))

    def connect(self):
        """Connect to LinkedIn Ads API."""
        if not self.account_id:
            raise ValueError("account_id is required")
        self.client.connect()
        self.is_connected = True

    def disconnect(self):
        """Disconnect from LinkedIn Ads API."""
        self.client.disconnect()
        super().disconnect()

    def check_connection(self) -> StatusResponse:
        """Check connection to LinkedIn Ads API by fetching account details.

        Returns:
            StatusResponse with success status and any error message
        """
        response = StatusResponse(success=False)
        try:
            self.connect()
            # Verify account is accessible
            self.client.request_json("GET", f"{self.AD_ACCOUNTS_ENDPOINT}/{self.account_id}")
            response.success = True
        except Exception as exc:  # noqa: BLE001
            logger.error("Error connecting to LinkedIn Ads: %s", exc)
            response.success = False
            response.error_message = str(exc)
            self.disconnect()
        return response

    def native_query(self, query: str = None) -> Response:
        """Execute a native SQL query.

        Args:
            query: SQL query string

        Returns:
            HandlerResponse with query results
        """
        ast = parse_sql(query)
        return self.query(ast)

    def fetch_campaigns(
        self,
        conditions: list[FilterCondition] | None = None,
        limit: int | None = None,
        sort: list[SortColumn] | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch campaigns from LinkedIn Ads API.

        Args:
            conditions: Filter conditions from SQL query
            limit: Maximum number of results
            sort: Sort columns

        Returns:
            List of normalized campaign dictionaries
        """
        endpoint = self.CAMPAIGNS_ENDPOINT.format(account_id=self.account_id)
        search_filters = utils.extract_search_filters(conditions, self.CAMPAIGN_SEARCH_FILTERS)
        search_filters = utils.normalize_search_id_filters(search_filters, "urn:li:sponsoredCampaign:")
        elements = self.client.fetch_collection(endpoint, search_filters=search_filters, limit=limit, sort=sort)
        return [utils.normalize_campaign(item) for item in elements]

    def fetch_campaign_groups(
        self,
        conditions: list[FilterCondition] | None = None,
        limit: int | None = None,
        sort: list[SortColumn] | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch campaign groups from LinkedIn Ads API.

        Uses batch endpoint for ID-based queries, falls back to search for other filters.

        Args:
            conditions: Filter conditions from SQL query
            limit: Maximum number of results
            sort: Sort columns

        Returns:
            List of normalized campaign group dictionaries
        """
        endpoint = self.CAMPAIGN_GROUPS_ENDPOINT.format(account_id=self.account_id)
        search_filters = utils.extract_search_filters(conditions, self.CAMPAIGN_GROUP_SEARCH_FILTERS)
        id_values = utils.extract_search_id_values(search_filters)

        # Use batch endpoint for ID queries
        if id_values:
            # Extract numeric IDs from URNs
            numeric_ids = [utils.extract_urn_id(id_val) for id_val in id_values]
            elements = self.client.fetch_by_ids(endpoint, numeric_ids)
            rows = [utils.normalize_campaign_group(item) for item in elements]
            # Apply remaining filters locally
            rows = utils.filter_rows_locally(rows, search_filters)
            rows = utils.sort_rows_locally(rows, sort)
            return rows[:limit] if limit is not None else rows

        # Use search endpoint for other queries
        elements = self.client.fetch_collection(endpoint, search_filters=search_filters, limit=limit, sort=sort)
        return [utils.normalize_campaign_group(item) for item in elements]

    def fetch_creatives(
        self,
        conditions: list[FilterCondition] | None = None,
        limit: int | None = None,
        sort: list[SortColumn] | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch creatives from LinkedIn Ads API.

        Args:
            conditions: Filter conditions from SQL query
            limit: Maximum number of results
            sort: Sort columns

        Returns:
            List of normalized creative dictionaries
        """
        endpoint = self.CREATIVES_ENDPOINT.format(account_id=self.account_id)
        params = utils.build_creative_params(conditions, self.CREATIVE_CRITERIA_FILTERS, sort=sort)

        # Mark sort as applied since we pass it to API
        if sort:
            for sort_column in sort:
                sort_column.applied = True

        elements = self.client.fetch_collection(
            endpoint,
            params=params,
            limit=limit,
            sort=sort,
            extra_headers={"X-RestLi-Method": "FINDER"},
        )
        return [utils.normalize_creative(item) for item in elements]

    def fetch_campaign_analytics(
        self,
        conditions: list[FilterCondition] | None = None,
        limit: int | None = None,
        sort: list[SortColumn] | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch campaign analytics from LinkedIn Ads API.

        Note: LinkedIn analytics API doesn't support sorting, so we apply sort locally.

        Args:
            conditions: Filter conditions from SQL query
            limit: Maximum number of results
            sort: Sort columns (applied locally)

        Returns:
            List of normalized analytics dictionaries
        """
        params = utils.build_campaign_analytics_params(
            conditions,
            self.account_id,
            default_lookback_days=self.DEFAULT_ANALYTICS_LOOKBACK_DAYS,
            supported_granularities=self.SUPPORTED_TIME_GRANULARITIES,
            analytics_fields=self.CAMPAIGN_ANALYTICS_FIELDS,
        )
        request_url = utils.build_raw_request_url(self.AD_ANALYTICS_ENDPOINT, params)
        payload = self.client.request_json("GET", request_url)
        elements = payload.get("elements", [])
        if not isinstance(elements, list):
            raise ValueError("LinkedIn Ads analytics response elements is not a list")

        rows = [utils.normalize_campaign_analytics(item, params["timeGranularity"]) for item in elements]

        # Apply sort locally since API doesn't support it for analytics
        rows = utils.sort_rows_locally(rows, sort)

        return rows[:limit] if limit is not None else rows
