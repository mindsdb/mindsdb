from collections import OrderedDict
from datetime import datetime, timedelta, timezone
import unittest
from unittest.mock import MagicMock, patch

import pytest

try:
    from mindsdb.integrations.handlers.linkedin_ads_handler.linkedin_ads_handler import LinkedInAdsHandler
    from mindsdb.integrations.libs.response import (
        HandlerResponse as Response,
        HandlerStatusResponse as StatusResponse,
        RESPONSE_TYPE,
    )
    from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator
except ImportError:
    pytestmark = pytest.mark.skip("LinkedIn Ads handler not installed")

from base_handler_test import BaseHandlerTestSetup


class TestLinkedInAdsHandler(BaseHandlerTestSetup, unittest.TestCase):
    EXPECTED_TABLES = ["campaigns", "campaign_groups", "creatives", "campaign_analytics"]

    @property
    def dummy_connection_data(self):
        return OrderedDict(
            account_id="516413367",
            access_token="test_access_token",
            refresh_token="test_refresh_token",
            client_id="test_client_id",
            client_secret="test_client_secret",
            api_version="202602",
        )

    @property
    def registered_tables(self):
        return self.EXPECTED_TABLES

    @property
    def err_to_raise_on_connect_failure(self):
        return Exception("Authentication failed")

    def create_handler(self):
        self.handler_storage = MagicMock()
        self.handler_storage.encrypted_json_get.return_value = None
        return LinkedInAdsHandler(
            "linkedin_ads",
            connection_data=self.dummy_connection_data,
            handler_storage=self.handler_storage,
        )

    def create_patcher(self):
        return patch("mindsdb.integrations.handlers.linkedin_ads_handler.linkedin_ads_handler.requests.Session")

    def _build_json_response(self, payload):
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = payload
        response.raise_for_status.return_value = None
        return response

    def test_initialization(self):
        self.assertEqual(self.handler.name, "linkedin_ads")
        self.assertFalse(self.handler.is_connected)
        self.assertEqual(self.handler.account_id, self.dummy_connection_data["account_id"])
        self.assertEqual(sorted(self.handler._tables.keys()), sorted(self.EXPECTED_TABLES))

    def test_connect_success(self):
        session = MagicMock()
        session.headers = {}
        self.mock_connect.return_value = session

        connection = self.handler.connect()

        self.assertIs(connection, session)
        self.assertTrue(self.handler.is_connected)
        self.assertEqual(session.headers["Authorization"], "Bearer test_access_token")
        self.assertEqual(session.headers["LinkedIn-Version"], "202602")
        self.handler_storage.encrypted_json_set.assert_called_once()

    def test_check_connection_success(self):
        session = MagicMock()
        session.headers = {}
        session.request.return_value = self._build_json_response({"id": "516413367", "name": "Talentify Ads"})
        self.mock_connect.return_value = session

        response = self.handler.check_connection()

        assert isinstance(response, StatusResponse)
        self.assertTrue(response.success)
        self.assertIsNone(response.error_message)

    def test_check_connection_failure(self):
        session = MagicMock()
        session.headers = {}
        session.request.side_effect = self.err_to_raise_on_connect_failure
        self.mock_connect.return_value = session

        response = self.handler.check_connection()

        assert isinstance(response, StatusResponse)
        self.assertFalse(response.success)
        self.assertIn("Authentication failed", response.error_message)

    def test_refreshes_expired_token(self):
        session = MagicMock()
        session.headers = {}
        self.mock_connect.return_value = session
        self.handler_storage.encrypted_json_get.return_value = {
            "access_token": "expired_access_token",
            "refresh_token": "stored_refresh_token",
            "expires_at": (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat(),
        }

        with patch(
            "mindsdb.integrations.handlers.linkedin_ads_handler.linkedin_ads_handler.requests.post"
        ) as mock_post:
            mock_post.return_value = self._build_json_response(
                {
                    "access_token": "fresh_access_token",
                    "refresh_token": "fresh_refresh_token",
                    "expires_in": 3600,
                    "refresh_token_expires_in": 86400,
                }
            )

            connection = self.handler.connect()

        self.assertIs(connection, session)
        self.assertEqual(session.headers["Authorization"], "Bearer fresh_access_token")
        self.assertEqual(self.handler.refresh_token, "fresh_refresh_token")
        mock_post.assert_called_once()
        self.assertGreaterEqual(self.handler_storage.encrypted_json_set.call_count, 1)

    def test_get_tables(self):
        response = self.handler.get_tables()

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertEqual(sorted(response.data_frame["table_name"].tolist()), sorted(self.EXPECTED_TABLES))

    def test_get_columns(self):
        response = self.handler.get_columns("campaigns")

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertEqual(response.data_frame.columns.tolist(), ["Field", "Type"])
        self.assertIn("id", response.data_frame["Field"].tolist())
        self.assertIn("name", response.data_frame["Field"].tolist())

    def test_extract_search_filters_returns_none_without_conditions(self):
        filters = self.handler._extract_search_filters([], self.handler.CAMPAIGN_SEARCH_FILTERS)

        self.assertIsNone(filters)

    def test_fetch_campaigns_without_filters_does_not_send_search_param(self):
        session = MagicMock()
        session.headers = {}
        session.request.side_effect = [
            self._build_json_response({"elements": [{"id": 1, "name": "Campaign 1", "status": "ACTIVE"}]})
        ]
        self.mock_connect.return_value = session

        rows = self.handler.fetch_campaigns(limit=1)

        self.assertEqual(rows[0]["id"], 1)
        _, kwargs = session.request.call_args
        self.assertEqual(kwargs["params"]["q"], "search")
        self.assertEqual(kwargs["params"]["pageSize"], 1)
        self.assertNotIn("search", kwargs["params"])

    def test_fetch_campaigns_with_id_filter_uses_raw_search_url(self):
        session = MagicMock()
        session.headers = {}
        session.request.side_effect = [
            self._build_json_response({"elements": [{"id": 341766444, "name": "Campaign 1", "status": "ACTIVE"}]})
        ]
        self.mock_connect.return_value = session

        rows = self.handler.fetch_campaigns(
            conditions=[FilterCondition("id", FilterOperator.IN, [341766444, "urn:li:sponsoredCampaign:344169684"])],
            limit=2,
        )

        self.assertEqual(rows[0]["id"], 341766444)
        _, kwargs = session.request.call_args
        self.assertIsNone(kwargs["params"])
        self.assertEqual(
            kwargs["url"],
            "https://api.linkedin.com/rest/adAccounts/516413367/adCampaigns?q=search&search=(id:(values:List(urn:li:sponsoredCampaign:341766444,urn:li:sponsoredCampaign:344169684)))&pageSize=2&sortOrder=ASCENDING",
        )

    def test_fetch_campaigns_with_status_filter_uses_raw_search_url(self):
        session = MagicMock()
        session.headers = {}
        session.request.side_effect = [
            self._build_json_response({"elements": [{"id": 1, "name": "Campaign 1", "status": "ACTIVE"}]})
        ]
        self.mock_connect.return_value = session

        rows = self.handler.fetch_campaigns(
            conditions=[FilterCondition("status", FilterOperator.EQUAL, "ACTIVE")],
            limit=5,
        )

        self.assertEqual(rows[0]["status"], "ACTIVE")
        _, kwargs = session.request.call_args
        self.assertIsNone(kwargs["params"])
        self.assertEqual(
            kwargs["url"],
            "https://api.linkedin.com/rest/adAccounts/516413367/adCampaigns?q=search&search=(status:(values:List(ACTIVE)))&pageSize=5&sortOrder=ASCENDING",
        )

    def test_fetch_campaign_groups_with_id_filter_uses_batch_get_ids(self):
        session = MagicMock()
        session.headers = {}
        session.request.side_effect = [
            self._build_json_response(
                {
                    "results": {
                        "719136146": {"id": 719136146, "name": "Group 1", "status": "ACTIVE", "test": False},
                        "721673054": {"id": 721673054, "name": "Group 2", "status": "ACTIVE", "test": False},
                    },
                    "statuses": {},
                    "errors": {},
                }
            )
        ]
        self.mock_connect.return_value = session

        rows = self.handler.fetch_campaign_groups(
            conditions=[FilterCondition("id", FilterOperator.IN, [719136146, "urn:li:sponsoredCampaignGroup:721673054"])],
            limit=2,
        )

        self.assertEqual(len(rows), 2)
        _, kwargs = session.request.call_args
        self.assertIsNone(kwargs["params"])
        self.assertEqual(
            kwargs["url"],
            "https://api.linkedin.com/rest/adAccounts/516413367/adCampaignGroups?ids=List(719136146,721673054)",
        )

    def test_fetch_campaign_groups_with_name_filter_uses_raw_search_url(self):
        session = MagicMock()
        session.headers = {}
        session.request.side_effect = [
            self._build_json_response({"elements": [{"id": 719136146, "name": "1st Wave Health Companies", "status": "ARCHIVED"}]})
        ]
        self.mock_connect.return_value = session

        rows = self.handler.fetch_campaign_groups(
            conditions=[FilterCondition("name", FilterOperator.EQUAL, "1st Wave Health Companies")],
            limit=5,
        )

        self.assertEqual(rows[0]["name"], "1st Wave Health Companies")
        _, kwargs = session.request.call_args
        self.assertIsNone(kwargs["params"])
        self.assertEqual(
            kwargs["url"],
            "https://api.linkedin.com/rest/adAccounts/516413367/adCampaignGroups?q=search&search=(name:(values:List(1st Wave Health Companies)))&pageSize=5&sortOrder=ASCENDING",
        )

    def test_build_campaign_analytics_params_defaults(self):
        params = self.handler._build_campaign_analytics_params([])

        self.assertEqual(params["q"], "analytics")
        self.assertEqual(params["pivot"], "CAMPAIGN")
        self.assertEqual(params["fields"], "dateRange,pivotValues,impressions,clicks,landingPageClicks,likes,shares,costInLocalCurrency,externalWebsiteConversions")
        self.assertEqual(params["timeGranularity"], "DAILY")
        self.assertIn("accounts", params)
        self.assertIn("dateRange", params)

    def test_build_campaign_analytics_params_with_filters(self):
        conditions = [
            FilterCondition("campaign_id", FilterOperator.IN, ["123", "456"]),
            FilterCondition("date", FilterOperator.GREATER_THAN_OR_EQUAL, "2026-03-01"),
            FilterCondition("date", FilterOperator.LESS_THAN_OR_EQUAL, "2026-03-07"),
            FilterCondition("time_granularity", FilterOperator.EQUAL, "monthly"),
        ]

        params = self.handler._build_campaign_analytics_params(conditions)

        self.assertEqual(params["campaigns"], "List(urn%3Ali%3AsponsoredCampaign%3A123,urn%3Ali%3AsponsoredCampaign%3A456)")
        self.assertEqual(params["timeGranularity"], "MONTHLY")
        self.assertEqual(
            params["dateRange"],
            "(start:(year:2026,month:3,day:1),end:(year:2026,month:3,day:7))",
        )
        self.assertTrue(all(condition.applied for condition in conditions))

    def test_native_query_campaigns(self):
        self.handler.fetch_campaigns = MagicMock(
            return_value=[
                {
                    "id": 1,
                    "name": "Campaign 1",
                    "status": "ACTIVE",
                    "type": "TEXT_AD",
                }
            ]
        )

        response = self.handler.native_query("SELECT id, name FROM campaigns LIMIT 1")

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertEqual(response.data_frame.columns.tolist(), ["id", "name"])
        self.assertEqual(response.data_frame.iloc[0]["id"], 1)

    def test_native_query_campaign_groups(self):
        self.handler.fetch_campaign_groups = MagicMock(
            return_value=[
                {
                    "id": 10,
                    "name": "Group 1",
                    "status": "ACTIVE",
                }
            ]
        )

        response = self.handler.native_query("SELECT id, name FROM campaign_groups LIMIT 1")

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertEqual(response.data_frame.columns.tolist(), ["id", "name"])
        self.assertEqual(response.data_frame.iloc[0]["name"], "Group 1")

    def test_native_query_creatives(self):
        self.handler.fetch_creatives = MagicMock(
            return_value=[
                {
                    "id": "urn:li:sponsoredCreative:123",
                    "creative_id": "123",
                    "campaign_id": "456",
                }
            ]
        )

        response = self.handler.native_query("SELECT id, creative_id FROM creatives LIMIT 1")

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertEqual(response.data_frame.columns.tolist(), ["id", "creative_id"])
        self.assertEqual(response.data_frame.iloc[0]["creative_id"], "123")

    def test_native_query_campaign_analytics(self):
        self.handler.fetch_campaign_analytics = MagicMock(
            return_value=[
                {
                    "campaign_urn": "urn:li:sponsoredCampaign:456",
                    "campaign_id": "456",
                    "date_start": "2026-03-01",
                    "date_end": "2026-03-01",
                    "impressions": 1000,
                    "clicks": 25,
                    "cost_in_local_currency": 42.5,
                }
            ]
        )

        response = self.handler.native_query(
            "SELECT campaign_id, date_start, impressions FROM campaign_analytics LIMIT 1"
        )

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertEqual(response.data_frame.columns.tolist(), ["campaign_id", "date_start", "impressions"])
        self.assertEqual(response.data_frame.iloc[0]["campaign_id"], "456")
