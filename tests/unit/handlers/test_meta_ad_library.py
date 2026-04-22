import os
import sys
import types
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import types as sqlalchemy_types

os.environ.setdefault("MINDSDB_STORAGE_DIR", "/tmp/mindsdb_meta_ad_library_test")
os.makedirs(os.environ["MINDSDB_STORAGE_DIR"], exist_ok=True)

mind_castle_module = types.ModuleType("mind_castle")
sqlalchemy_type_module = types.ModuleType("mind_castle.sqlalchemy_type")


class SecretData(sqlalchemy_types.TypeDecorator):
    impl = sqlalchemy_types.String
    cache_ok = True

    def __init__(self, *args, **kwargs):
        super().__init__()


sqlalchemy_type_module.SecretData = SecretData
mind_castle_module.sqlalchemy_type = sqlalchemy_type_module
sys.modules.setdefault("mind_castle", mind_castle_module)
sys.modules.setdefault("mind_castle.sqlalchemy_type", sqlalchemy_type_module)

try:
    from mindsdb.integrations.handlers.meta_ad_library_handler.meta_ad_library_handler import (
        MetaAdLibraryHandler,
    )
    from mindsdb.integrations.libs.response import (
        HandlerResponse as Response,
        HandlerStatusResponse as StatusResponse,
        RESPONSE_TYPE,
    )
    from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator
except ImportError:
    pytestmark = pytest.mark.skip("Meta Ad Library handler not installed")


def _build_json_response(payload, status_code=200):
    response = MagicMock()
    response.status_code = status_code
    response.ok = status_code < 400
    response.text = str(payload)
    response.json.return_value = payload
    return response


@pytest.fixture
def handler():
    return MetaAdLibraryHandler(
        "meta_ad_library",
        connection_data={
            "access_token": "meta_token",
            "search_page_ids": ["257702164651631"],
            "ad_reached_countries": ["ALL"],
            "ad_type": "ALL",
            "ad_active_status": "ALL",
        },
    )


def test_initialization_registers_ads_table(handler):
    assert handler.name == "meta_ad_library"
    assert handler.access_token == "meta_token"
    assert handler.base_url == "https://graph.facebook.com/v24.0/ads_archive"
    assert list(handler._tables) == ["ads"]


def test_connect_requires_access_token():
    handler = MetaAdLibraryHandler("meta_ad_library", connection_data={})

    with pytest.raises(ValueError, match="access_token is required"):
        handler.connect()


def test_check_connection_success(handler):
    session = MagicMock()
    session.get.return_value = _build_json_response({"data": []})

    with patch(
        "mindsdb.integrations.handlers.meta_ad_library_handler.meta_ad_library_handler.requests.Session",
        return_value=session,
    ):
        response = handler.check_connection()

    assert isinstance(response, StatusResponse)
    assert response.success is True
    assert response.error_message is None
    session.get.assert_called_once()


def test_check_connection_fails_when_no_search_scope():
    """Meta API returns 400 when neither search_page_ids nor search_terms is provided."""
    handler = MetaAdLibraryHandler(
        "meta_ad_library",
        connection_data={"access_token": "meta_token"},
    )
    session = MagicMock()
    session.get.return_value = _build_json_response(
        {
            "error": {
                "message": "A search_terms or search_page_ids parameter is required",
                "type": "GraphMethodException",
                "code": 100,
            }
        },
        status_code=400,
    )

    with patch(
        "mindsdb.integrations.handlers.meta_ad_library_handler.meta_ad_library_handler.requests.Session",
        return_value=session,
    ):
        response = handler.check_connection()

    assert isinstance(response, StatusResponse)
    assert response.success is False
    assert "search_terms or search_page_ids" in response.error_message
    session.get.assert_called_once()


def test_build_request_params_for_page_scope_does_not_send_search_type(handler):
    params = handler._build_request_params(conditions=[], limit=25)

    assert params["access_token"] == "meta_token"
    assert params["ad_reached_countries"] == '["ALL"]'
    assert params["ad_type"] == "ALL"
    assert params["ad_active_status"] == "ALL"
    assert params["limit"] == 25
    assert params["search_page_ids"] == '["257702164651631"]'
    assert "search_type" not in params


def test_build_request_params_for_keyword_scope_sends_default_search_type():
    handler = MetaAdLibraryHandler(
        "meta_ad_library",
        connection_data={
            "access_token": "meta_token",
            "search_terms": "software engineer",
            "ad_reached_countries": ["ALL"],
        },
    )

    params = handler._build_request_params(conditions=[], limit=10)

    assert params["search_terms"] == "software engineer"
    assert params["search_type"] == "KEYWORD_UNORDERED"
    assert "search_page_ids" not in params


def test_build_request_params_translates_delivery_date_bounds(handler):
    conditions = [
        FilterCondition("ad_delivery_start_time", FilterOperator.GREATER_THAN_OR_EQUAL, "2026-03-01"),
        FilterCondition("ad_delivery_stop_time", FilterOperator.LESS_THAN_OR_EQUAL, "2026-03-31T23:59:59Z"),
        FilterCondition("page_name", FilterOperator.EQUAL, "Talentify"),
    ]

    params = handler._build_request_params(conditions=conditions, limit=10)

    assert params["ad_delivery_date_min"] == "2026-03-01"
    assert params["ad_delivery_date_max"] == "2026-03-31"
    assert conditions[0].applied is True
    assert conditions[1].applied is True
    assert conditions[2].applied is False


def test_fetch_ads_paginates_and_normalizes_rows(handler):
    session = MagicMock()
    session.get.side_effect = [
        _build_json_response(
            {
                "data": [
                    {
                        "id": "1",
                        "page_id": "257702164651631",
                        "page_name": "Talentify",
                        "ad_creative_bodies": ["Body 1"],
                    }
                ],
                "paging": {"cursors": {"after": "cursor-1"}},
            }
        ),
        _build_json_response(
            {
                "data": [
                    {
                        "id": "2",
                        "page_id": "257702164651631",
                        "page_name": "Talentify",
                        "ad_snapshot_url": (
                            "https://www.facebook.com/ads/archive/render_ad/"
                            "?id=1234567890&access_token=meta_token"
                        ),
                        "bylines": "Paid for by Talentify",
                        "currency": "USD",
                        "delivery_by_region": [
                            {"region": "California", "percentage": "0.45"},
                        ],
                        "demographic_distribution": [
                            {"age": "25-34", "gender": "female", "percentage": "0.30"},
                        ],
                        "estimated_audience_size": {"lower_bound": "1000", "upper_bound": "5000"},
                        "impressions": {"lower_bound": "100", "upper_bound": "499"},
                        "spend": {"lower_bound": "0", "upper_bound": "99"},
                    }
                ]
            }
        ),
    ]
    handler.session = session

    rows = handler.fetch_ads(limit=2)

    assert [row["id"] for row in rows] == ["1", "2"]
    assert rows[0]["ad_creative_bodies"] == ["Body 1"]
    assert rows[0]["ad_snapshot_url"] is None
    assert rows[1]["ad_snapshot_url"] == "https://www.facebook.com/ads/library/?id=1234567890"
    assert "access_token" not in rows[1]["ad_snapshot_url"]
    assert rows[1]["currency"] == "USD"
    assert rows[1]["spend"] == {"lower_bound": "0", "upper_bound": "99"}
    assert rows[1]["impressions"] == {"lower_bound": "100", "upper_bound": "499"}
    assert rows[1]["demographic_distribution"] == [
        {"age": "25-34", "gender": "female", "percentage": "0.30"},
    ]
    assert session.get.call_count == 2
    assert session.get.call_args_list[1].kwargs["params"]["after"] == "cursor-1"


def test_fetch_ads_does_not_pre_limit_when_local_filters_remain(handler):
    session = MagicMock()
    session.get.return_value = _build_json_response(
        {
            "data": [
                {"id": "1", "page_name": "Ligon For Texas"},
                {"id": "2", "page_name": "Wildfire Victims First"},
                {"id": "3", "page_name": "Moms for Liberty"},
                {"id": "4", "page_name": "Pulse Media"},
                {"id": "5", "page_name": "Pulse Media"},
            ]
        }
    )
    handler.session = session
    conditions = [
        FilterCondition("page_name", FilterOperator.EQUAL, "Pulse Media"),
    ]

    rows = handler.fetch_ads(conditions=conditions, limit=2)

    assert [row["id"] for row in rows] == ["1", "2", "3", "4", "5"]
    assert session.get.call_args.kwargs["params"]["limit"] == handler.DEFAULT_PAGE_SIZE
    assert conditions[0].applied is False


def test_sanitize_ad_snapshot_url_removes_token_from_non_render_urls(handler):
    assert handler._sanitize_ad_snapshot_url("https://example.com/ad?id=1&access_token=token") == (
        "https://example.com/ad?id=1"
    )
    assert handler._sanitize_ad_snapshot_url("https://example.com/ad?id=1") == (
        "https://example.com/ad?id=1"
    )
    assert handler._sanitize_ad_snapshot_url(None) is None


def test_fetch_ads_raises_meta_error_message(handler):
    session = MagicMock()
    session.get.return_value = _build_json_response(
        {"error": {"message": "Application does not have permission for this action"}},
        status_code=400,
    )
    handler.session = session

    with pytest.raises(RuntimeError, match="Application does not have permission"):
        handler.fetch_ads(limit=1)


def test_native_query_projects_selected_columns(handler):
    handler.fetch_ads = MagicMock(
        return_value=[
            {
                "id": "1",
                "page_name": "Talentify",
                "ad_snapshot_url": "https://example.com/ad",
            }
        ]
    )

    response = handler.native_query("SELECT id, page_name FROM ads LIMIT 1")

    assert isinstance(response, Response)
    assert response.type == RESPONSE_TYPE.TABLE
    assert response.data_frame.columns.tolist() == ["id", "page_name"]
    assert response.data_frame.iloc[0]["page_name"] == "Talentify"
