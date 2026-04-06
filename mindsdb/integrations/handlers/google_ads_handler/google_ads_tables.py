"""
Google Ads Tables

All tables use Pattern A: always fetch all columns from the API via GAQL, return the
full DataFrame, and let DuckDB (SubSelectStep) handle aggregations, CASE WHEN, CAST,
arithmetic, and complex WHERE expressions.

Safe pushdown: simple equality / comparison conditions are forwarded to GAQL to
reduce the amount of data retrieved from the API. DuckDB re-applies the full original
WHERE clause on top of the DataFrame, so correctness is guaranteed regardless.
"""

from datetime import date, timedelta

import pandas as pd
from mindsdb_sql_parser import ast
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.utilities import log

logger = log.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _normalize_customer_id(customer_id: str) -> str:
    return customer_id.replace('-', '').strip()


def _extract_date_range(where):
    """Extract start_date / end_date from WHERE clause.

    Returns (start_date, end_date, other_conditions) where other_conditions is
    a list of [op, col, val] triples for non-date conditions.
    """
    conditions = extract_comparison_conditions(where) if where else []
    start_date = None
    end_date = None
    other_conditions = []
    for cond in conditions:
        if not isinstance(cond, list):
            continue
        op, col, val = cond
        if col == 'start_date' and op == '=':
            start_date = val
        elif col == 'end_date' and op == '=':
            end_date = val
        else:
            other_conditions.append(cond)
    return start_date, end_date, other_conditions


def _build_simple_filters(conditions, col_map):
    """Convert [op, col, val] conditions to GAQL WHERE fragments.

    Only pushes down simple equality and numeric comparisons for columns
    in col_map (SQL col name → GAQL field name). Everything else is left
    for DuckDB to handle via SubSelectStep.

    Returns a list of GAQL condition strings.
    """
    fragments = []
    safe_ops = {'=', '!=', '<', '<=', '>', '>='}
    for cond in conditions:
        if not isinstance(cond, list):
            continue
        op, col, val = cond
        if op not in safe_ops or col not in col_map:
            continue
        gaql_field = col_map[col]
        # Quote string values; leave numeric values unquoted
        if isinstance(val, str):
            # Escape single quotes inside the value
            escaped = val.replace("'", "\\'")
            fragments.append(f"{gaql_field} {op} '{escaped}'")
        else:
            fragments.append(f"{gaql_field} {op} {val}")
    return fragments


def _pattern_a_columns(query, all_columns):
    """Pattern A column selection.

    Returns all columns when any complex expression (non-Identifier) is found
    in query.targets. Never returns an empty list.
    """
    selected = []
    for target in query.targets:
        if isinstance(target, ast.Star):
            return list(all_columns)
        elif isinstance(target, ast.Identifier):
            selected.append(target.parts[-1])
        else:
            # Complex expression (CASE WHEN, Function, BinaryOperation, etc.)
            # Return all columns so DuckDB has everything it needs.
            return list(all_columns)
    return selected if selected else list(all_columns)


def _enum_name(val) -> str:
    """Convert a proto-plus enum value to its string name, or return as-is."""
    if val is None:
        return None
    if hasattr(val, 'name'):
        return val.name
    return str(val)


# ---------------------------------------------------------------------------
# CampaignsTable
# ---------------------------------------------------------------------------

# Maps SQL WHERE column names → GAQL field names (safe pushdown only)
_CAMPAIGNS_FILTER_MAP = {
    'id': 'campaign.id',
    'status': 'campaign.status',
    'advertising_channel_type': 'campaign.advertising_channel_type',
}

CAMPAIGNS_COLUMNS = [
    'id', 'name', 'status', 'advertising_channel_type', 'bidding_strategy_type',
    'budget_amount_micros', 'budget_name', 'serving_status',
]

_CAMPAIGNS_GAQL = """
    SELECT
        campaign.id,
        campaign.name,
        campaign.status,
        campaign.advertising_channel_type,
        campaign.bidding_strategy_type,
        campaign_budget.amount_micros,
        campaign_budget.name,
        campaign.serving_status
    FROM campaign
"""


class CampaignsTable(APITable):
    """Campaigns in the Google Ads account."""

    def get_columns(self):
        return list(CAMPAIGNS_COLUMNS)

    def select(self, query: ast.Select) -> pd.DataFrame:
        self.handler.connect()
        conditions = extract_comparison_conditions(query.where) if query.where else []
        push_filters = _build_simple_filters(conditions, _CAMPAIGNS_FILTER_MAP)

        gaql = _CAMPAIGNS_GAQL.strip()
        if push_filters:
            gaql += " WHERE " + " AND ".join(push_filters)

        logger.debug(f"CampaignsTable GAQL: {gaql}")

        ga_service = self.handler.client.get_service("GoogleAdsService")
        response = ga_service.search(customer_id=self.handler.customer_id, query=gaql)

        rows = []
        for row in response:
            c = row.campaign
            b = row.campaign_budget
            rows.append({
                'id': str(c.id),
                'name': c.name,
                'status': _enum_name(c.status),
                'advertising_channel_type': _enum_name(c.advertising_channel_type),
                'bidding_strategy_type': _enum_name(c.bidding_strategy_type),
                'budget_amount_micros': b.amount_micros if b else None,
                'budget_name': b.name if b else None,
                'serving_status': _enum_name(c.serving_status),
            })

        df = pd.DataFrame(rows, columns=self.get_columns()) if rows else pd.DataFrame(columns=self.get_columns())
        return df[_pattern_a_columns(query, self.get_columns())]


# ---------------------------------------------------------------------------
# AdGroupsTable
# ---------------------------------------------------------------------------

_AD_GROUPS_FILTER_MAP = {
    'id': 'ad_group.id',
    'campaign_id': 'campaign.id',
    'status': 'ad_group.status',
    'type': 'ad_group.type',
}

AD_GROUPS_COLUMNS = [
    'id', 'name', 'campaign_id', 'campaign_name', 'status', 'type', 'cpc_bid_micros',
]

_AD_GROUPS_GAQL = """
    SELECT
        ad_group.id,
        ad_group.name,
        campaign.id,
        campaign.name,
        ad_group.status,
        ad_group.type,
        ad_group.cpc_bid_micros
    FROM ad_group
"""


class AdGroupsTable(APITable):
    """Ad groups in the Google Ads account."""

    def get_columns(self):
        return list(AD_GROUPS_COLUMNS)

    def select(self, query: ast.Select) -> pd.DataFrame:
        self.handler.connect()
        conditions = extract_comparison_conditions(query.where) if query.where else []
        push_filters = _build_simple_filters(conditions, _AD_GROUPS_FILTER_MAP)

        gaql = _AD_GROUPS_GAQL.strip()
        if push_filters:
            gaql += " WHERE " + " AND ".join(push_filters)

        logger.debug(f"AdGroupsTable GAQL: {gaql}")

        ga_service = self.handler.client.get_service("GoogleAdsService")
        response = ga_service.search(customer_id=self.handler.customer_id, query=gaql)

        rows = []
        for row in response:
            ag = row.ad_group
            c = row.campaign
            rows.append({
                'id': str(ag.id),
                'name': ag.name,
                'campaign_id': str(c.id),
                'campaign_name': c.name,
                'status': _enum_name(ag.status),
                'type': _enum_name(ag.type_),
                'cpc_bid_micros': ag.cpc_bid_micros,
            })

        df = pd.DataFrame(rows, columns=self.get_columns()) if rows else pd.DataFrame(columns=self.get_columns())
        return df[_pattern_a_columns(query, self.get_columns())]


# ---------------------------------------------------------------------------
# AdsTable
# ---------------------------------------------------------------------------

_ADS_FILTER_MAP = {
    'id': 'ad_group_ad.ad.id',
    'campaign_id': 'campaign.id',
    'ad_group_id': 'ad_group.id',
    'status': 'ad_group_ad.status',
}

ADS_COLUMNS = [
    'id', 'ad_group_id', 'ad_group_name', 'campaign_id', 'campaign_name',
    'status', 'type', 'final_urls', 'headlines', 'descriptions',
]

_ADS_GAQL = """
    SELECT
        ad_group_ad.ad.id,
        ad_group.id,
        ad_group.name,
        campaign.id,
        campaign.name,
        ad_group_ad.status,
        ad_group_ad.ad.type,
        ad_group_ad.ad.final_urls,
        ad_group_ad.ad.responsive_search_ad.headlines,
        ad_group_ad.ad.responsive_search_ad.descriptions
    FROM ad_group_ad
"""


class AdsTable(APITable):
    """Ads (ad_group_ad) in the Google Ads account."""

    def get_columns(self):
        return list(ADS_COLUMNS)

    def select(self, query: ast.Select) -> pd.DataFrame:
        self.handler.connect()
        conditions = extract_comparison_conditions(query.where) if query.where else []
        push_filters = _build_simple_filters(conditions, _ADS_FILTER_MAP)

        gaql = _ADS_GAQL.strip()
        if push_filters:
            gaql += " WHERE " + " AND ".join(push_filters)

        logger.debug(f"AdsTable GAQL: {gaql}")

        ga_service = self.handler.client.get_service("GoogleAdsService")
        response = ga_service.search(customer_id=self.handler.customer_id, query=gaql)

        rows = []
        for row in response:
            ad = row.ad_group_ad.ad
            ag = row.ad_group
            c = row.campaign

            # Extract headlines and descriptions from responsive search ads
            rsa = ad.responsive_search_ad
            headlines = [a.text for a in rsa.headlines] if rsa and rsa.headlines else []
            descriptions = [a.text for a in rsa.descriptions] if rsa and rsa.descriptions else []

            rows.append({
                'id': str(ad.id),
                'ad_group_id': str(ag.id),
                'ad_group_name': ag.name,
                'campaign_id': str(c.id),
                'campaign_name': c.name,
                'status': _enum_name(row.ad_group_ad.status),
                'type': _enum_name(ad.type_),
                'final_urls': list(ad.final_urls) if ad.final_urls else [],
                'headlines': headlines,
                'descriptions': descriptions,
            })

        df = pd.DataFrame(rows, columns=self.get_columns()) if rows else pd.DataFrame(columns=self.get_columns())
        return df[_pattern_a_columns(query, self.get_columns())]


# ---------------------------------------------------------------------------
# KeywordsTable
# ---------------------------------------------------------------------------

_KEYWORDS_FILTER_MAP = {
    'id': 'ad_group_criterion.criterion_id',
    'campaign_id': 'campaign.id',
    'ad_group_id': 'ad_group.id',
    'status': 'ad_group_criterion.status',
    'match_type': 'ad_group_criterion.keyword.match_type',
}

KEYWORDS_COLUMNS = [
    'id', 'ad_group_id', 'campaign_id', 'keyword_text', 'match_type',
    'status', 'quality_score', 'cpc_bid_micros',
]

_KEYWORDS_GAQL = """
    SELECT
        ad_group_criterion.criterion_id,
        ad_group.id,
        campaign.id,
        ad_group_criterion.keyword.text,
        ad_group_criterion.keyword.match_type,
        ad_group_criterion.status,
        ad_group_criterion.quality_info.quality_score,
        ad_group_criterion.cpc_bid_micros
    FROM ad_group_criterion
    WHERE ad_group_criterion.type = 'KEYWORD'
"""


class KeywordsTable(APITable):
    """Keywords in the Google Ads account."""

    def get_columns(self):
        return list(KEYWORDS_COLUMNS)

    def select(self, query: ast.Select) -> pd.DataFrame:
        self.handler.connect()
        conditions = extract_comparison_conditions(query.where) if query.where else []
        push_filters = _build_simple_filters(conditions, _KEYWORDS_FILTER_MAP)

        # Base GAQL already has WHERE ad_group_criterion.type = 'KEYWORD'
        gaql = _KEYWORDS_GAQL.strip()
        if push_filters:
            gaql += " AND " + " AND ".join(push_filters)

        logger.debug(f"KeywordsTable GAQL: {gaql}")

        ga_service = self.handler.client.get_service("GoogleAdsService")
        response = ga_service.search(customer_id=self.handler.customer_id, query=gaql)

        rows = []
        for row in response:
            kw = row.ad_group_criterion
            rows.append({
                'id': str(kw.criterion_id),
                'ad_group_id': str(row.ad_group.id),
                'campaign_id': str(row.campaign.id),
                'keyword_text': kw.keyword.text,
                'match_type': _enum_name(kw.keyword.match_type),
                'status': _enum_name(kw.status),
                'quality_score': kw.quality_info.quality_score if kw.quality_info else None,
                'cpc_bid_micros': kw.cpc_bid_micros,
            })

        df = pd.DataFrame(rows, columns=self.get_columns()) if rows else pd.DataFrame(columns=self.get_columns())
        return df[_pattern_a_columns(query, self.get_columns())]


# ---------------------------------------------------------------------------
# CampaignPerformanceTable
# ---------------------------------------------------------------------------

_PERF_FILTER_MAP = {
    'campaign_id': 'campaign.id',
    'status': 'campaign.status',
}

CAMPAIGN_PERFORMANCE_COLUMNS = [
    'campaign_id', 'campaign_name', 'date', 'impressions', 'clicks',
    'cost_micros', 'conversions', 'conversions_value', 'ctr',
    'average_cpc', 'average_cpm',
]

_CAMPAIGN_PERFORMANCE_GAQL = """
    SELECT
        campaign.id,
        campaign.name,
        segments.date,
        metrics.impressions,
        metrics.clicks,
        metrics.cost_micros,
        metrics.conversions,
        metrics.conversions_value,
        metrics.ctr,
        metrics.average_cpc,
        metrics.average_cpm
    FROM campaign
    WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
"""


class CampaignPerformanceTable(APITable):
    """Daily campaign performance metrics.

    Required WHERE: start_date = 'YYYY-MM-DD', end_date = 'YYYY-MM-DD'
    Optional WHERE: campaign_id = '...'
    """

    def get_columns(self):
        return list(CAMPAIGN_PERFORMANCE_COLUMNS)

    def select(self, query: ast.Select) -> pd.DataFrame:
        self.handler.connect()
        start_date, end_date, other_conditions = _extract_date_range(query.where)

        if not start_date or not end_date:
            end_date = end_date or date.today().isoformat()
            start_date = start_date or (date.today() - timedelta(days=30)).isoformat()

        push_filters = _build_simple_filters(other_conditions, _PERF_FILTER_MAP)

        gaql = _CAMPAIGN_PERFORMANCE_GAQL.format(
            start_date=start_date, end_date=end_date
        ).strip()
        if push_filters:
            gaql += " AND " + " AND ".join(push_filters)

        logger.debug(f"CampaignPerformanceTable GAQL: {gaql}")

        ga_service = self.handler.client.get_service("GoogleAdsService")
        response = ga_service.search(customer_id=self.handler.customer_id, query=gaql)

        rows = []
        for row in response:
            m = row.metrics
            rows.append({
                'campaign_id': str(row.campaign.id),
                'campaign_name': row.campaign.name,
                'date': row.segments.date,
                'impressions': m.impressions,
                'clicks': m.clicks,
                'cost_micros': m.cost_micros,
                'conversions': m.conversions,
                'conversions_value': m.conversions_value,
                'ctr': m.ctr,
                'average_cpc': m.average_cpc,
                'average_cpm': m.average_cpm,
            })

        df = pd.DataFrame(rows, columns=self.get_columns()) if rows else pd.DataFrame(columns=self.get_columns())
        return df[_pattern_a_columns(query, self.get_columns())]


# ---------------------------------------------------------------------------
# SearchTermsTable
# ---------------------------------------------------------------------------

_SEARCH_TERMS_FILTER_MAP = {
    'campaign_id': 'campaign.id',
    'ad_group_id': 'ad_group.id',
}

SEARCH_TERMS_COLUMNS = [
    'search_term', 'campaign_id', 'campaign_name', 'ad_group_id', 'ad_group_name',
    'impressions', 'clicks', 'cost_micros', 'conversions', 'date', 'status',
]

_SEARCH_TERMS_GAQL = """
    SELECT
        search_term_view.search_term,
        campaign.id,
        campaign.name,
        ad_group.id,
        ad_group.name,
        metrics.impressions,
        metrics.clicks,
        metrics.cost_micros,
        metrics.conversions,
        segments.date,
        search_term_view.status
    FROM search_term_view
    WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
"""


class SearchTermsTable(APITable):
    """Search term performance data.

    Required WHERE: start_date = 'YYYY-MM-DD', end_date = 'YYYY-MM-DD'
    Optional WHERE: campaign_id = '...', ad_group_id = '...'
    """

    def get_columns(self):
        return list(SEARCH_TERMS_COLUMNS)

    def select(self, query: ast.Select) -> pd.DataFrame:
        self.handler.connect()
        start_date, end_date, other_conditions = _extract_date_range(query.where)

        if not start_date or not end_date:
            end_date = end_date or date.today().isoformat()
            start_date = start_date or (date.today() - timedelta(days=30)).isoformat()

        push_filters = _build_simple_filters(other_conditions, _SEARCH_TERMS_FILTER_MAP)

        gaql = _SEARCH_TERMS_GAQL.format(
            start_date=start_date, end_date=end_date
        ).strip()
        if push_filters:
            gaql += " AND " + " AND ".join(push_filters)

        logger.debug(f"SearchTermsTable GAQL: {gaql}")

        ga_service = self.handler.client.get_service("GoogleAdsService")
        response = ga_service.search(customer_id=self.handler.customer_id, query=gaql)

        rows = []
        for row in response:
            m = row.metrics
            stv = row.search_term_view
            rows.append({
                'search_term': stv.search_term,
                'campaign_id': str(row.campaign.id),
                'campaign_name': row.campaign.name,
                'ad_group_id': str(row.ad_group.id),
                'ad_group_name': row.ad_group.name,
                'impressions': m.impressions,
                'clicks': m.clicks,
                'cost_micros': m.cost_micros,
                'conversions': m.conversions,
                'date': row.segments.date,
                'status': _enum_name(stv.status),
            })

        df = pd.DataFrame(rows, columns=self.get_columns()) if rows else pd.DataFrame(columns=self.get_columns())
        return df[_pattern_a_columns(query, self.get_columns())]
