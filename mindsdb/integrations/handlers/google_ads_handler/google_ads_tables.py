"""
Google Ads Tables

All tables use Pattern A: always fetch all columns from the API via GAQL, return the
full DataFrame, and let DuckDB (SubSelectStep) handle aggregations, CASE WHEN, CAST,
arithmetic, and complex WHERE expressions.

Safe pushdown: simple equality / comparison conditions are forwarded to GAQL to
reduce the amount of data retrieved from the API. DuckDB re-applies the full original
WHERE clause on top of the DataFrame, so correctness is guaranteed regardless.
"""

import json
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


# ---------------------------------------------------------------------------
# LanguagesTable (API-backed lookup)
# ---------------------------------------------------------------------------

LANGUAGES_COLUMNS = ['id', 'name', 'code', 'targetable']

_LANGUAGES_GAQL = """
    SELECT
        language_constant.id,
        language_constant.name,
        language_constant.code,
        language_constant.targetable
    FROM language_constant
"""


class LanguagesTable(APITable):
    """Lookup table for Google Ads language criterion IDs.

    Queries the language_constant resource via the Google Ads API.
    DuckDB handles all WHERE / LIKE filtering via SubSelectStep.

    Example:
        SELECT * FROM google_ads.languages WHERE name LIKE '%Portuguese%';
    """

    def get_columns(self):
        return list(LANGUAGES_COLUMNS)

    def select(self, query: ast.Select) -> pd.DataFrame:
        self.handler.connect()

        gaql = _LANGUAGES_GAQL.strip()
        logger.debug(f"LanguagesTable GAQL: {gaql}")

        ga_service = self.handler.client.get_service("GoogleAdsService")
        response = ga_service.search(customer_id=self.handler.customer_id, query=gaql)

        rows = []
        for row in response:
            lc = row.language_constant
            rows.append({
                'id': str(lc.id),
                'name': lc.name,
                'code': lc.code,
                'targetable': lc.targetable,
            })

        df = pd.DataFrame(rows, columns=self.get_columns()) if rows else pd.DataFrame(columns=self.get_columns())
        return df[_pattern_a_columns(query, self.get_columns())]


# ---------------------------------------------------------------------------
# GeoTargetsTable (API-backed lookup)
# ---------------------------------------------------------------------------

GEOTARGETS_COLUMNS = [
    'id', 'name', 'canonical_name', 'country_code', 'target_type', 'status',
]

_GEOTARGETS_GAQL = """
    SELECT
        geo_target_constant.id,
        geo_target_constant.name,
        geo_target_constant.canonical_name,
        geo_target_constant.country_code,
        geo_target_constant.target_type,
        geo_target_constant.status
    FROM geo_target_constant
"""


class GeoTargetsTable(APITable):
    """Lookup table for Google Ads geo target criterion IDs.

    Queries the geo_target_constant resource via the Google Ads API.
    DuckDB handles all WHERE / LIKE filtering via SubSelectStep.

    Example:
        SELECT * FROM google_ads.geo_targets WHERE name LIKE '%Brazil%';
    """

    def get_columns(self):
        return list(GEOTARGETS_COLUMNS)

    def select(self, query: ast.Select) -> pd.DataFrame:
        self.handler.connect()

        gaql = _GEOTARGETS_GAQL.strip()
        logger.debug(f"GeoTargetsTable GAQL: {gaql}")

        ga_service = self.handler.client.get_service("GoogleAdsService")
        response = ga_service.search(customer_id=self.handler.customer_id, query=gaql)

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

        df = pd.DataFrame(rows, columns=self.get_columns()) if rows else pd.DataFrame(columns=self.get_columns())
        return df[_pattern_a_columns(query, self.get_columns())]


# ---------------------------------------------------------------------------
# Keyword Planner helpers
# ---------------------------------------------------------------------------

_KP_PARAM_NAMES = {
    'keywords', 'url', 'language', 'geo_target', 'network',
    'include_adult_keywords', 'page_size',
    'match_type', 'max_cpc_bid_micros', 'start_date', 'end_date',
}


def _extract_keyword_planner_params(where):
    """Extract keyword planner parameters from WHERE clause.

    Returns a dict with any recognised params and a list of remaining conditions.
    """
    conditions = extract_comparison_conditions(where) if where else []
    params = {}
    other = []
    for cond in conditions:
        if not isinstance(cond, list):
            continue
        op, col, val = cond
        if op == '=' and col in _KP_PARAM_NAMES:
            params[col] = val
        else:
            other.append(cond)
    return params, other


# ---------------------------------------------------------------------------
# KeywordIdeasTable
# ---------------------------------------------------------------------------

KEYWORD_IDEAS_COLUMNS = [
    'keyword', 'avg_monthly_searches', 'competition', 'competition_index',
    'low_top_of_page_bid_micros', 'high_top_of_page_bid_micros',
]


class KeywordIdeasTable(APITable):
    """Generate keyword ideas using Google Ads Keyword Planner.

    Required WHERE (at least one):
        keywords = 'seo tools, keyword research'   — comma-separated seed keywords
        url = 'https://example.com'                 — URL seed

    Optional WHERE:
        language = '1000'          (default: 1000 = English)
        geo_target = '2840'        (default: 2840 = United States)
        network = 'GOOGLE_SEARCH'  (default: GOOGLE_SEARCH)
        include_adult_keywords = 'true'  (default: false)
        page_size = '100'          (default: API default)

    Example:
        SELECT * FROM google_ads.keyword_ideas
        WHERE keywords = 'digital marketing, seo'
          AND language = '1000'
          AND geo_target = '2840';
    """

    def get_columns(self):
        return list(KEYWORD_IDEAS_COLUMNS)

    def select(self, query: ast.Select) -> pd.DataFrame:
        self.handler.connect()
        params, _ = _extract_keyword_planner_params(query.where)

        seed_keywords = params.get('keywords')
        seed_url = params.get('url')
        if not seed_keywords and not seed_url:
            raise ValueError(
                "keyword_ideas requires at least 'keywords' or 'url' in the WHERE clause. "
                "Example: WHERE keywords = 'seo tools, keyword research'"
            )

        language = params.get('language', '1000')
        geo_target = params.get('geo_target', '2840')
        network = params.get('network', 'GOOGLE_SEARCH')
        include_adult = str(params.get('include_adult_keywords', 'false')).lower() == 'true'
        page_size = int(params['page_size']) if params.get('page_size') else None

        client = self.handler.client
        kp_service = client.get_service("KeywordPlanIdeaService")

        request = client.get_type("GenerateKeywordIdeasRequest")
        request.customer_id = self.handler.customer_id
        request.language = f"languageConstants/{language}"
        request.geo_target_constants.append(f"geoTargetConstants/{geo_target}")
        request.include_adult_keywords = include_adult

        # Map network string to enum
        network_enum = client.enums.KeywordPlanNetworkEnum.KeywordPlanNetwork
        request.keyword_plan_network = getattr(network_enum, network, network_enum.GOOGLE_SEARCH)

        if page_size:
            request.page_size = page_size

        # Set the appropriate seed based on provided params
        kw_list = [k.strip() for k in seed_keywords.split(',')] if seed_keywords else []
        if kw_list and seed_url:
            request.keyword_and_url_seed.url = seed_url
            request.keyword_and_url_seed.keywords.extend(kw_list)
        elif seed_url:
            request.url_seed.url = seed_url
        else:
            request.keyword_seed.keywords.extend(kw_list)

        logger.debug(f"KeywordIdeasTable: language={language}, geo={geo_target}, "
                      f"network={network}, keywords={kw_list}, url={seed_url}")

        response = kp_service.generate_keyword_ideas(request=request)

        rows = []
        for result in response:
            m = result.keyword_idea_metrics
            rows.append({
                'keyword': result.text,
                'avg_monthly_searches': m.avg_monthly_searches,
                'competition': _enum_name(m.competition),
                'competition_index': m.competition_index,
                'low_top_of_page_bid_micros': m.low_top_of_page_bid_micros,
                'high_top_of_page_bid_micros': m.high_top_of_page_bid_micros,
            })

        df = pd.DataFrame(rows, columns=self.get_columns()) if rows else pd.DataFrame(columns=self.get_columns())
        return df[_pattern_a_columns(query, self.get_columns())]


# ---------------------------------------------------------------------------
# KeywordHistoricalMetricsTable
# ---------------------------------------------------------------------------

KEYWORD_HISTORICAL_METRICS_COLUMNS = [
    'keyword', 'avg_monthly_searches', 'competition', 'competition_index',
    'low_top_of_page_bid_micros', 'high_top_of_page_bid_micros', 'close_variants',
]


class KeywordHistoricalMetricsTable(APITable):
    """Get historical metrics for specific keywords using Google Ads Keyword Planner.

    Required WHERE:
        keywords = 'seo tools, keyword research'  — comma-separated keywords

    Optional WHERE:
        language = '1000'          (default: 1000 = English)
        geo_target = '2840'        (default: 2840 = United States)
        network = 'GOOGLE_SEARCH'  (default: GOOGLE_SEARCH)

    Example:
        SELECT * FROM google_ads.keyword_historical_metrics
        WHERE keywords = 'seo tools, keyword research, digital marketing';
    """

    def get_columns(self):
        return list(KEYWORD_HISTORICAL_METRICS_COLUMNS)

    def select(self, query: ast.Select) -> pd.DataFrame:
        self.handler.connect()
        params, _ = _extract_keyword_planner_params(query.where)

        seed_keywords = params.get('keywords')
        if not seed_keywords:
            raise ValueError(
                "keyword_historical_metrics requires 'keywords' in the WHERE clause. "
                "Example: WHERE keywords = 'seo tools, keyword research'"
            )

        language = params.get('language', '1000')
        geo_target = params.get('geo_target', '2840')
        network = params.get('network', 'GOOGLE_SEARCH')

        kw_list = [k.strip() for k in seed_keywords.split(',')]

        client = self.handler.client
        kp_service = client.get_service("KeywordPlanIdeaService")

        request = client.get_type("GenerateKeywordHistoricalMetricsRequest")
        request.customer_id = self.handler.customer_id
        request.keywords.extend(kw_list)
        request.language = f"languageConstants/{language}"
        request.geo_target_constants.append(f"geoTargetConstants/{geo_target}")

        network_enum = client.enums.KeywordPlanNetworkEnum.KeywordPlanNetwork
        request.keyword_plan_network = getattr(network_enum, network, network_enum.GOOGLE_SEARCH)

        logger.debug(f"KeywordHistoricalMetricsTable: language={language}, geo={geo_target}, "
                      f"network={network}, keywords={kw_list}")

        response = kp_service.generate_keyword_historical_metrics(request=request)

        rows = []
        for result in response.results:
            m = result.keyword_metrics
            rows.append({
                'keyword': result.text,
                'avg_monthly_searches': m.avg_monthly_searches if m else None,
                'competition': _enum_name(m.competition) if m else None,
                'competition_index': m.competition_index if m else None,
                'low_top_of_page_bid_micros': m.low_top_of_page_bid_micros if m else None,
                'high_top_of_page_bid_micros': m.high_top_of_page_bid_micros if m else None,
                'close_variants': json.dumps(list(result.close_variants)) if result.close_variants else '[]',
            })

        df = pd.DataFrame(rows, columns=self.get_columns()) if rows else pd.DataFrame(columns=self.get_columns())
        return df[_pattern_a_columns(query, self.get_columns())]


# ---------------------------------------------------------------------------
# KeywordForecastMetricsTable
# ---------------------------------------------------------------------------

KEYWORD_FORECAST_METRICS_COLUMNS = [
    'keyword', 'match_type', 'impressions', 'clicks',
    'cost_micros', 'average_cpc_micros', 'ctr',
]


class KeywordForecastMetricsTable(APITable):
    """Generate forecast metrics for keywords using Google Ads Keyword Planner.

    Builds a temporary campaign structure and returns estimated performance.

    Required WHERE:
        keywords = 'seo tools, keyword research'  — comma-separated keywords
        max_cpc_bid_micros = '2500000'             — max CPC bid in micros (e.g. 2500000 = $2.50)

    Optional WHERE:
        match_type = 'BROAD'           (default: BROAD; also PHRASE or EXACT)
        language = '1000'              (default: 1000 = English)
        geo_target = '2840'            (default: 2840 = United States)
        network = 'GOOGLE_SEARCH'      (default: GOOGLE_SEARCH)
        start_date = 'YYYY-MM-DD'      (default: today)
        end_date = 'YYYY-MM-DD'        (default: 30 days from today)

    Example:
        SELECT * FROM google_ads.keyword_forecast_metrics
        WHERE keywords = 'seo tools, keyword research'
          AND max_cpc_bid_micros = '2500000'
          AND match_type = 'EXACT'
          AND language = '1000'
          AND geo_target = '2840';
    """

    def get_columns(self):
        return list(KEYWORD_FORECAST_METRICS_COLUMNS)

    def select(self, query: ast.Select) -> pd.DataFrame:
        self.handler.connect()
        params, _ = _extract_keyword_planner_params(query.where)

        seed_keywords = params.get('keywords')
        max_cpc = params.get('max_cpc_bid_micros')
        if not seed_keywords:
            raise ValueError(
                "keyword_forecast_metrics requires 'keywords' in the WHERE clause. "
                "Example: WHERE keywords = 'seo tools, keyword research'"
            )
        if not max_cpc:
            raise ValueError(
                "keyword_forecast_metrics requires 'max_cpc_bid_micros' in the WHERE clause. "
                "Example: WHERE max_cpc_bid_micros = '2500000'"
            )

        max_cpc_value = int(max_cpc)
        match_type_str = params.get('match_type', 'BROAD').upper()
        language = params.get('language', '1000')
        geo_target = params.get('geo_target', '2840')
        network = params.get('network', 'GOOGLE_SEARCH')
        start = params.get('start_date', date.today().isoformat())
        end = params.get('end_date', (date.today() + timedelta(days=30)).isoformat())

        kw_list = [k.strip() for k in seed_keywords.split(',')]

        client = self.handler.client
        kp_service = client.get_service("KeywordPlanIdeaService")

        # Build the match type enum
        match_type_enum = client.enums.KeywordMatchTypeEnum.KeywordMatchType
        match_type = getattr(match_type_enum, match_type_str, match_type_enum.BROAD)

        # Build biddable keywords
        biddable_keywords = []
        for kw_text in kw_list:
            bk = client.get_type("BiddableKeyword")
            bk.max_cpc_bid_micros = max_cpc_value
            bk.keyword.text = kw_text
            bk.keyword.match_type = match_type
            biddable_keywords.append(bk)

        # Build forecast ad group
        ad_group = client.get_type("ForecastAdGroup")
        ad_group.biddable_keywords.extend(biddable_keywords)

        # Build campaign to forecast
        campaign = client.get_type("CampaignToForecast")
        campaign.language_constants.append(f"languageConstants/{language}")

        geo_modifier = client.get_type("CriterionBidModifier")
        geo_modifier.geo_target_constant = f"geoTargetConstants/{geo_target}"
        campaign.geo_modifiers.append(geo_modifier)

        network_enum = client.enums.KeywordPlanNetworkEnum.KeywordPlanNetwork
        campaign.keyword_plan_network = getattr(network_enum, network, network_enum.GOOGLE_SEARCH)

        bidding = client.get_type("CampaignToForecast.CampaignBiddingStrategy")
        manual_cpc = client.get_type("ManualCpcBiddingStrategy")
        manual_cpc.max_cpc_bid_micros = max_cpc_value
        bidding.manual_cpc_bidding_strategy = manual_cpc
        campaign.bidding_strategy = bidding

        campaign.ad_groups.append(ad_group)

        # Build the request
        request = client.get_type("GenerateKeywordForecastMetricsRequest")
        request.customer_id = self.handler.customer_id
        request.campaign = campaign

        forecast_period = client.get_type("DateRange")
        forecast_period.start_date = start
        forecast_period.end_date = end
        request.forecast_period = forecast_period

        logger.debug(f"KeywordForecastMetricsTable: language={language}, geo={geo_target}, "
                      f"network={network}, keywords={kw_list}, max_cpc={max_cpc_value}, "
                      f"match_type={match_type_str}, period={start}..{end}")

        response = kp_service.generate_keyword_forecast_metrics(request=request)

        rows = []
        for i, kw_forecast in enumerate(response.keyword_forecasts):
            m = kw_forecast.keyword_forecast
            kw_text = kw_list[i] if i < len(kw_list) else None
            rows.append({
                'keyword': kw_text,
                'match_type': match_type_str,
                'impressions': m.impressions if m else None,
                'clicks': m.clicks if m else None,
                'cost_micros': m.cost_micros if m else None,
                'average_cpc_micros': m.average_cpc_micros if m else None,
                'ctr': m.ctr if m else None,
            })

        df = pd.DataFrame(rows, columns=self.get_columns()) if rows else pd.DataFrame(columns=self.get_columns())
        return df[_pattern_a_columns(query, self.get_columns())]
