import time
import tempfile
import os
from datetime import date, timedelta

import pandas as pd
from mindsdb_sql_parser import ast

from mindsdb.integrations.libs.api_handler import APITable, APIResource
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions, FilterOperator
from mindsdb.utilities import log

logger = log.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared utilities
# ---------------------------------------------------------------------------

def _soap_to_dict(obj, field_map):
    """Convert a SOAP object to a dict using field_map {output_key: 'Attr.SubAttr'}."""
    row = {}
    for key, attr_path in field_map.items():
        val = obj
        for part in attr_path.split('.'):
            val = getattr(val, part, None)
            if val is None:
                break
        row[key] = val
    return row


def _soap_list_to_df(items, field_map, columns):
    """Convert a list of SOAP objects to a DataFrame."""
    if items is None:
        return pd.DataFrame(columns=columns)
    # The bingads SDK wraps lists in an object with a single attribute
    # e.g. response.Campaign is a list-like SOAP array
    if hasattr(items, '__iter__'):
        rows = [_soap_to_dict(item, field_map) for item in items]
    else:
        rows = [_soap_to_dict(items, field_map)]
    if not rows:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(rows, columns=columns)


def _get_condition_value(conditions, column_name):
    """Extract value for a column from FilterCondition list, marking it as applied."""
    for cond in conditions:
        if cond.column == column_name and cond.op == FilterOperator.EQUAL:
            cond.applied = True
            return cond.value
    return None


# ---------------------------------------------------------------------------
# Entity Tables (APIResource — get free WHERE/LIMIT/ORDER BY post-processing)
# ---------------------------------------------------------------------------

class CampaignsTable(APIResource):
    """Microsoft Advertising campaigns for the account."""

    FIELD_MAP = {
        'id': 'Id',
        'name': 'Name',
        'status': 'Status',
        'campaign_type': 'CampaignType',
        'budget_type': 'BudgetType',
        'daily_budget': 'DailyBudget',
        'time_zone': 'TimeZone',
    }

    def list(self, conditions=None, limit=None, sort=None, targets=None, **kwargs):
        self.handler.connect()
        service = self.handler.campaign_service

        response = service.GetCampaignsByAccountId(
            AccountId=self.handler.account_id,
        )
        campaigns = response.Campaign if response else []
        return _soap_list_to_df(campaigns, self.FIELD_MAP, self.get_columns())

    def get_columns(self):
        return list(self.FIELD_MAP.keys())


class AdGroupsTable(APIResource):
    """Ad groups within campaigns. Use WHERE campaign_id = '...' for direct lookup."""

    FIELD_MAP = {
        'id': 'Id',
        'name': 'Name',
        'campaign_id': 'CampaignId',
        'status': 'Status',
        'cpc_bid': 'CpcBid.Amount',
        'language': 'Language',
        'network': 'Network',
        'start_date': 'StartDate',
        'end_date': 'EndDate',
    }

    def list(self, conditions=None, limit=None, sort=None, targets=None, **kwargs):
        conditions = conditions or []
        self.handler.connect()
        service = self.handler.campaign_service

        campaign_id = _get_condition_value(conditions, 'campaign_id')

        if campaign_id:
            response = service.GetAdGroupsByCampaignId(CampaignId=int(campaign_id))
            ad_groups = response.AdGroup if response else []
            df = _soap_list_to_df(ad_groups, self.FIELD_MAP, self.get_columns())
        else:
            # Iterate all campaigns
            campaigns_response = service.GetCampaignsByAccountId(
                AccountId=self.handler.account_id,
            )
            campaigns = campaigns_response.Campaign if campaigns_response else []
            dfs = []
            for c in campaigns:
                try:
                    resp = service.GetAdGroupsByCampaignId(CampaignId=c.Id)
                    groups = resp.AdGroup if resp else []
                    dfs.append(_soap_list_to_df(groups, self.FIELD_MAP, self.get_columns()))
                except Exception as e:
                    logger.debug(f"Skipping ad groups for campaign {c.Id}: {e}")
            df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame(columns=self.get_columns())

        return df

    def get_columns(self):
        return list(self.FIELD_MAP.keys())


class AdsTable(APIResource):
    """Ads within ad groups. Use WHERE ad_group_id = '...' for direct lookup."""

    FIELD_MAP = {
        'id': 'Id',
        'ad_group_id': 'AdGroupId',
        'type': 'Type',
        'status': 'Status',
        'final_urls': 'FinalUrls',
        'title_part1': 'TitlePart1',
        'title_part2': 'TitlePart2',
        'title_part3': 'TitlePart3',
        'description': 'Descriptions',
        'path1': 'Path1',
        'path2': 'Path2',
    }

    def list(self, conditions=None, limit=None, sort=None, targets=None, **kwargs):
        conditions = conditions or []
        self.handler.connect()
        service = self.handler.campaign_service

        ad_group_id = _get_condition_value(conditions, 'ad_group_id')
        campaign_id = _get_condition_value(conditions, 'campaign_id')

        if ad_group_id:
            response = service.GetAdsByAdGroupId(
                AdGroupId=int(ad_group_id),
                AdTypes={'AdType': ['ExpandedText', 'ResponsiveSearch']},
            )
            ads = response.Ad if response else []
            return _soap_list_to_df(ads, self.FIELD_MAP, self.get_columns())

        # Fall back to iterating ad groups (optionally scoped by campaign_id)
        ad_groups_table = AdGroupsTable(self.handler)
        from mindsdb.integrations.utilities.sql_utils import FilterCondition
        ag_conditions = []
        if campaign_id:
            ag_conditions.append(FilterCondition('campaign_id', FilterOperator.EQUAL, campaign_id))
        ag_df = ad_groups_table.list(conditions=ag_conditions)

        dfs = []
        for ag_id in ag_df['id']:
            try:
                resp = service.GetAdsByAdGroupId(
                    AdGroupId=int(ag_id),
                    AdTypes={'AdType': ['ExpandedText', 'ResponsiveSearch']},
                )
                ads = resp.Ad if resp else []
                dfs.append(_soap_list_to_df(ads, self.FIELD_MAP, self.get_columns()))
            except Exception as e:
                logger.debug(f"Skipping ads for ad group {ag_id}: {e}")
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame(columns=self.get_columns())

    def get_columns(self):
        return list(self.FIELD_MAP.keys())


class KeywordsTable(APIResource):
    """Keywords within ad groups. Use WHERE ad_group_id = '...' for direct lookup."""

    FIELD_MAP = {
        'id': 'Id',
        'ad_group_id': 'AdGroupId',
        'text': 'Text',
        'match_type': 'MatchType',
        'status': 'Status',
        'bid_amount': 'Bid.Amount',
        'quality_score': 'QualityScore',
        'final_urls': 'FinalUrls',
    }

    def list(self, conditions=None, limit=None, sort=None, targets=None, **kwargs):
        conditions = conditions or []
        self.handler.connect()
        service = self.handler.campaign_service

        ad_group_id = _get_condition_value(conditions, 'ad_group_id')
        campaign_id = _get_condition_value(conditions, 'campaign_id')

        if ad_group_id:
            response = service.GetKeywordsByAdGroupId(AdGroupId=int(ad_group_id))
            keywords = response.Keyword if response else []
            return _soap_list_to_df(keywords, self.FIELD_MAP, self.get_columns())

        # Fall back to iterating ad groups
        ad_groups_table = AdGroupsTable(self.handler)
        from mindsdb.integrations.utilities.sql_utils import FilterCondition
        ag_conditions = []
        if campaign_id:
            ag_conditions.append(FilterCondition('campaign_id', FilterOperator.EQUAL, campaign_id))
        ag_df = ad_groups_table.list(conditions=ag_conditions)

        dfs = []
        for ag_id in ag_df['id']:
            try:
                resp = service.GetKeywordsByAdGroupId(AdGroupId=int(ag_id))
                kws = resp.Keyword if resp else []
                dfs.append(_soap_list_to_df(kws, self.FIELD_MAP, self.get_columns()))
            except Exception as e:
                logger.debug(f"Skipping keywords for ad group {ag_id}: {e}")
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame(columns=self.get_columns())

    def get_columns(self):
        return list(self.FIELD_MAP.keys())


# ---------------------------------------------------------------------------
# Report Tables (APITable — custom select() for date-range + async download)
# ---------------------------------------------------------------------------

# Column name mapping: Microsoft Ads PascalCase report headers → snake_case
CAMPAIGN_PERF_COLUMN_MAP = {
    'TimePeriod': 'date',
    'CampaignId': 'campaign_id',
    'CampaignName': 'campaign_name',
    'AccountId': 'account_id',
    'Impressions': 'impressions',
    'Clicks': 'clicks',
    'Spend': 'spend',
    'Ctr': 'ctr',
    'AverageCpc': 'average_cpc',
    'AverageCpm': 'average_cpm',
    'Conversions': 'conversions',
    'ConversionRate': 'conversion_rate',
    'CostPerConversion': 'cost_per_conversion',
    'Revenue': 'revenue',
}

SEARCH_TERMS_COLUMN_MAP = {
    'TimePeriod': 'date',
    'SearchQuery': 'search_query',
    'CampaignId': 'campaign_id',
    'CampaignName': 'campaign_name',
    'AdGroupId': 'ad_group_id',
    'AdGroupName': 'ad_group_name',
    'Keyword': 'keyword_text',
    'Impressions': 'impressions',
    'Clicks': 'clicks',
    'Spend': 'spend',
    'Ctr': 'ctr',
    'AverageCpc': 'average_cpc',
    'Conversions': 'conversions',
    'ConversionRate': 'conversion_rate',
}


def _extract_date_range(where):
    """Extract start_date and end_date from WHERE clause. Raises if missing."""
    conditions = extract_comparison_conditions(where)
    start_date = None
    end_date = None
    other_conditions = []
    for cond in conditions:
        if isinstance(cond, list):
            op, col, val = cond
            if col == 'start_date' and op == '=':
                start_date = val
            elif col == 'end_date' and op == '=':
                end_date = val
            else:
                other_conditions.append(cond)
    if not end_date:
        end_date = date.today().strftime('%Y-%m-%d')
    if not start_date:
        start_date = (date.today() - timedelta(days=30)).strftime('%Y-%m-%d')
    return start_date, end_date, other_conditions


def _make_report_date(service, date_str):
    """Create a Microsoft Ads Date SOAP object from 'YYYY-MM-DD' string."""
    date_obj = service.factory.create('Date')
    parts = date_str.split('-')
    date_obj.Year = int(parts[0])
    date_obj.Month = int(parts[1])
    date_obj.Day = int(parts[2])
    return date_obj


def _download_report(reporting_service, report_request, timeout_seconds=300):
    """Submit, poll, and download a Microsoft Ads report. Returns DataFrame or None."""
    # Submit the report request
    report_request_id = reporting_service.SubmitGenerateReport(ReportRequest=report_request)

    # Poll until complete
    poll_interval = 5
    elapsed = 0
    while elapsed < timeout_seconds:
        report_status = reporting_service.PollGenerateReport(ReportRequestId=report_request_id)
        status = report_status.Status

        if status == 'Success':
            download_url = report_status.ReportDownloadUrl
            if download_url is None:
                return None  # No data for the requested period
            break
        elif status == 'Error':
            raise RuntimeError(f"Report generation failed: {report_status}")
        # status == 'Pending' — keep polling
        time.sleep(poll_interval)
        elapsed += poll_interval
    else:
        raise TimeoutError(f"Report not ready after {timeout_seconds}s")

    # Download the report CSV
    import urllib.request
    import zipfile
    import io

    with urllib.request.urlopen(download_url) as resp:
        data = resp.read()

    # Bing reports are typically delivered as a ZIP containing a single CSV
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            csv_name = zf.namelist()[0]
            csv_bytes = zf.read(csv_name)
    except zipfile.BadZipFile:
        # Not zipped — raw CSV
        csv_bytes = data

    # Parse CSV — Bing reports have a variable number of header/footer rows.
    # The actual data starts after lines beginning with non-data content.
    csv_text = csv_bytes.decode('utf-8-sig')
    lines = csv_text.strip().split('\n')

    # Find the header row (first row that looks like column headers)
    header_idx = 0
    for i, line in enumerate(lines):
        # Header row contains known column names like "TimePeriod" or "CampaignId"
        if 'TimePeriod' in line or 'CampaignId' in line or 'SearchQuery' in line:
            header_idx = i
            break

    # Find footer (lines starting with copyright or summary markers)
    data_end = len(lines)
    for i in range(len(lines) - 1, header_idx, -1):
        line = lines[i].strip()
        if line.startswith('\u00a9') or line.startswith('Copyright') or line == '':
            data_end = i
        else:
            break

    if header_idx >= data_end:
        return None

    data_lines = lines[header_idx:data_end]
    csv_content = '\n'.join(data_lines)
    df = pd.read_csv(io.StringIO(csv_content))
    return df


class CampaignPerformanceTable(APITable):
    """Campaign performance report with daily metrics.

    Required WHERE: start_date = '...', end_date = '...'
    Optional WHERE: campaign_id = '...'
    """

    REPORT_COLUMNS = list(CAMPAIGN_PERF_COLUMN_MAP.keys())
    OUTPUT_COLUMNS = list(CAMPAIGN_PERF_COLUMN_MAP.values())

    def select(self, query):
        self.handler.connect()
        service = self.handler.reporting_service

        start_date, end_date, other_conditions = _extract_date_range(query.where)

        # Build report request
        report_request = service.factory.create('CampaignPerformanceReportRequest')
        report_request.Format = 'Csv'
        report_request.ReportName = 'CampaignPerformance'
        report_request.ReturnOnlyCompleteData = False
        report_request.Aggregation = 'Daily'

        # Time period
        report_time = service.factory.create('ReportTime')
        report_time.CustomDateRangeStart = _make_report_date(service, start_date)
        report_time.CustomDateRangeEnd = _make_report_date(service, end_date)
        report_time.PredefinedTime = None
        report_time.ReportTimeZone = 'GreenwichMeanTimeDublinEdinburghLisbonLondon'
        report_request.Time = report_time

        # Columns to request
        report_columns = service.factory.create('ArrayOfCampaignPerformanceReportColumn')
        for col in self.REPORT_COLUMNS:
            report_columns.CampaignPerformanceReportColumn.append(col)
        report_request.Columns = report_columns

        # Scope — account level, optionally filtered to specific campaign
        scope = service.factory.create('AccountThroughCampaignReportScope')
        scope.AccountIds = {'long': [int(self.handler.account_id)]}
        scope.Campaigns = None

        # Optional campaign_id filter
        campaign_id = None
        for cond in other_conditions:
            op, col, val = cond
            if col == 'campaign_id' and op == '=':
                campaign_id = val

        if campaign_id:
            campaigns_scope = service.factory.create('ArrayOfCampaignReportScope')
            campaign_scope = service.factory.create('CampaignReportScope')
            campaign_scope.CampaignId = int(campaign_id)
            campaign_scope.AccountId = int(self.handler.account_id)
            campaigns_scope.CampaignReportScope.append(campaign_scope)
            scope.Campaigns = campaigns_scope

        report_request.Scope = scope

        # Download and parse
        df = _download_report(service, report_request)
        if df is None or df.empty:
            return pd.DataFrame(columns=self.OUTPUT_COLUMNS)

        # Rename columns from PascalCase to snake_case
        df = df.rename(columns=CAMPAIGN_PERF_COLUMN_MAP)

        # Keep only expected columns (report may include extras)
        for col in self.OUTPUT_COLUMNS:
            if col not in df.columns:
                df[col] = None
        df = df[self.OUTPUT_COLUMNS]

        return df

    def get_columns(self):
        return self.OUTPUT_COLUMNS


class SearchTermsTable(APITable):
    """Search query performance report.

    Required WHERE: start_date = '...', end_date = '...'
    Optional WHERE: campaign_id = '...'
    """

    REPORT_COLUMNS = list(SEARCH_TERMS_COLUMN_MAP.keys())
    OUTPUT_COLUMNS = list(SEARCH_TERMS_COLUMN_MAP.values())

    def select(self, query):
        self.handler.connect()
        service = self.handler.reporting_service

        start_date, end_date, other_conditions = _extract_date_range(query.where)

        # Build report request
        report_request = service.factory.create('SearchQueryPerformanceReportRequest')
        report_request.Format = 'Csv'
        report_request.ReportName = 'SearchTerms'
        report_request.ReturnOnlyCompleteData = False
        report_request.Aggregation = 'Daily'

        # Time period
        report_time = service.factory.create('ReportTime')
        report_time.CustomDateRangeStart = _make_report_date(service, start_date)
        report_time.CustomDateRangeEnd = _make_report_date(service, end_date)
        report_time.PredefinedTime = None
        report_time.ReportTimeZone = 'GreenwichMeanTimeDublinEdinburghLisbonLondon'
        report_request.Time = report_time

        # Columns to request
        report_columns = service.factory.create('ArrayOfSearchQueryPerformanceReportColumn')
        for col in self.REPORT_COLUMNS:
            report_columns.SearchQueryPerformanceReportColumn.append(col)
        report_request.Columns = report_columns

        # Scope
        scope = service.factory.create('AccountThroughAdGroupReportScope')
        scope.AccountIds = {'long': [int(self.handler.account_id)]}
        scope.Campaigns = None
        scope.AdGroups = None

        # Optional campaign_id filter
        campaign_id = None
        for cond in other_conditions:
            op, col, val = cond
            if col == 'campaign_id' and op == '=':
                campaign_id = val

        if campaign_id:
            campaigns_scope = service.factory.create('ArrayOfCampaignReportScope')
            campaign_scope = service.factory.create('CampaignReportScope')
            campaign_scope.CampaignId = int(campaign_id)
            campaign_scope.AccountId = int(self.handler.account_id)
            campaigns_scope.CampaignReportScope.append(campaign_scope)
            scope.Campaigns = campaigns_scope

        report_request.Scope = scope

        # Download and parse
        df = _download_report(service, report_request)
        if df is None or df.empty:
            return pd.DataFrame(columns=self.OUTPUT_COLUMNS)

        # Rename columns
        df = df.rename(columns=SEARCH_TERMS_COLUMN_MAP)

        # Keep only expected columns
        for col in self.OUTPUT_COLUMNS:
            if col not in df.columns:
                df[col] = None
        df = df[self.OUTPUT_COLUMNS]

        return df

    def get_columns(self):
        return self.OUTPUT_COLUMNS
