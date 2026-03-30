"""Utilities for LinkedIn Ads handler.

Includes URN handling, search expression building, data normalization,
and date/money extraction helpers.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from urllib.parse import quote
from typing import Any

from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator, SortColumn


# ============================================================================
# Sort and Filter Helpers
# ============================================================================


def build_sort_order(sort: list[SortColumn] | None) -> str:
    """Build LinkedIn API sort order from sort columns.

    Args:
        sort: List of sort columns (only first is used)

    Returns:
        "ASCENDING" or "DESCENDING"
    """
    if not sort:
        return "ASCENDING"
    direction = getattr(sort[0], "direction", None)
    return "DESCENDING" if str(direction).lower() == "desc" else "ASCENDING"


def extract_search_filters(
    conditions: list[FilterCondition] | None,
    allowed_filters: dict[str, str],
) -> dict[str, list[str]] | None:
    """Extract search filters from SQL conditions.

    Args:
        conditions: List of filter conditions from SQL query
        allowed_filters: Mapping of SQL column names to API filter names

    Returns:
        Dictionary of API filter name -> list of values, or None if no filters
    """
    if not conditions:
        return None

    filters: dict[str, list[str]] = {}
    for condition in conditions:
        field = allowed_filters.get(condition.column)
        if field is None:
            continue

        values: list[str] | None = None
        if condition.op == FilterOperator.EQUAL:
            values = [format_search_value(condition.value)]
        elif condition.op == FilterOperator.IN and isinstance(condition.value, list):
            values = [format_search_value(value) for value in condition.value]

        if not values:
            continue

        filters.setdefault(field, []).extend(values)
        condition.applied = True

    return filters or None


def format_search_value(value: Any) -> str:
    """Format a value for LinkedIn search API.

    Args:
        value: Value to format

    Returns:
        String representation suitable for API
    """
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def extract_search_id_values(search_filters: dict[str, list[str]] | None) -> list[str] | None:
    """Extract ID values from search filters.

    Args:
        search_filters: Search filters dictionary

    Returns:
        List of ID values or None
    """
    if not search_filters:
        return None
    values = search_filters.get("id")
    if not values:
        return None
    return list(values)


def normalize_search_id_filters(
    search_filters: dict[str, list[str]] | None,
    prefix: str,
) -> dict[str, list[str]] | None:
    """Add URN prefix to ID filters if not already present.

    Args:
        search_filters: Search filters dictionary
        prefix: URN prefix to add (e.g., "urn:li:sponsoredCampaign:")

    Returns:
        Modified filters dictionary or None
    """
    if not search_filters or "id" not in search_filters:
        return search_filters

    normalized = dict(search_filters)
    normalized["id"] = [
        value if str(value).startswith(prefix) else f"{prefix}{value}"
        for value in search_filters["id"]
    ]
    return normalized


def build_search_expression(search_filters: dict[str, list[str]]) -> str:
    """Build LinkedIn RestLi search expression from filters.

    Args:
        search_filters: Dictionary of field -> list of values

    Returns:
        RestLi search expression string
    """
    parts = [
        f"{field}:(values:{restli_list(values)})"
        for field, values in search_filters.items()
        if values
    ]
    return f"({','.join(parts)})" if parts else ""


def build_search_request_url(
    endpoint: str,
    search_expression: str,
    page_size: int,
    sort_order: str,
    page_token: str | None = None,
) -> str:
    """Build full search request URL with parameters.

    Args:
        endpoint: API endpoint base URL
        search_expression: RestLi search expression
        page_size: Number of results per page
        sort_order: "ASCENDING" or "DESCENDING"
        page_token: Optional pagination token

    Returns:
        Complete URL string
    """
    url = (
        f"{endpoint}?q=search&search={search_expression}"
        f"&pageSize={page_size}&sortOrder={sort_order}"
    )
    if page_token:
        url += f"&pageToken={page_token}"
    return url


def build_raw_request_url(endpoint: str, params: dict[str, Any]) -> str:
    """Build URL with parameters without encoding.

    Used for analytics endpoints that need pre-formatted param values.

    Args:
        endpoint: API endpoint base URL
        params: Query parameters

    Returns:
        Complete URL string
    """
    query_parts = [f"{key}={value}" for key, value in params.items()]
    return f"{endpoint}?{'&'.join(query_parts)}"


def filter_rows_locally(
    rows: list[dict[str, Any]],
    search_filters: dict[str, list[str]] | None,
) -> list[dict[str, Any]]:
    """Apply search filters locally to rows.

    Used when API doesn't support certain filters directly.

    Args:
        rows: List of result rows
        search_filters: Filters to apply

    Returns:
        Filtered list of rows
    """
    if not search_filters:
        return rows

    filtered = rows
    for field, values in search_filters.items():
        if not values or field == "id":
            continue

        if field == "status":
            allowed = {str(value).upper() for value in values}
            filtered = [row for row in filtered if str(row.get("status", "")).upper() in allowed]
            continue

        if field == "name":
            allowed = {str(value) for value in values}
            filtered = [row for row in filtered if str(row.get("name")) in allowed]
            continue

        if field == "test":
            allowed = {str(value).lower() == "true" for value in values}
            filtered = [row for row in filtered if bool(row.get("test")) in allowed]

    return filtered


def sort_rows_locally(
    rows: list[dict[str, Any]],
    sort: list[SortColumn] | None,
) -> list[dict[str, Any]]:
    """Apply sort locally to rows.

    Args:
        rows: List of result rows
        sort: Sort columns to apply

    Returns:
        Sorted list of rows
    """
    if not sort:
        return rows

    sorted_rows = list(rows)
    for sort_column in reversed(sort):
        column = getattr(sort_column, "column", None)
        if not column:
            continue
        direction = getattr(sort_column, "direction", None)
        reverse = str(direction).lower() == "desc"
        sorted_rows.sort(key=lambda row: row.get(column), reverse=reverse)
        sort_column.applied = True
    return sorted_rows


# ============================================================================
# URN and RestLi Formatting
# ============================================================================


def extract_urn_id(value: Any) -> Any:
    """Extract numeric ID from URN string.

    Args:
        value: URN string or plain ID

    Returns:
        ID portion of URN or original value
    """
    if isinstance(value, str) and ":" in value:
        return value.rsplit(":", 1)[-1]
    return value


def restli_list(values: list[str], encode_values: bool = False) -> str:
    """Format values as RestLi List(...) syntax.

    Args:
        values: List of string values
        encode_values: Whether to URL-encode values

    Returns:
        RestLi list string like "List(val1,val2)"
    """
    prepared_values = [quote(value, safe="") if encode_values else value for value in values]
    return "List(" + ",".join(prepared_values) + ")"


# ============================================================================
# Creative Filter Building
# ============================================================================


def build_creative_params(
    conditions: list[FilterCondition] | None,
    creative_filters: dict[str, str],
    sort: list[SortColumn] | None = None,
) -> dict[str, Any]:
    """Build query parameters for creative criteria endpoint.

    Args:
        conditions: Filter conditions from SQL
        creative_filters: Mapping of column names to API criteria fields
        sort: Sort columns

    Returns:
        Dictionary of query parameters
    """
    params: dict[str, Any] = {"q": "criteria"}
    if sort:
        sort_order = build_sort_order(sort)
        params["sortOrder"] = sort_order

    for condition in conditions or []:
        field = creative_filters.get(condition.column)
        if field is None:
            continue

        values: list[str] | None = None
        if condition.op == FilterOperator.EQUAL:
            values = [format_creative_filter_value(condition.column, condition.value)]
        elif condition.op == FilterOperator.IN and isinstance(condition.value, list):
            values = [format_creative_filter_value(condition.column, value) for value in condition.value]

        if not values:
            continue

        params[field] = restli_list(values)
        condition.applied = True

    return params


def format_creative_filter_value(column: str, value: Any) -> str:
    """Format creative filter value with proper URN prefix.

    Args:
        column: SQL column name
        value: Filter value

    Returns:
        Formatted value string
    """
    if column == "creative_id":
        return f"urn:li:sponsoredCreative:{value}"
    if column == "campaign_id":
        return f"urn:li:sponsoredCampaign:{value}"
    if column == "content_reference_id":
        return f"urn:li:share:{value}"
    return format_search_value(value)


# ============================================================================
# Analytics Parameter Building
# ============================================================================


def build_campaign_analytics_params(
    conditions: list[FilterCondition] | None,
    account_id: str,
    default_lookback_days: int = 30,
    supported_granularities: set[str] = None,
    analytics_fields: tuple[str, ...] = None,
) -> dict[str, Any]:
    """Build query parameters for campaign analytics endpoint.

    Args:
        conditions: Filter conditions from SQL
        account_id: LinkedIn Ads account ID
        default_lookback_days: Default date range if not specified
        supported_granularities: Set of valid time granularities
        analytics_fields: Tuple of field names to fetch

    Returns:
        Dictionary of query parameters
    """
    if supported_granularities is None:
        supported_granularities = {"ALL", "DAILY", "MONTHLY"}
    if analytics_fields is None:
        analytics_fields = (
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

    time_granularity = "DAILY"
    start_date: date | None = None
    end_date: date | None = None
    campaign_urns: list[str] = []

    for condition in conditions or []:
        # Campaign ID filters
        if condition.column in {"campaign_id", "id"}:
            values = extract_list_values(condition, prefix="urn:li:sponsoredCampaign:")
            if values:
                campaign_urns.extend(values)
                condition.applied = True
            continue

        if condition.column == "campaign_urn":
            values = extract_list_values(condition)
            if values:
                campaign_urns.extend(values)
                condition.applied = True
            continue

        # Time granularity
        if condition.column == "time_granularity" and condition.op == FilterOperator.EQUAL:
            candidate = str(condition.value).upper()
            if candidate in supported_granularities:
                time_granularity = candidate
                condition.applied = True
            continue

        # Date range
        if condition.column in {"date", "date_start", "start_date", "date_end", "end_date"}:
            start_date, end_date, applied = merge_date_condition(condition, start_date, end_date)
            if applied:
                condition.applied = True

    # Apply defaults for date range
    today = datetime.now().date()
    if end_date is None:
        end_date = today
    if start_date is None:
        start_date = end_date - timedelta(days=default_lookback_days - 1)
    if start_date > end_date:
        raise ValueError("LinkedIn Ads analytics start_date cannot be after end_date")

    params: dict[str, Any] = {
        "q": "analytics",
        "pivot": "CAMPAIGN",
        "timeGranularity": time_granularity,
        "dateRange": build_date_range_param(start_date, end_date),
        "fields": ",".join(analytics_fields),
    }

    if campaign_urns:
        params["campaigns"] = restli_list(sorted(set(campaign_urns)), encode_values=True)
    else:
        params["accounts"] = restli_list([f"urn:li:sponsoredAccount:{account_id}"], encode_values=True)

    return params


def extract_list_values(condition: FilterCondition, prefix: str | None = None) -> list[str] | None:
    """Extract list values from a filter condition.

    Args:
        condition: Filter condition
        prefix: Optional URN prefix to add to values

    Returns:
        List of string values or None
    """
    raw_values: list[Any] | None = None
    if condition.op == FilterOperator.EQUAL:
        raw_values = [condition.value]
    elif condition.op == FilterOperator.IN and isinstance(condition.value, list):
        raw_values = condition.value

    if not raw_values:
        return None

    values = []
    for value in raw_values:
        string_value = str(value)
        if prefix and not string_value.startswith(prefix):
            string_value = f"{prefix}{string_value}"
        values.append(string_value)
    return values


def merge_date_condition(
    condition: FilterCondition,
    current_start: date | None,
    current_end: date | None,
) -> tuple[date | None, date | None, bool]:
    """Merge a date condition into current date range.

    Args:
        condition: Filter condition
        current_start: Current start date
        current_end: Current end date

    Returns:
        Tuple of (new_start, new_end, was_applied)
    """
    if condition.op == FilterOperator.BETWEEN and isinstance(condition.value, tuple) and len(condition.value) == 2:
        start_candidate = coerce_date(condition.value[0])
        end_candidate = coerce_date(condition.value[1])
        if start_candidate and end_candidate:
            return start_candidate, end_candidate, True
        return current_start, current_end, False

    value = coerce_date(condition.value)
    if value is None:
        return current_start, current_end, False

    if condition.column == "date":
        if condition.op == FilterOperator.EQUAL:
            return value, value, True
        if condition.op == FilterOperator.GREATER_THAN:
            return value + timedelta(days=1), current_end, True
        if condition.op == FilterOperator.GREATER_THAN_OR_EQUAL:
            return value, current_end, True
        if condition.op == FilterOperator.LESS_THAN:
            return current_start, value - timedelta(days=1), True
        if condition.op == FilterOperator.LESS_THAN_OR_EQUAL:
            return current_start, value, True
        return current_start, current_end, False

    if condition.column in {"date_start", "start_date"}:
        if condition.op == FilterOperator.EQUAL:
            return value, current_end, True
        if condition.op == FilterOperator.GREATER_THAN:
            return value + timedelta(days=1), current_end, True
        if condition.op == FilterOperator.GREATER_THAN_OR_EQUAL:
            return value, current_end, True
        return current_start, current_end, False

    if condition.column in {"date_end", "end_date"}:
        if condition.op == FilterOperator.EQUAL:
            return current_start, value, True
        if condition.op == FilterOperator.LESS_THAN:
            return current_start, value - timedelta(days=1), True
        if condition.op == FilterOperator.LESS_THAN_OR_EQUAL:
            return current_start, value, True
        return current_start, current_end, False

    return current_start, current_end, False


def coerce_date(value: Any) -> date | None:
    """Coerce a value to a date object.

    Args:
        value: Value to coerce (str, date, datetime)

    Returns:
        Date object or None if coercion fails
    """
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value:
        normalized = value.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(normalized).date()
        except ValueError:
            try:
                return date.fromisoformat(value)
            except ValueError:
                return None
    return None


def build_date_range_param(start_date: date, end_date: date) -> str:
    """Build LinkedIn RestLi date range parameter.

    Args:
        start_date: Start date
        end_date: End date

    Returns:
        RestLi date range string
    """
    return (
        f"(start:(year:{start_date.year},month:{start_date.month},day:{start_date.day}),"
        f"end:(year:{end_date.year},month:{end_date.month},day:{end_date.day}))"
    )


# ============================================================================
# Data Normalization
# ============================================================================


def extract_money(money: Any) -> tuple[Any, Any]:
    """Extract amount and currency from money object.

    Args:
        money: Money dictionary from API

    Returns:
        Tuple of (amount, currency_code)
    """
    if isinstance(money, dict):
        return money.get("amount"), money.get("currencyCode")
    return None, None


def normalize_multi_value(value: Any) -> str | None:
    """Normalize multi-value field to comma-separated string.

    Args:
        value: List or string value

    Returns:
        Comma-separated string or None
    """
    if isinstance(value, list):
        return ",".join(str(item) for item in value)
    if isinstance(value, str):
        return value
    return None


def normalize_date_dict(value: Any) -> str | None:
    """Normalize LinkedIn date dict to ISO date string.

    Args:
        value: Dictionary with year, month, day keys

    Returns:
        ISO date string or None
    """
    if not isinstance(value, dict):
        return None
    year = value.get("year")
    month = value.get("month")
    day = value.get("day")
    if not isinstance(year, int) or not isinstance(month, int) or not isinstance(day, int):
        return None
    try:
        return date(year, month, day).isoformat()
    except ValueError:
        return None


def normalize_creative(item: dict[str, Any]) -> dict[str, Any]:
    """Normalize creative API response to standard format.

    Args:
        item: Raw creative from API

    Returns:
        Normalized creative dictionary
    """
    creative_urn = item.get("id")
    campaign_urn = item.get("campaign")
    account_urn = item.get("account")
    content = item.get("content") if isinstance(item.get("content"), dict) else {}
    content_reference = content.get("reference")

    return {
        "id": creative_urn,
        "creative_id": extract_urn_id(creative_urn),
        "campaign_urn": campaign_urn,
        "campaign_id": extract_urn_id(campaign_urn),
        "account_urn": account_urn,
        "account_id": extract_urn_id(account_urn),
        "intended_status": item.get("intendedStatus"),
        "is_test": item.get("isTest"),
        "is_serving": item.get("isServing"),
        "serving_hold_reasons": normalize_multi_value(item.get("servingHoldReasons")),
        "content_reference": content_reference,
        "content_reference_id": extract_urn_id(content_reference),
        "created_at": item.get("createdAt"),
        "last_modified_at": item.get("lastModifiedAt"),
        "created_by": item.get("createdBy"),
        "last_modified_by": item.get("lastModifiedBy"),
    }


def normalize_campaign_analytics(item: dict[str, Any], time_granularity: str) -> dict[str, Any]:
    """Normalize campaign analytics API response to standard format.

    Args:
        item: Raw analytics row from API
        time_granularity: Time granularity used in query

    Returns:
        Normalized analytics dictionary
    """
    pivot_values = item.get("pivotValues") if isinstance(item.get("pivotValues"), list) else []
    campaign_urn = pivot_values[0] if pivot_values else None
    date_range = item.get("dateRange") if isinstance(item.get("dateRange"), dict) else {}
    start = date_range.get("start") if isinstance(date_range.get("start"), dict) else None
    end = date_range.get("end") if isinstance(date_range.get("end"), dict) else None

    return {
        "campaign_urn": campaign_urn,
        "campaign_id": extract_urn_id(campaign_urn),
        "date_start": normalize_date_dict(start),
        "date_end": normalize_date_dict(end),
        "time_granularity": time_granularity,
        "impressions": item.get("impressions"),
        "clicks": item.get("clicks"),
        "landing_page_clicks": item.get("landingPageClicks"),
        "likes": item.get("likes"),
        "shares": item.get("shares"),
        "cost_in_local_currency": item.get("costInLocalCurrency"),
        "external_website_conversions": item.get("externalWebsiteConversions"),
    }


def normalize_campaign(item: dict[str, Any]) -> dict[str, Any]:
    """Normalize campaign API response to standard format.

    Args:
        item: Raw campaign from API

    Returns:
        Normalized campaign dictionary
    """
    daily_budget_amount, daily_budget_currency_code = extract_money(item.get("dailyBudget"))
    total_budget_amount, total_budget_currency_code = extract_money(item.get("totalBudget"))
    unit_cost_amount, unit_cost_currency_code = extract_money(item.get("unitCost"))
    run_schedule = item.get("runSchedule") if isinstance(item.get("runSchedule"), dict) else {}
    audit = item.get("changeAuditStamps") if isinstance(item.get("changeAuditStamps"), dict) else {}
    created = audit.get("created") if isinstance(audit.get("created"), dict) else {}
    last_modified = audit.get("lastModified") if isinstance(audit.get("lastModified"), dict) else {}
    locale = item.get("locale") if isinstance(item.get("locale"), dict) else {}

    account_urn = item.get("account")
    campaign_group_urn = item.get("campaignGroup")
    return {
        "id": item.get("id"),
        "name": item.get("name"),
        "status": item.get("status"),
        "type": item.get("type"),
        "test": item.get("test"),
        "account_urn": account_urn,
        "account_id": extract_urn_id(account_urn),
        "campaign_group_urn": campaign_group_urn,
        "campaign_group_id": extract_urn_id(campaign_group_urn),
        "associated_entity_urn": item.get("associatedEntity"),
        "cost_type": item.get("costType"),
        "creative_selection": item.get("creativeSelection"),
        "objective_type": item.get("objectiveType"),
        "optimization_target_type": item.get("optimizationTargetType"),
        "format": item.get("format"),
        "locale_country": locale.get("country"),
        "locale_language": locale.get("language"),
        "audience_expansion_enabled": item.get("audienceExpansionEnabled"),
        "offsite_delivery_enabled": item.get("offsiteDeliveryEnabled"),
        "serving_statuses": normalize_multi_value(item.get("servingStatuses")),
        "daily_budget_amount": daily_budget_amount,
        "daily_budget_currency_code": daily_budget_currency_code,
        "total_budget_amount": total_budget_amount,
        "total_budget_currency_code": total_budget_currency_code,
        "unit_cost_amount": unit_cost_amount,
        "unit_cost_currency_code": unit_cost_currency_code,
        "run_schedule_start": run_schedule.get("start"),
        "run_schedule_end": run_schedule.get("end"),
        "created_at": created.get("time"),
        "last_modified_at": last_modified.get("time"),
    }


def normalize_campaign_group(item: dict[str, Any]) -> dict[str, Any]:
    """Normalize campaign group API response to standard format.

    Args:
        item: Raw campaign group from API

    Returns:
        Normalized campaign group dictionary
    """
    total_budget_amount, total_budget_currency_code = extract_money(item.get("totalBudget"))
    daily_budget_amount, daily_budget_currency_code = extract_money(item.get("dailyBudget"))
    run_schedule = item.get("runSchedule") if isinstance(item.get("runSchedule"), dict) else {}
    audit = item.get("changeAuditStamps") if isinstance(item.get("changeAuditStamps"), dict) else {}
    created = audit.get("created") if isinstance(audit.get("created"), dict) else {}
    last_modified = audit.get("lastModified") if isinstance(audit.get("lastModified"), dict) else {}

    account_urn = item.get("account")
    return {
        "id": item.get("id"),
        "name": item.get("name"),
        "status": item.get("status"),
        "test": item.get("test"),
        "account_urn": account_urn,
        "account_id": extract_urn_id(account_urn),
        "run_schedule_start": run_schedule.get("start"),
        "run_schedule_end": run_schedule.get("end"),
        "serving_statuses": normalize_multi_value(item.get("servingStatuses")),
        "total_budget_amount": total_budget_amount,
        "total_budget_currency_code": total_budget_currency_code,
        "daily_budget_amount": daily_budget_amount,
        "daily_budget_currency_code": daily_budget_currency_code,
        "created_at": created.get("time"),
        "last_modified_at": last_modified.get("time"),
    }
