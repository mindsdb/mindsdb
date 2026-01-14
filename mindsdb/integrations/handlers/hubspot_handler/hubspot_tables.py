from typing import List, Dict, Text, Any, Optional, Tuple, Set, Iterable

import pandas as pd
from hubspot import HubSpot
from hubspot.crm.objects import (
    SimplePublicObjectId as HubSpotObjectId,
    SimplePublicObjectBatchInput as HubSpotObjectBatchInput,
    SimplePublicObjectInputForCreate as HubSpotObjectInputCreate,
    BatchInputSimplePublicObjectBatchInputForCreate,
    BatchInputSimplePublicObjectBatchInput,
    BatchInputSimplePublicObjectId,
)
from mindsdb_sql_parser import ast as sql_ast
from mindsdb_sql_parser.ast import ASTNode

from mindsdb.integrations.utilities.handlers.query_utilities import UPDATEQueryExecutor, DELETEQueryExecutor
from mindsdb.integrations.utilities.query_traversal import query_traversal
from mindsdb.integrations.libs.api_handler import APIResource
from mindsdb.integrations.utilities.sql_utils import FilterCondition, SortColumn, extract_comparison_conditions
from mindsdb.utilities import log

logger = log.getLogger(__name__)


# Reference: https://developers.hubspot.com/docs/api-reference/crm-properties-v3/guide#create-unique-identifier-properties
PROPERTY_ALIASES = {
    "lastmodifieddate": "hs_lastmodifieddate",
    "id": "hs_object_id",
}

REVERSE_PROPERTY_ALIASES = {value: key for key, value in PROPERTY_ALIASES.items()}


def to_hubspot_property(col: str) -> str:
    """Map internal column names to HubSpot property names."""
    return PROPERTY_ALIASES.get(col, col)


def to_internal_property(prop: str) -> str:
    """Map HubSpot property names to internal column names."""
    return REVERSE_PROPERTY_ALIASES.get(prop, prop)


# Reference https://developers.hubspot.com/docs/api-reference/crm-properties-v3/guide#operators
CANONICAL_OPERATOR_MAP = {
    "=": "eq",
    "==": "eq",
    "eq": "eq",
    "!=": "neq",
    "<>": "neq",
    "ne": "neq",
    "neq": "neq",
    "<": "lt",
    "lt": "lt",
    "<=": "lte",
    "lte": "lte",
    ">": "gt",
    "gt": "gt",
    ">=": "gte",
    "gte": "gte",
    "in": "in",
    "not in": "not_in",
    "not_in": "not_in",
}

CANONICAL_TOKENS = set(CANONICAL_OPERATOR_MAP.values())

OPERATOR_MAP = {token: token.upper() for token in CANONICAL_TOKENS}

SQL_OPERATOR_MAP = {
    "eq": "=",
    "neq": "!=",
    "lt": "<",
    "lte": "<=",
    "gt": ">",
    "gte": ">=",
    "in": "in",
    "not_in": "not in",
}


def canonical_op(op: Any) -> str:
    """Normalize operators to canonical tokens used across search and post-filtering."""
    if hasattr(op, "value"):
        op = op.value
    op_str = str(op).strip().lower()
    return CANONICAL_OPERATOR_MAP.get(op_str, op_str)


HUBSPOT_TABLE_COLUMN_DEFINITIONS: Dict[str, List[Tuple[str, str, str]]] = {
    "companies": [
        ("name", "VARCHAR", "Company name"),
        ("domain", "VARCHAR", "Company domain"),
        ("industry", "VARCHAR", "Industry"),
        ("city", "VARCHAR", "City"),
        ("state", "VARCHAR", "State"),
        ("phone", "VARCHAR", "Phone number"),
        ("website", "VARCHAR", "Company website URL"),
        ("address", "VARCHAR", "Street address"),
        ("zip", "VARCHAR", "Postal code"),
        ("numberofemployees", "INTEGER", "Employee count"),
        ("annualrevenue", "DECIMAL", "Annual revenue"),
        ("lifecyclestage", "VARCHAR", "Lifecycle stage"),
        ("current_erp", "VARCHAR", "Current ERP system"),
        ("current_erp_version", "VARCHAR", "Current ERP version"),
        ("current_web_platform", "VARCHAR", "Current web platform"),
        ("accounting_software", "VARCHAR", "Accounting software"),
        ("credit_card_processor", "VARCHAR", "Credit card processor"),
        ("data_integration_platform", "VARCHAR", "Data integration platform"),
        ("marketing_platform", "VARCHAR", "Marketing automation platform"),
        ("pos_software", "VARCHAR", "POS software"),
        ("shipping_software", "VARCHAR", "Shipping software"),
        ("tax_platform", "VARCHAR", "Tax platform"),
        ("partner", "BOOLEAN", "Partner flag"),
        ("partner_type", "VARCHAR", "Partner type"),
        ("partnership_status", "VARCHAR", "Partnership status"),
        ("partner_payout_ytd", "DECIMAL", "Partner payout YTD"),
        ("partnership_commission", "DECIMAL", "Partnership commission YTD"),
        ("total_customer_value", "DECIMAL", "Total customer value"),
        ("total_revenue", "DECIMAL", "Total revenue"),
        ("createdate", "TIMESTAMP", "Creation date"),
        ("lastmodifieddate", "TIMESTAMP", "Last modification date"),
    ],
    "contacts": [
        ("email", "VARCHAR", "Email address"),
        ("firstname", "VARCHAR", "First name"),
        ("lastname", "VARCHAR", "Last name"),
        ("phone", "VARCHAR", "Phone number"),
        ("mobilephone", "VARCHAR", "Mobile phone number"),
        ("jobtitle", "VARCHAR", "Job title"),
        ("company", "VARCHAR", "Associated company"),
        ("city", "VARCHAR", "City"),
        ("website", "VARCHAR", "Website URL"),
        ("lifecyclestage", "VARCHAR", "Lifecycle stage"),
        ("hs_lead_status", "VARCHAR", "Lead status"),
        ("hubspot_owner_id", "VARCHAR", "Owner ID"),
        ("dc_contact", "BOOLEAN", "Direct Commerce contact indicator"),
        ("current_ecommerce_platform", "VARCHAR", "Current ecommerce platform"),
        ("departments", "VARCHAR", "Departments"),
        ("demo__requested", "BOOLEAN", "Demo requested flag"),
        ("linkedin_url", "VARCHAR", "LinkedIn profile URL"),
        ("referral_name", "VARCHAR", "Referral name"),
        ("referral_company_name", "VARCHAR", "Referral company name"),
        ("notes_last_contacted", "TIMESTAMP", "Last contacted timestamp"),
        ("notes_last_updated", "TIMESTAMP", "Last activity updated timestamp"),
        ("notes_next_activity_date", "TIMESTAMP", "Next activity date"),
        ("num_contacted_notes", "INTEGER", "Number of contacted notes"),
        ("hs_sales_email_last_clicked", "TIMESTAMP", "Last sales email clicked"),
        ("hs_sales_email_last_opened", "TIMESTAMP", "Last sales email opened"),
        ("createdate", "TIMESTAMP", "Creation date"),
        ("lastmodifieddate", "TIMESTAMP", "Last modification date"),
    ],
    "deals": [
        ("dealname", "VARCHAR", "Deal name"),
        ("amount", "DECIMAL", "Deal amount"),
        ("dealstage", "VARCHAR", "Deal stage"),
        ("pipeline", "VARCHAR", "Sales pipeline"),
        ("closedate", "DATE", "Expected close date"),
        ("hubspot_owner_id", "VARCHAR", "Owner ID"),
        ("closed_won_reason", "VARCHAR", "Reason deal was won"),
        ("closed_lost_reason", "VARCHAR", "Reason deal was lost"),
        ("lead_attribution", "VARCHAR", "Lead attribution"),
        ("services_requested", "VARCHAR", "Services requested"),
        ("platform", "VARCHAR", "Platform"),
        ("referral_partner", "VARCHAR", "Referral partner"),
        ("referral_commission_amount", "DECIMAL", "Referral commission amount"),
        ("tech_partners_involved", "VARCHAR", "Tech partners involved"),
        ("sales_tier", "VARCHAR", "Sales tier"),
        ("commission_status", "VARCHAR", "Commission status"),
        ("createdate", "TIMESTAMP", "Creation date"),
        ("lastmodifieddate", "TIMESTAMP", "Last modification date"),
    ],
    "tickets": [
        ("subject", "VARCHAR", "Ticket subject"),
        ("content", "TEXT", "Ticket content/description"),
        ("hs_pipeline", "VARCHAR", "Pipeline"),
        ("hs_pipeline_stage", "VARCHAR", "Pipeline stage"),
        ("hs_ticket_priority", "VARCHAR", "Priority"),
        ("hs_ticket_category", "VARCHAR", "Category"),
        ("hubspot_owner_id", "VARCHAR", "Owner ID"),
        ("createdate", "TIMESTAMP", "Creation date"),
        ("lastmodifieddate", "TIMESTAMP", "Last modification date"),
    ],
    "tasks": [
        ("hs_task_subject", "VARCHAR", "Task subject"),
        ("hs_task_body", "TEXT", "Task body/description"),
        ("hs_task_status", "VARCHAR", "Task status"),
        ("hs_task_priority", "VARCHAR", "Task priority"),
        ("hs_task_type", "VARCHAR", "Task type"),
        ("hs_timestamp", "TIMESTAMP", "Due date"),
        ("hubspot_owner_id", "VARCHAR", "Owner ID"),
        ("createdate", "TIMESTAMP", "Creation date"),
        ("lastmodifieddate", "TIMESTAMP", "Last modification date"),
    ],
    # Reference: https://developers.hubspot.com/docs/api-reference/crm-calls-v3/guide#create-a-call-engagement
    "calls": [
        ("hs_call_title", "VARCHAR", "Call title"),
        ("hs_call_body", "TEXT", "Call notes/description"),
        ("hs_call_direction", "VARCHAR", "Call direction (INBOUND/OUTBOUND)"),
        ("hs_call_disposition", "VARCHAR", "Call outcome"),
        ("hs_call_duration", "INTEGER", "Call duration in milliseconds"),
        ("hs_call_status", "VARCHAR", "Call status"),
        ("hubspot_owner_id", "VARCHAR", "Owner ID"),
        ("hs_timestamp", "TIMESTAMP", "Call timestamp"),
        ("createdate", "TIMESTAMP", "Creation date"),
        ("lastmodifieddate", "TIMESTAMP", "Last modification date"),
    ],
    # Reference: https://developers.hubspot.com/docs/api-reference/crm-emails-v3/guide#create-an-email-engagement
    "emails": [
        ("hs_email_subject", "VARCHAR", "Email subject"),
        ("hs_email_text", "TEXT", "Email body text"),
        ("hs_email_direction", "VARCHAR", "Email direction (INCOMING/FORWARDED/EMAIL)"),
        ("hs_email_status", "VARCHAR", "Email status"),
        ("hs_email_sender_email", "VARCHAR", "Sender email address"),
        ("hs_email_to_email", "VARCHAR", "Recipient email address"),
        ("hubspot_owner_id", "VARCHAR", "Owner ID"),
        ("hs_timestamp", "TIMESTAMP", "Email timestamp"),
        ("createdate", "TIMESTAMP", "Creation date"),
        ("lastmodifieddate", "TIMESTAMP", "Last modification date"),
    ],
    # Reference: https://developers.hubspot.com/docs/api-reference/crm-meetings-v3/guide#create-a-meeting-engagement
    "meetings": [
        ("hs_meeting_title", "VARCHAR", "Meeting title"),
        ("hs_meeting_body", "TEXT", "Meeting description"),
        ("hs_meeting_location", "VARCHAR", "Meeting location"),
        ("hs_meeting_outcome", "VARCHAR", "Meeting outcome"),
        ("hs_meeting_start_time", "TIMESTAMP", "Meeting start time"),
        ("hs_meeting_end_time", "TIMESTAMP", "Meeting end time"),
        ("hubspot_owner_id", "VARCHAR", "Owner ID"),
        ("hs_timestamp", "TIMESTAMP", "Meeting timestamp"),
        ("createdate", "TIMESTAMP", "Creation date"),
        ("lastmodifieddate", "TIMESTAMP", "Last modification date"),
    ],
    # Reference: https://developers.hubspot.com/docs/api-reference/crm-notes-v3/guide#create-a-note
    "notes": [
        ("hs_note_body", "TEXT", "Note content"),
        ("hubspot_owner_id", "VARCHAR", "Owner ID"),
        ("hs_timestamp", "TIMESTAMP", "Note timestamp"),
        ("createdate", "TIMESTAMP", "Creation date"),
        ("lastmodifieddate", "TIMESTAMP", "Last modification date"),
    ],
}


def _extract_in_values(value: Any) -> List[Any]:
    """
    Extract values from IN clause, handling various formats:
    - Python list/tuple/set: return as list
    - AST Tuple node: extract values from args
    - Single value: wrap in list
    """
    if hasattr(value, "args"):
        extracted = []
        for arg in value.args:
            if hasattr(arg, "value"):
                extracted.append(arg.value)
            else:
                extracted.append(arg)
        return extracted

    if isinstance(value, (list, tuple, set)):
        return list(value)

    return [value]


def _extract_scalar_value(value: Any) -> Any:
    """
    Extract scalar value from AST Constant node or return as-is.
    """
    if hasattr(value, "value") and not hasattr(value, "args"):
        return value.value
    return value


def _normalize_filter_conditions(conditions: Optional[List[FilterCondition]]) -> List[List[Any]]:
    """
    Convert FilterCondition instances into the condition format expected by query executors.
    """
    normalized: List[List[Any]] = []
    if not conditions:
        return normalized

    for condition in conditions:
        if isinstance(condition, FilterCondition):
            op = canonical_op(condition.op)
            col = to_internal_property(condition.column)
            val = condition.value

            # Check if this is an IN/NOT IN operator with AST Tuple
            if op in ("in", "not_in") and hasattr(val, "args"):
                val = _extract_in_values(val)
            else:
                val = _extract_scalar_value(val)

            normalized.append([op, col, val])
        elif isinstance(condition, (list, tuple)) and len(condition) >= 3:
            normalized.append([canonical_op(condition[0]), to_internal_property(condition[1]), condition[2]])
    return normalized


def _normalize_conditions_for_executor(conditions: List[List[Any]]) -> List[List[Any]]:
    normalized = []
    for condition in conditions:
        if len(condition) < 3:
            continue
        op, col, val = condition[0], condition[1], condition[2]
        normalized.append([SQL_OPERATOR_MAP.get(op, op), col, val])
    return normalized


def _build_hubspot_search_filters(
    conditions: List[List[Any]],
    searchable_columns: Set[str],
) -> Optional[List[Dict]]:
    """
    Convert normalized conditions to HubSpot Search API filter format.
    Returns a list of filter dicts if all conditions are supported, otherwise None.
    """
    if not conditions:
        return None

    filters: List[Dict[str, Any]] = []

    for condition in conditions:
        if not isinstance(condition, (list, tuple)) or len(condition) < 3:
            logger.debug(f"Invalid condition format: {condition}")
            return None

        operator, column, value = condition[0], condition[1], condition[2]
        operator_key = canonical_op(operator)

        if operator_key not in OPERATOR_MAP:
            logger.debug(f"Unsupported operator '{operator_key}' for HubSpot search, falling back to post-filter")
            return None

        if column not in searchable_columns:
            logger.debug(f"Column '{column}' not searchable in HubSpot, falling back to post-filter")
            return None

        property_name = to_hubspot_property(column)

        hubspot_operator = OPERATOR_MAP[operator_key]

        if hubspot_operator in {"IN", "NOT_IN"}:
            values = _extract_in_values(value)
            if not values:
                logger.warning(f"Empty IN clause values for column '{column}'")
                return None

            logger.debug(f"Building IN filter for {column}: {values}")
            filters.append(
                {
                    "propertyName": property_name,
                    "operator": hubspot_operator,
                    "values": [str(val) for val in values],
                }
            )
        else:
            actual_value = _extract_scalar_value(value)
            filters.append(
                {
                    "propertyName": property_name,
                    "operator": hubspot_operator,
                    "value": str(actual_value),
                }
            )

    if not filters:
        return None

    return filters


def _build_hubspot_search_sorts(
    sort_columns: List[SortColumn],
    searchable_columns: Set[str],
) -> Optional[List[Dict[str, Any]]]:
    if not sort_columns:
        return None

    sorts: List[Dict[str, Any]] = []
    for sort in sort_columns:
        column = to_internal_property(sort.column)
        if column not in searchable_columns:
            logger.debug(f"Column '{column}' not sortable in HubSpot, falling back to post-sort")
            return None
        sorts.append(
            {
                "propertyName": to_hubspot_property(column),
                "direction": "ASCENDING" if sort.ascending else "DESCENDING",
            }
        )
    return sorts


def _build_hubspot_properties(columns: Iterable[str]) -> List[str]:
    properties = []
    for col in columns:
        prop = to_hubspot_property(col)
        if prop == "hs_object_id":
            continue
        properties.append(prop)
    return list(dict.fromkeys(properties))


def _execute_hubspot_search(
    search_api,
    filters: List[Dict],
    properties: List[str],
    limit: Optional[int],
    to_dict_fn: callable,
    sorts: Optional[List[Dict[str, Any]]] = None,
    object_type: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Execute paginated HubSpot search with filters.
    """
    collected: List[Dict[str, Any]] = []
    remaining = limit if limit is not None else float("inf")
    after = None

    while remaining > 0:
        page_limit = min(int(remaining) if remaining != float("inf") else 200, 200)
        search_request = {
            "properties": properties,
            "limit": page_limit,
        }

        if filters:
            search_request["filterGroups"] = [{"filters": filters}]
        if sorts:
            search_request["sorts"] = sorts

        if after is not None:
            search_request["after"] = after

        if object_type is None:
            response = search_api.do_search(public_object_search_request=search_request)
        else:
            response = search_api.do_search(object_type, public_object_search_request=search_request)

        results = getattr(response, "results", []) or []
        for result in results:
            collected.append(to_dict_fn(result))
            if limit is not None and len(collected) >= limit:
                return collected

        paging = getattr(response, "paging", None)
        next_page = getattr(paging, "next", None) if paging else None
        after = getattr(next_page, "after", None) if next_page else None

        if after is None:
            break

        if remaining != float("inf"):
            remaining = limit - len(collected)

    return collected


class HubSpotAPIResource(APIResource):
    """
    Base class for HubSpot table resources with custom select handling.

    Overrides the default select() method to properly handle server-side filtering
    and avoid double-filtering issues with AST nodes.
    """

    # Reference: https://developers.hubspot.com/docs/api-reference/search/guide
    SEARCHABLE_COLUMNS: Set[str] = set()

    def select(self, query: ASTNode) -> pd.DataFrame:
        """
        Override select to handle server-side filtering properly.
        """
        conditions, order_by, result_limit = self._extract_query_params(query)
        targets = self._get_targets(query)
        original_targets = list(targets)
        normalized_conditions = _normalize_filter_conditions(conditions)
        self._validate_query_columns(targets, normalized_conditions, order_by)

        filters = (
            _build_hubspot_search_filters(normalized_conditions, self.SEARCHABLE_COLUMNS)
            if normalized_conditions
            else None
        )
        sorts = _build_hubspot_search_sorts(order_by, self.SEARCHABLE_COLUMNS) if order_by else None
        use_search = filters is not None or sorts is not None

        fetch_columns = self._get_fetch_columns(
            targets=targets,
            normalized_conditions=normalized_conditions,
            order_by=order_by,
            use_search=use_search,
        )

        result = self.list(
            conditions=conditions if not use_search else None,
            limit=result_limit,
            sort=order_by if not use_search else None,
            targets=fetch_columns,
            search_filters=filters,
            search_sorts=sorts,
            allow_search=use_search,
        )

        if use_search:
            logger.debug("Filters/sorts pushed to HubSpot API, skipping post-filter/sort")
            return self._apply_column_selection(result, original_targets)

        if normalized_conditions and not result.empty:
            result = self._apply_post_filter(result, normalized_conditions)

        if order_by and not result.empty:
            result = self._apply_post_sort(result, order_by)

        return self._apply_column_selection(result, original_targets)

    def _extract_query_params(self, query: ASTNode) -> Tuple[List, List, Optional[int]]:
        """Extract conditions, order_by, and limit from query AST."""
        conditions = extract_comparison_conditions(query.where) if query.where else []

        order_by = []
        if query.order_by:
            for col in query.order_by:
                ascending = True
                if hasattr(col, "direction") and col.direction:
                    ascending = col.direction.upper() != "DESC"
                elif hasattr(col, "ascending"):
                    ascending = col.ascending
                order_by.append(SortColumn(col.field.parts[-1], ascending))

        result_limit = query.limit.value if query.limit else None

        return conditions, order_by, result_limit

    def _get_targets(self, query: ASTNode) -> List[str]:
        """Extract target column names from query."""
        targets = []
        if query.targets:
            for target in query.targets:
                if isinstance(target, sql_ast.Star):
                    continue
                if isinstance(target, sql_ast.Identifier):
                    targets.append(to_internal_property(target.parts[-1]))
                    continue
                targets.extend(self._extract_target_columns(target))
        return list(dict.fromkeys(targets))

    @staticmethod
    def _extract_target_columns(target: ASTNode) -> List[str]:
        columns: List[str] = []

        def collect_identifiers(node, **kwargs):
            if isinstance(node, sql_ast.Identifier):
                columns.append(to_internal_property(node.parts[-1]))
            return None

        query_traversal(target, collect_identifiers)
        return columns

    def _apply_post_filter(self, df: pd.DataFrame, conditions: List[List[Any]]) -> pd.DataFrame:
        """Apply post-filtering using pandas operations instead of SQL rendering."""
        if df.empty:
            return df

        mask = pd.Series([True] * len(df), index=df.index)

        for condition in conditions:
            if len(condition) < 3:
                continue

            op, column, value = condition[0], condition[1], condition[2]
            op_key = canonical_op(op)

            if column not in df.columns:
                logger.warning(f"Column '{column}' not found in DataFrame for post-filtering")
                continue

            try:
                if op_key == "eq":
                    mask &= df[column] == value
                elif op_key == "neq":
                    mask &= df[column] != value
                elif op_key == "lt":
                    mask &= df[column] < value
                elif op_key == "lte":
                    mask &= df[column] <= value
                elif op_key == "gt":
                    mask &= df[column] > value
                elif op_key == "gte":
                    mask &= df[column] >= value
                elif op_key == "in":
                    values = value if isinstance(value, (list, tuple, set)) else [value]
                    mask &= df[column].isin(values)
                elif op_key == "not_in":
                    values = value if isinstance(value, (list, tuple, set)) else [value]
                    mask &= ~df[column].isin(values)
            except Exception as e:
                logger.warning(f"Error applying post-filter for {column}: {e}")
                continue

        return df[mask].reset_index(drop=True)

    def _apply_post_sort(self, df: pd.DataFrame, sort: List[SortColumn]) -> pd.DataFrame:
        sort_columns = []
        sort_ascending = []
        for sort_item in sort:
            column = to_internal_property(sort_item.column)
            if column not in df.columns:
                logger.warning(f"Column '{column}' not found in DataFrame for post-sorting")
                continue
            sort_columns.append(column)
            sort_ascending.append(sort_item.ascending)

        if not sort_columns:
            return df

        try:
            return df.sort_values(by=sort_columns, ascending=sort_ascending).reset_index(drop=True)
        except Exception as e:
            logger.warning(f"Error applying post-sort: {e}")
            return df

    def _apply_column_selection(self, df: pd.DataFrame, targets: List[str]) -> pd.DataFrame:
        """Apply column selection if specific columns requested."""
        if not targets or df.empty:
            return df

        existing_targets = [t for t in targets if t in df.columns]
        if existing_targets:
            return df[existing_targets]
        return df

    def _validate_query_columns(
        self,
        targets: List[str],
        normalized_conditions: List[List[Any]],
        order_by: List[SortColumn],
    ) -> None:
        requested = set()
        requested.update(targets or [])

        for condition in normalized_conditions:
            if len(condition) >= 2:
                requested.add(condition[1])

        for sort_item in order_by or []:
            requested.add(to_internal_property(sort_item.column))

        if not requested:
            return

        available = set(self.get_columns())
        missing = [col for col in requested if col not in available]
        if not missing:
            return

        missing_cols = ", ".join(missing)
        available_cols = ", ".join(sorted(available))
        raise ValueError(
            f"Column(s) {missing_cols} do not exist for this HubSpot table. Available columns: {available_cols}."
        )

    def _get_fetch_columns(
        self,
        targets: List[str],
        normalized_conditions: List[List[Any]],
        order_by: List[SortColumn],
        use_search: bool,
    ) -> List[str]:
        if targets:
            base_columns = list(targets)
        else:
            base_columns = list(self.get_columns())

        if use_search:
            return list(dict.fromkeys(base_columns))

        extra_columns = []
        for condition in normalized_conditions:
            if len(condition) >= 2:
                extra_columns.append(condition[1])
        for sort in order_by or []:
            extra_columns.append(to_internal_property(sort.column))

        return list(dict.fromkeys(base_columns + extra_columns))

    def _object_to_dict(self, obj: Any, columns: List[str]) -> Dict[str, Any]:
        properties = getattr(obj, "properties", {}) or {}
        row = {}
        for col in columns:
            if col == "id":
                row["id"] = getattr(obj, "id", None)
                continue
            row[col] = properties.get(to_hubspot_property(col))
        return row


class CompaniesTable(HubSpotAPIResource):
    """Hubspot Companies table."""

    SEARCHABLE_COLUMNS = {
        "name",
        "domain",
        "industry",
        "city",
        "state",
        "id",
        "website",
        "address",
        "zip",
        "numberofemployees",
        "annualrevenue",
        "lifecyclestage",
        "current_erp",
        "current_erp_version",
        "current_web_platform",
        "accounting_software",
        "credit_card_processor",
        "data_integration_platform",
        "marketing_platform",
        "pos_software",
        "shipping_software",
        "tax_platform",
        "partner",
        "partner_type",
        "partnership_status",
        "partner_payout_ytd",
        "partnership_commission",
        "total_customer_value",
        "total_revenue",
        "lastmodifieddate",
    }

    def meta_get_tables(self, table_name: str) -> Dict[str, Any]:
        row_count = None
        try:
            self.handler.connect()
            row_count = self.handler._estimate_table_rows("companies")
        except Exception as e:
            logger.warning(f"Could not estimate HubSpot companies row count: {e}")

        return {
            "TABLE_NAME": "companies",
            "TABLE_TYPE": "BASE TABLE",
            "TABLE_DESCRIPTION": self.handler._get_table_description("companies"),
            "ROW_COUNT": row_count,
        }

    def meta_get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        return self.handler._get_default_meta_columns("companies")

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[str] = None,
        search_filters: Optional[List[Dict[str, Any]]] = None,
        search_sorts: Optional[List[Dict[str, Any]]] = None,
        allow_search: bool = True,
    ) -> pd.DataFrame:
        companies_df = pd.json_normalize(
            self.get_companies(
                limit=limit,
                where_conditions=conditions,
                properties=targets,
                search_filters=search_filters,
                search_sorts=search_sorts,
                allow_search=allow_search,
            )
        )
        if companies_df.empty:
            companies_df = pd.DataFrame(columns=targets or self._get_default_company_columns())
        return companies_df

    def add(self, company_data: List[dict]):
        self.create_companies(company_data)

    def modify(self, conditions: List[FilterCondition], values: Dict) -> None:
        normalized_conditions = _normalize_filter_conditions(conditions)
        companies_df = pd.json_normalize(self.get_companies(limit=200, where_conditions=normalized_conditions))

        if companies_df.empty:
            raise ValueError("No companies retrieved from HubSpot to evaluate update conditions.")

        executor_conditions = _normalize_conditions_for_executor(normalized_conditions)
        update_query_executor = UPDATEQueryExecutor(companies_df, executor_conditions)
        filtered_df = update_query_executor.execute_query()

        if filtered_df.empty:
            raise ValueError(f"No companies found matching WHERE conditions: {conditions}.")

        company_ids = filtered_df["id"].astype(str).tolist()
        logger.info(f"Updating {len(company_ids)} compan(ies) matching WHERE conditions")
        self.update_companies(company_ids, values)

    def remove(self, conditions: List[FilterCondition]) -> None:
        normalized_conditions = _normalize_filter_conditions(conditions)
        companies_df = pd.json_normalize(self.get_companies(limit=200, where_conditions=normalized_conditions))

        if companies_df.empty:
            raise ValueError("No companies retrieved from HubSpot to evaluate delete conditions.")

        executor_conditions = _normalize_conditions_for_executor(normalized_conditions)
        delete_query_executor = DELETEQueryExecutor(companies_df, executor_conditions)
        filtered_df = delete_query_executor.execute_query()

        if filtered_df.empty:
            raise ValueError(f"No companies found matching WHERE conditions: {conditions}.")

        company_ids = filtered_df["id"].astype(str).tolist()
        logger.info(f"Deleting {len(company_ids)} compan(ies) matching WHERE conditions")
        self.delete_companies(company_ids)

    def get_columns(self) -> List[Text]:
        return self._get_default_company_columns()

    @staticmethod
    def _get_default_company_columns() -> List[str]:
        return [
            "id",
            "name",
            "city",
            "phone",
            "state",
            "domain",
            "industry",
            "website",
            "address",
            "zip",
            "numberofemployees",
            "annualrevenue",
            "lifecyclestage",
            "current_erp",
            "current_erp_version",
            "current_web_platform",
            "accounting_software",
            "credit_card_processor",
            "data_integration_platform",
            "marketing_platform",
            "pos_software",
            "shipping_software",
            "tax_platform",
            "partner",
            "partner_type",
            "partnership_status",
            "partner_payout_ytd",
            "partnership_commission",
            "total_customer_value",
            "total_revenue",
            "createdate",
            "lastmodifieddate",
        ]

    def get_companies(
        self,
        limit: Optional[int] = None,
        where_conditions: Optional[List] = None,
        properties: Optional[List[str]] = None,
        search_filters: Optional[List[Dict[str, Any]]] = None,
        search_sorts: Optional[List[Dict[str, Any]]] = None,
        allow_search: bool = True,
        **kwargs,
    ) -> List[Dict]:
        normalized_conditions = _normalize_filter_conditions(where_conditions)
        hubspot = self.handler.connect()

        requested_properties = properties or []
        default_properties = self._get_default_company_columns()
        columns = requested_properties or default_properties
        hubspot_properties = _build_hubspot_properties(columns)

        api_kwargs = {**kwargs, "properties": hubspot_properties}
        if limit is not None:
            api_kwargs["limit"] = limit
        else:
            api_kwargs.pop("limit", None)

        if allow_search and (search_filters or search_sorts or normalized_conditions):
            filters = search_filters
            if filters is None and normalized_conditions:
                filters = _build_hubspot_search_filters(normalized_conditions, self.SEARCHABLE_COLUMNS)
            if filters is not None or search_sorts is not None:
                search_results = self._search_companies_by_conditions(
                    hubspot, filters, hubspot_properties, limit, search_sorts, columns
                )
                logger.info(f"Retrieved {len(search_results)} companies from HubSpot via search API")
                return search_results

        companies = hubspot.crm.companies.get_all(**api_kwargs)
        companies_dict = []
        for company in companies:
            try:
                companies_dict.append(self._company_to_dict(company, columns))
            except Exception as e:
                logger.warning(f"Error processing company {getattr(company, 'id', 'unknown')}: {str(e)}")
                continue

        logger.info(f"Retrieved {len(companies_dict)} companies from HubSpot")
        return companies_dict

    def _search_companies_by_conditions(
        self,
        hubspot: HubSpot,
        filters: Optional[List[Dict[str, Any]]],
        properties: List[str],
        limit: Optional[int],
        sorts: Optional[List[Dict[str, Any]]],
        columns: List[str],
    ) -> List[Dict[str, Any]]:
        return _execute_hubspot_search(
            hubspot.crm.companies.search_api,
            filters or [],
            properties,
            limit,
            lambda obj: self._company_to_dict(obj, columns),
            sorts=sorts,
        )

    def _company_to_dict(self, company: Any, columns: Optional[List[str]] = None) -> Dict[str, Any]:
        columns = columns or self._get_default_company_columns()
        return self._object_to_dict(company, columns)

    def create_companies(self, companies_data: List[Dict[Text, Any]]) -> None:
        if not companies_data:
            raise ValueError("No company data provided for creation")

        logger.info(f"Attempting to create {len(companies_data)} compan(ies)")
        hubspot = self.handler.connect()
        companies_to_create = [HubSpotObjectInputCreate(properties=company) for company in companies_data]
        batch_input = BatchInputSimplePublicObjectBatchInputForCreate(inputs=companies_to_create)

        try:
            created_companies = hubspot.crm.companies.batch_api.create(
                batch_input_simple_public_object_batch_input_for_create=batch_input
            )
            if not created_companies or not hasattr(created_companies, "results") or not created_companies.results:
                raise Exception("Company creation returned no results")
            created_ids = [c.id for c in created_companies.results]
            logger.info(f"Successfully created {len(created_ids)} compan(ies) with IDs: {created_ids}")
        except Exception as e:
            logger.error(f"Companies creation failed: {str(e)}")
            raise Exception(f"Companies creation failed {e}")

    def update_companies(self, company_ids: List[Text], values_to_update: Dict[Text, Any]) -> None:
        hubspot = self.handler.connect()
        companies_to_update = [HubSpotObjectBatchInput(id=cid, properties=values_to_update) for cid in company_ids]
        batch_input = BatchInputSimplePublicObjectBatchInput(inputs=companies_to_update)
        try:
            updated = hubspot.crm.companies.batch_api.update(batch_input_simple_public_object_batch_input=batch_input)
            logger.info(f"Companies with ID {[c.id for c in updated.results]} updated")
        except Exception as e:
            raise Exception(f"Companies update failed {e}")

    def delete_companies(self, company_ids: List[Text]) -> None:
        hubspot = self.handler.connect()
        companies_to_delete = [HubSpotObjectId(id=cid) for cid in company_ids]
        batch_input = BatchInputSimplePublicObjectId(inputs=companies_to_delete)
        try:
            hubspot.crm.companies.batch_api.archive(batch_input_simple_public_object_id=batch_input)
            logger.info("Companies deleted")
        except Exception as e:
            raise Exception(f"Companies deletion failed {e}")


class ContactsTable(HubSpotAPIResource):
    """Hubspot Contacts table."""

    SEARCHABLE_COLUMNS = {
        "email",
        "id",
        "firstname",
        "lastname",
        "phone",
        "mobilephone",
        "jobtitle",
        "company",
        "city",
        "website",
        "lifecyclestage",
        "hs_lead_status",
        "hubspot_owner_id",
        "dc_contact",
        "current_ecommerce_platform",
        "departments",
        "demo__requested",
        "linkedin_url",
        "referral_name",
        "referral_company_name",
        "notes_last_contacted",
        "notes_last_updated",
        "notes_next_activity_date",
        "num_contacted_notes",
        "hs_sales_email_last_clicked",
        "hs_sales_email_last_opened",
        "lastmodifieddate",
    }

    def meta_get_tables(self, table_name: str) -> Dict[str, Any]:
        row_count = None
        try:
            self.handler.connect()
            row_count = self.handler._estimate_table_rows("contacts")
        except Exception as e:
            logger.warning(f"Could not estimate HubSpot contacts row count: {e}")

        return {
            "TABLE_NAME": "contacts",
            "TABLE_TYPE": "BASE TABLE",
            "TABLE_DESCRIPTION": self.handler._get_table_description("contacts"),
            "ROW_COUNT": row_count,
        }

    def meta_get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        return self.handler._get_default_meta_columns("contacts")

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[str] = None,
        search_filters: Optional[List[Dict[str, Any]]] = None,
        search_sorts: Optional[List[Dict[str, Any]]] = None,
        allow_search: bool = True,
    ) -> pd.DataFrame:
        contacts_df = pd.json_normalize(
            self.get_contacts(
                limit=limit,
                where_conditions=conditions,
                properties=targets,
                search_filters=search_filters,
                search_sorts=search_sorts,
                allow_search=allow_search,
            )
        )
        if contacts_df.empty:
            contacts_df = pd.DataFrame(columns=targets or self._get_default_contact_columns())
        else:
            if "id" in contacts_df.columns:
                contacts_df["id"] = pd.to_numeric(contacts_df["id"], errors="coerce")
        return contacts_df

    def add(self, contact_data: List[dict]):
        self.create_contacts(contact_data)

    def modify(self, conditions: List[FilterCondition], values: Dict) -> None:
        where_conditions = _normalize_filter_conditions(conditions)
        contacts_df = pd.json_normalize(self.get_contacts(limit=200, where_conditions=where_conditions))

        if contacts_df.empty:
            raise ValueError("No contacts retrieved from HubSpot to evaluate update conditions.")

        executor_conditions = _normalize_conditions_for_executor(where_conditions)
        update_query_executor = UPDATEQueryExecutor(contacts_df, executor_conditions)
        filtered_df = update_query_executor.execute_query()

        if filtered_df.empty:
            raise ValueError(f"No contacts found matching WHERE conditions: {conditions}.")

        contact_ids = filtered_df["id"].astype(str).tolist()
        logger.info(f"Updating {len(contact_ids)} contact(s) matching WHERE conditions")
        self.update_contacts(contact_ids, values)

    def remove(self, conditions: List[FilterCondition]) -> None:
        where_conditions = _normalize_filter_conditions(conditions)
        contacts_df = pd.json_normalize(self.get_contacts(limit=200, where_conditions=where_conditions))

        if contacts_df.empty:
            raise ValueError("No contacts retrieved from HubSpot to evaluate delete conditions.")

        executor_conditions = _normalize_conditions_for_executor(where_conditions)
        delete_query_executor = DELETEQueryExecutor(contacts_df, executor_conditions)
        filtered_df = delete_query_executor.execute_query()

        if filtered_df.empty:
            raise ValueError(f"No contacts found matching WHERE conditions: {conditions}.")

        contact_ids = filtered_df["id"].astype(str).tolist()
        logger.info(f"Deleting {len(contact_ids)} contact(s) matching WHERE conditions")
        self.delete_contacts(contact_ids)

    def get_columns(self) -> List[Text]:
        return self._get_default_contact_columns()

    @staticmethod
    def _get_default_contact_columns() -> List[str]:
        return [
            "id",
            "email",
            "firstname",
            "lastname",
            "phone",
            "mobilephone",
            "jobtitle",
            "company",
            "city",
            "website",
            "lifecyclestage",
            "hs_lead_status",
            "hubspot_owner_id",
            "dc_contact",
            "current_ecommerce_platform",
            "departments",
            "demo__requested",
            "linkedin_url",
            "referral_name",
            "referral_company_name",
            "notes_last_contacted",
            "notes_last_updated",
            "notes_next_activity_date",
            "num_contacted_notes",
            "hs_sales_email_last_clicked",
            "hs_sales_email_last_opened",
            "createdate",
            "lastmodifieddate",
        ]

    def get_contacts(
        self,
        limit: Optional[int] = None,
        where_conditions: Optional[List] = None,
        properties: Optional[List[str]] = None,
        search_filters: Optional[List[Dict[str, Any]]] = None,
        search_sorts: Optional[List[Dict[str, Any]]] = None,
        allow_search: bool = True,
        **kwargs,
    ) -> List[Dict]:
        normalized_conditions = _normalize_filter_conditions(where_conditions)
        hubspot = self.handler.connect()
        requested_properties = properties or []
        default_properties = self._get_default_contact_columns()
        columns = requested_properties or default_properties
        hubspot_properties = _build_hubspot_properties(columns)

        api_kwargs = {**kwargs, "properties": hubspot_properties}
        if limit is not None:
            api_kwargs["limit"] = limit
        else:
            api_kwargs.pop("limit", None)

        if allow_search and (search_filters or search_sorts or normalized_conditions):
            filters = search_filters
            if filters is None and normalized_conditions:
                filters = _build_hubspot_search_filters(normalized_conditions, self.SEARCHABLE_COLUMNS)
            if filters is not None or search_sorts is not None:
                search_results = self._search_contacts_by_conditions(
                    hubspot, filters, hubspot_properties, limit, search_sorts, columns
                )
                logger.info(f"Retrieved {len(search_results)} contacts from HubSpot via search API")
                return search_results

        contacts = hubspot.crm.contacts.get_all(**api_kwargs)
        contacts_dict = []
        try:
            for contact in contacts:
                contacts_dict.append(self._contact_to_dict(contact, columns))
                if limit is not None and len(contacts_dict) >= limit:
                    break
        except Exception as e:
            logger.error(f"Failed to iterate HubSpot contacts: {str(e)}")
            raise

        logger.info(f"Retrieved {len(contacts_dict)} contacts from HubSpot")
        return contacts_dict

    def _search_contacts_by_conditions(
        self,
        hubspot: HubSpot,
        filters: Optional[List[Dict[str, Any]]],
        properties: List[str],
        limit: Optional[int],
        sorts: Optional[List[Dict[str, Any]]],
        columns: List[str],
    ) -> List[Dict[str, Any]]:
        return _execute_hubspot_search(
            hubspot.crm.contacts.search_api,
            filters or [],
            properties,
            limit,
            lambda obj: self._contact_to_dict(obj, columns),
            sorts=sorts,
        )

    def _contact_to_dict(self, contact: Any, columns: Optional[List[str]] = None) -> Dict[str, Any]:
        columns = columns or self._get_default_contact_columns()
        try:
            return self._object_to_dict(contact, columns)
        except Exception as e:
            logger.warning(f"Error processing contact {getattr(contact, 'id', 'unknown')}: {str(e)}")
            return {
                "id": getattr(contact, "id", None),
                **{col: None for col in columns if col != "id"},
            }

    def create_contacts(self, contacts_data: List[Dict[Text, Any]]) -> None:
        if not contacts_data:
            raise ValueError("No contact data provided for creation")

        logger.info(f"Attempting to create {len(contacts_data)} contact(s)")
        hubspot = self.handler.connect()
        contacts_to_create = [HubSpotObjectInputCreate(properties=contact) for contact in contacts_data]
        batch_input = BatchInputSimplePublicObjectBatchInputForCreate(inputs=contacts_to_create)

        try:
            created_contacts = hubspot.crm.contacts.batch_api.create(
                batch_input_simple_public_object_batch_input_for_create=batch_input
            )
            if not created_contacts or not hasattr(created_contacts, "results") or not created_contacts.results:
                raise Exception("Contact creation returned no results")
            created_ids = [c.id for c in created_contacts.results]
            logger.info(f"Successfully created {len(created_ids)} contact(s) with IDs: {created_ids}")
        except Exception as e:
            logger.error(f"Contacts creation failed: {str(e)}")
            raise Exception(f"Contacts creation failed {e}")

    def update_contacts(self, contact_ids: List[Text], values_to_update: Dict[Text, Any]) -> None:
        hubspot = self.handler.connect()
        contacts_to_update = [HubSpotObjectBatchInput(id=cid, properties=values_to_update) for cid in contact_ids]
        batch_input = BatchInputSimplePublicObjectBatchInput(inputs=contacts_to_update)
        try:
            updated = hubspot.crm.contacts.batch_api.update(batch_input_simple_public_object_batch_input=batch_input)
            logger.info(f"Contacts with ID {[c.id for c in updated.results]} updated")
        except Exception as e:
            raise Exception(f"Contacts update failed {e}")

    def delete_contacts(self, contact_ids: List[Text]) -> None:
        hubspot = self.handler.connect()
        contacts_to_delete = [HubSpotObjectId(id=cid) for cid in contact_ids]
        batch_input = BatchInputSimplePublicObjectId(inputs=contacts_to_delete)
        try:
            hubspot.crm.contacts.batch_api.archive(batch_input_simple_public_object_id=batch_input)
            logger.info("Contacts deleted")
        except Exception as e:
            raise Exception(f"Contacts deletion failed {e}")


class DealsTable(HubSpotAPIResource):
    """Hubspot Deals table."""

    SEARCHABLE_COLUMNS = {
        "dealname",
        "amount",
        "dealstage",
        "pipeline",
        "closedate",
        "hubspot_owner_id",
        "closed_won_reason",
        "closed_lost_reason",
        "lead_attribution",
        "services_requested",
        "platform",
        "referral_partner",
        "referral_commission_amount",
        "tech_partners_involved",
        "sales_tier",
        "commission_status",
        "id",
        "lastmodifieddate",
    }

    def meta_get_tables(self, table_name: str) -> Dict[str, Any]:
        row_count = None
        try:
            self.handler.connect()
            row_count = self.handler._estimate_table_rows("deals")
        except Exception as e:
            logger.warning(f"Could not estimate HubSpot deals row count: {e}")

        return {
            "TABLE_NAME": "deals",
            "TABLE_TYPE": "BASE TABLE",
            "TABLE_DESCRIPTION": self.handler._get_table_description("deals"),
            "ROW_COUNT": row_count,
        }

    def meta_get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        return self.handler._get_default_meta_columns("deals")

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[str] = None,
        search_filters: Optional[List[Dict[str, Any]]] = None,
        search_sorts: Optional[List[Dict[str, Any]]] = None,
        allow_search: bool = True,
    ) -> pd.DataFrame:
        deals_df = pd.json_normalize(
            self.get_deals(
                limit=limit,
                where_conditions=conditions,
                properties=targets,
                search_filters=search_filters,
                search_sorts=search_sorts,
                allow_search=allow_search,
            )
        )
        if deals_df.empty:
            deals_df = pd.DataFrame(columns=targets or self._get_default_deal_columns())
        else:
            deals_df = self._cast_deal_columns(deals_df)
        return deals_df

    def add(self, deal_data: List[dict]):
        self.create_deals(deal_data)

    def modify(self, conditions: List[FilterCondition], values: Dict) -> None:
        where_conditions = _normalize_filter_conditions(conditions)
        deals_df = pd.json_normalize(self.get_deals(limit=200, where_conditions=where_conditions))

        if deals_df.empty:
            raise ValueError("No deals retrieved from HubSpot to evaluate update conditions.")

        executor_conditions = _normalize_conditions_for_executor(where_conditions)
        update_query_executor = UPDATEQueryExecutor(deals_df, executor_conditions)
        filtered_df = update_query_executor.execute_query()

        if filtered_df.empty:
            raise ValueError(f"No deals found matching WHERE conditions: {conditions}.")

        deal_ids = filtered_df["id"].astype(str).tolist()
        logger.info(f"Updating {len(deal_ids)} deal(s) matching WHERE conditions")
        self.update_deals(deal_ids, values)

    def remove(self, conditions: List[FilterCondition]) -> None:
        where_conditions = _normalize_filter_conditions(conditions)
        deals_df = pd.json_normalize(self.get_deals(limit=200, where_conditions=where_conditions))

        if deals_df.empty:
            raise ValueError("No deals retrieved from HubSpot to evaluate delete conditions.")

        executor_conditions = _normalize_conditions_for_executor(where_conditions)
        delete_query_executor = DELETEQueryExecutor(deals_df, executor_conditions)
        filtered_df = delete_query_executor.execute_query()

        if filtered_df.empty:
            raise ValueError(f"No deals found matching WHERE conditions: {conditions}.")

        deal_ids = filtered_df["id"].astype(str).tolist()
        logger.info(f"Deleting {len(deal_ids)} deal(s) matching WHERE conditions")
        self.delete_deals(deal_ids)

    def get_columns(self) -> List[Text]:
        return self._get_default_deal_columns()

    @staticmethod
    def _get_default_deal_columns() -> List[str]:
        return [
            "id",
            "dealname",
            "amount",
            "pipeline",
            "closedate",
            "dealstage",
            "hubspot_owner_id",
            "closed_won_reason",
            "closed_lost_reason",
            "lead_attribution",
            "services_requested",
            "platform",
            "referral_partner",
            "referral_commission_amount",
            "tech_partners_involved",
            "sales_tier",
            "commission_status",
            "createdate",
            "lastmodifieddate",
        ]

    @staticmethod
    def _cast_deal_columns(deals_df: pd.DataFrame) -> pd.DataFrame:
        numeric_columns = ["amount"]
        datetime_columns = ["closedate", "createdate", "lastmodifieddate"]
        for column in numeric_columns:
            if column in deals_df.columns:
                deals_df[column] = pd.to_numeric(deals_df[column], errors="coerce")
        for column in datetime_columns:
            if column in deals_df.columns:
                deals_df[column] = pd.to_datetime(deals_df[column], errors="coerce")
        return deals_df

    def get_deals(
        self,
        limit: Optional[int] = None,
        where_conditions: Optional[List] = None,
        properties: Optional[List[str]] = None,
        search_filters: Optional[List[Dict[str, Any]]] = None,
        search_sorts: Optional[List[Dict[str, Any]]] = None,
        allow_search: bool = True,
        **kwargs,
    ) -> List[Dict]:
        normalized_conditions = _normalize_filter_conditions(where_conditions)
        hubspot = self.handler.connect()
        requested_properties = properties or []
        default_properties = self._get_default_deal_columns()
        columns = requested_properties or default_properties
        hubspot_properties = _build_hubspot_properties(columns)

        api_kwargs = {**kwargs, "properties": hubspot_properties}
        if limit is not None:
            api_kwargs["limit"] = limit
        else:
            api_kwargs.pop("limit", None)

        if allow_search and (search_filters or search_sorts or normalized_conditions):
            filters = search_filters
            if filters is None and normalized_conditions:
                filters = _build_hubspot_search_filters(normalized_conditions, self.SEARCHABLE_COLUMNS)
            if filters is not None or search_sorts is not None:
                search_results = self._search_deals_by_conditions(
                    hubspot, filters, hubspot_properties, limit, search_sorts, columns
                )
                logger.info(f"Retrieved {len(search_results)} deals from HubSpot via search API")
                return search_results

        deals = hubspot.crm.deals.get_all(**api_kwargs)
        deals_dict = []
        for deal in deals:
            try:
                deals_dict.append(self._deal_to_dict(deal, columns))
            except Exception as e:
                logger.error(f"Error processing deal {getattr(deal, 'id', 'unknown')}: {str(e)}")
                raise ValueError(f"Failed to process deal {getattr(deal, 'id', 'unknown')}.") from e

        logger.info(f"Retrieved {len(deals_dict)} deals from HubSpot")
        return deals_dict

    def _search_deals_by_conditions(
        self,
        hubspot: HubSpot,
        filters: Optional[List[Dict[str, Any]]],
        properties: List[str],
        limit: Optional[int],
        sorts: Optional[List[Dict[str, Any]]],
        columns: List[str],
    ) -> List[Dict[str, Any]]:
        return _execute_hubspot_search(
            hubspot.crm.deals.search_api,
            filters or [],
            properties,
            limit,
            lambda obj: self._deal_to_dict(obj, columns),
            sorts=sorts,
        )

    def _deal_to_dict(self, deal: Any, columns: Optional[List[str]] = None) -> Dict[str, Any]:
        columns = columns or self._get_default_deal_columns()
        return self._object_to_dict(deal, columns)

    def create_deals(self, deals_data: List[Dict[Text, Any]]) -> None:
        if not deals_data:
            raise ValueError("No deal data provided for creation")

        logger.info(f"Attempting to create {len(deals_data)} deal(s)")
        hubspot = self.handler.connect()
        deals_to_create = [HubSpotObjectInputCreate(properties=deal) for deal in deals_data]
        batch_input = BatchInputSimplePublicObjectBatchInputForCreate(inputs=deals_to_create)

        try:
            created_deals = hubspot.crm.deals.batch_api.create(
                batch_input_simple_public_object_batch_input_for_create=batch_input
            )
            if not created_deals or not hasattr(created_deals, "results") or not created_deals.results:
                raise Exception("Deal creation returned no results")
            created_ids = [d.id for d in created_deals.results]
            logger.info(f"Successfully created {len(created_ids)} deal(s) with IDs: {created_ids}")
        except Exception as e:
            logger.error(f"Deals creation failed: {str(e)}")
            raise Exception(f"Deals creation failed {e}")

    def update_deals(self, deal_ids: List[Text], values_to_update: Dict[Text, Any]) -> None:
        hubspot = self.handler.connect()
        deals_to_update = [HubSpotObjectBatchInput(id=did, properties=values_to_update) for did in deal_ids]
        batch_input = BatchInputSimplePublicObjectBatchInput(inputs=deals_to_update)
        try:
            updated = hubspot.crm.deals.batch_api.update(batch_input_simple_public_object_batch_input=batch_input)
            logger.info(f"Deals with ID {[d.id for d in updated.results]} updated")
        except Exception as e:
            raise Exception(f"Deals update failed {e}")

    def delete_deals(self, deal_ids: List[Text]) -> None:
        hubspot = self.handler.connect()
        deals_to_delete = [HubSpotObjectId(id=did) for did in deal_ids]
        batch_input = BatchInputSimplePublicObjectId(inputs=deals_to_delete)
        try:
            hubspot.crm.deals.batch_api.archive(batch_input_simple_public_object_id=batch_input)
            logger.info("Deals deleted")
        except Exception as e:
            raise Exception(f"Deals deletion failed {e}")


class TicketsTable(HubSpotAPIResource):
    """HubSpot Tickets table for support ticket management."""

    SEARCHABLE_COLUMNS = {"subject", "hs_pipeline", "hs_pipeline_stage", "hs_ticket_priority", "id"}

    def meta_get_tables(self, table_name: str) -> Dict[str, Any]:
        row_count = None
        try:
            self.handler.connect()
            row_count = self.handler._estimate_table_rows("tickets")
        except Exception as e:
            logger.warning(f"Could not estimate HubSpot tickets row count: {e}")

        return {
            "TABLE_NAME": "tickets",
            "TABLE_TYPE": "BASE TABLE",
            "TABLE_DESCRIPTION": "HubSpot tickets data including subject, status, priority and pipeline information",
            "ROW_COUNT": row_count,
        }

    def meta_get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        return self.handler._get_default_meta_columns("tickets")

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[str] = None,
        search_filters: Optional[List[Dict[str, Any]]] = None,
        search_sorts: Optional[List[Dict[str, Any]]] = None,
        allow_search: bool = True,
    ) -> pd.DataFrame:
        tickets_df = pd.json_normalize(
            self.get_tickets(
                limit=limit,
                where_conditions=conditions,
                properties=targets,
                search_filters=search_filters,
                search_sorts=search_sorts,
                allow_search=allow_search,
            )
        )
        if tickets_df.empty:
            tickets_df = pd.DataFrame(columns=targets or self._get_default_ticket_columns())
        return tickets_df

    def add(self, ticket_data: List[dict]):
        self.create_tickets(ticket_data)

    def modify(self, conditions: List[FilterCondition], values: Dict) -> None:
        normalized_conditions = _normalize_filter_conditions(conditions)
        tickets_df = pd.json_normalize(self.get_tickets(limit=200, where_conditions=normalized_conditions))

        if tickets_df.empty:
            raise ValueError("No tickets retrieved from HubSpot to evaluate update conditions.")

        executor_conditions = _normalize_conditions_for_executor(normalized_conditions)
        update_query_executor = UPDATEQueryExecutor(tickets_df, executor_conditions)
        filtered_df = update_query_executor.execute_query()

        if filtered_df.empty:
            raise ValueError(f"No tickets found matching WHERE conditions: {conditions}.")

        ticket_ids = filtered_df["id"].astype(str).tolist()
        logger.info(f"Updating {len(ticket_ids)} ticket(s) matching WHERE conditions")
        self.update_tickets(ticket_ids, values)

    def remove(self, conditions: List[FilterCondition]) -> None:
        normalized_conditions = _normalize_filter_conditions(conditions)
        tickets_df = pd.json_normalize(self.get_tickets(limit=200, where_conditions=normalized_conditions))

        if tickets_df.empty:
            raise ValueError("No tickets retrieved from HubSpot to evaluate delete conditions.")

        executor_conditions = _normalize_conditions_for_executor(normalized_conditions)
        delete_query_executor = DELETEQueryExecutor(tickets_df, executor_conditions)
        filtered_df = delete_query_executor.execute_query()

        if filtered_df.empty:
            raise ValueError(f"No tickets found matching WHERE conditions: {conditions}.")

        ticket_ids = filtered_df["id"].astype(str).tolist()
        logger.info(f"Deleting {len(ticket_ids)} ticket(s) matching WHERE conditions")
        self.delete_tickets(ticket_ids)

    def get_columns(self) -> List[Text]:
        return self._get_default_ticket_columns()

    @staticmethod
    def _get_default_ticket_columns() -> List[str]:
        return [
            "id",
            "subject",
            "content",
            "hs_pipeline",
            "hs_pipeline_stage",
            "hs_ticket_priority",
            "hs_ticket_category",
            "hubspot_owner_id",
            "createdate",
            "lastmodifieddate",
        ]

    def get_tickets(
        self,
        limit: Optional[int] = None,
        where_conditions: Optional[List] = None,
        properties: Optional[List[str]] = None,
        search_filters: Optional[List[Dict[str, Any]]] = None,
        search_sorts: Optional[List[Dict[str, Any]]] = None,
        allow_search: bool = True,
        **kwargs,
    ) -> List[Dict]:
        normalized_conditions = _normalize_filter_conditions(where_conditions)
        hubspot = self.handler.connect()

        requested_properties = properties or []
        default_properties = self._get_default_ticket_columns()
        columns = requested_properties or default_properties
        hubspot_properties = _build_hubspot_properties(columns)

        api_kwargs = {**kwargs, "properties": hubspot_properties}
        if limit is not None:
            api_kwargs["limit"] = limit
        else:
            api_kwargs.pop("limit", None)

        if allow_search and (search_filters or search_sorts or normalized_conditions):
            filters = search_filters
            if filters is None and normalized_conditions:
                filters = _build_hubspot_search_filters(normalized_conditions, self.SEARCHABLE_COLUMNS)
            if filters is not None or search_sorts is not None:
                search_results = self._search_tickets_by_conditions(
                    hubspot, filters, hubspot_properties, limit, search_sorts, columns
                )
                logger.info(f"Retrieved {len(search_results)} tickets from HubSpot via search API")
                return search_results

        tickets = hubspot.crm.tickets.get_all(**api_kwargs)
        tickets_dict = []
        for ticket in tickets:
            try:
                tickets_dict.append(self._ticket_to_dict(ticket, columns))
            except Exception as e:
                logger.warning(f"Error processing ticket {getattr(ticket, 'id', 'unknown')}: {str(e)}")
                continue

        logger.info(f"Retrieved {len(tickets_dict)} tickets from HubSpot")
        return tickets_dict

    def _search_tickets_by_conditions(
        self,
        hubspot: HubSpot,
        filters: Optional[List[Dict[str, Any]]],
        properties: List[str],
        limit: Optional[int],
        sorts: Optional[List[Dict[str, Any]]],
        columns: List[str],
    ) -> List[Dict[str, Any]]:
        return _execute_hubspot_search(
            hubspot.crm.tickets.search_api,
            filters or [],
            properties,
            limit,
            lambda obj: self._ticket_to_dict(obj, columns),
            sorts=sorts,
        )

    def _ticket_to_dict(self, ticket: Any, columns: Optional[List[str]] = None) -> Dict[str, Any]:
        columns = columns or self._get_default_ticket_columns()
        return self._object_to_dict(ticket, columns)

    def create_tickets(self, tickets_data: List[Dict[Text, Any]]) -> None:
        if not tickets_data:
            raise ValueError("No ticket data provided for creation")

        logger.info(f"Attempting to create {len(tickets_data)} ticket(s)")
        hubspot = self.handler.connect()
        tickets_to_create = [HubSpotObjectInputCreate(properties=ticket) for ticket in tickets_data]
        batch_input = BatchInputSimplePublicObjectBatchInputForCreate(inputs=tickets_to_create)

        try:
            created_tickets = hubspot.crm.tickets.batch_api.create(
                batch_input_simple_public_object_batch_input_for_create=batch_input
            )
            if not created_tickets or not hasattr(created_tickets, "results") or not created_tickets.results:
                raise Exception("Ticket creation returned no results")
            created_ids = [t.id for t in created_tickets.results]
            logger.info(f"Successfully created {len(created_ids)} ticket(s) with IDs: {created_ids}")
        except Exception as e:
            logger.error(f"Tickets creation failed: {str(e)}")
            raise Exception(f"Tickets creation failed {e}")

    def update_tickets(self, ticket_ids: List[Text], values_to_update: Dict[Text, Any]) -> None:
        hubspot = self.handler.connect()
        tickets_to_update = [HubSpotObjectBatchInput(id=tid, properties=values_to_update) for tid in ticket_ids]
        batch_input = BatchInputSimplePublicObjectBatchInput(inputs=tickets_to_update)
        try:
            updated = hubspot.crm.tickets.batch_api.update(batch_input_simple_public_object_batch_input=batch_input)
            logger.info(f"Tickets with ID {[t.id for t in updated.results]} updated")
        except Exception as e:
            raise Exception(f"Tickets update failed {e}")

    def delete_tickets(self, ticket_ids: List[Text]) -> None:
        hubspot = self.handler.connect()
        tickets_to_delete = [HubSpotObjectId(id=tid) for tid in ticket_ids]
        batch_input = BatchInputSimplePublicObjectId(inputs=tickets_to_delete)
        try:
            hubspot.crm.tickets.batch_api.archive(batch_input_simple_public_object_id=batch_input)
            logger.info("Tickets deleted")
        except Exception as e:
            raise Exception(f"Tickets deletion failed {e}")


class TasksTable(HubSpotAPIResource):
    """HubSpot Tasks table for task management and follow-ups."""

    SEARCHABLE_COLUMNS = {"hs_task_subject", "hs_task_status", "hs_task_priority", "hs_task_type", "id"}

    def meta_get_tables(self, table_name: str) -> Dict[str, Any]:
        row_count = None
        try:
            self.handler.connect()
            row_count = self.handler._estimate_table_rows("tasks")
        except Exception as e:
            logger.warning(f"Could not estimate HubSpot tasks row count: {e}")

        return {
            "TABLE_NAME": "tasks",
            "TABLE_TYPE": "BASE TABLE",
            "TABLE_DESCRIPTION": "HubSpot tasks data including subject, status, priority and due dates",
            "ROW_COUNT": row_count,
        }

    def meta_get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        return self.handler._get_default_meta_columns("tasks")

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[str] = None,
        search_filters: Optional[List[Dict[str, Any]]] = None,
        search_sorts: Optional[List[Dict[str, Any]]] = None,
        allow_search: bool = True,
    ) -> pd.DataFrame:
        tasks_df = pd.json_normalize(
            self.get_tasks(
                limit=limit,
                where_conditions=conditions,
                properties=targets,
                search_filters=search_filters,
                search_sorts=search_sorts,
                allow_search=allow_search,
            )
        )
        if tasks_df.empty:
            tasks_df = pd.DataFrame(columns=targets or self._get_default_task_columns())
        return tasks_df

    def add(self, task_data: List[dict]):
        self.create_tasks(task_data)

    def modify(self, conditions: List[FilterCondition], values: Dict) -> None:
        normalized_conditions = _normalize_filter_conditions(conditions)
        tasks_df = pd.json_normalize(self.get_tasks(limit=200, where_conditions=normalized_conditions))

        if tasks_df.empty:
            raise ValueError("No tasks retrieved from HubSpot to evaluate update conditions.")

        executor_conditions = _normalize_conditions_for_executor(normalized_conditions)
        update_query_executor = UPDATEQueryExecutor(tasks_df, executor_conditions)
        filtered_df = update_query_executor.execute_query()

        if filtered_df.empty:
            raise ValueError(f"No tasks found matching WHERE conditions: {conditions}.")

        task_ids = filtered_df["id"].astype(str).tolist()
        logger.info(f"Updating {len(task_ids)} task(s) matching WHERE conditions")
        self.update_tasks(task_ids, values)

    def remove(self, conditions: List[FilterCondition]) -> None:
        normalized_conditions = _normalize_filter_conditions(conditions)
        tasks_df = pd.json_normalize(self.get_tasks(limit=200, where_conditions=normalized_conditions))

        if tasks_df.empty:
            raise ValueError("No tasks retrieved from HubSpot to evaluate delete conditions.")

        executor_conditions = _normalize_conditions_for_executor(normalized_conditions)
        delete_query_executor = DELETEQueryExecutor(tasks_df, executor_conditions)
        filtered_df = delete_query_executor.execute_query()

        if filtered_df.empty:
            raise ValueError(f"No tasks found matching WHERE conditions: {conditions}.")

        task_ids = filtered_df["id"].astype(str).tolist()
        logger.info(f"Deleting {len(task_ids)} task(s) matching WHERE conditions")
        self.delete_tasks(task_ids)

    def get_columns(self) -> List[Text]:
        return self._get_default_task_columns()

    @staticmethod
    def _get_default_task_columns() -> List[str]:
        return [
            "id",
            "hs_task_subject",
            "hs_task_body",
            "hs_task_status",
            "hs_task_priority",
            "hs_task_type",
            "hs_timestamp",
            "hubspot_owner_id",
            "createdate",
            "lastmodifieddate",
        ]

    def get_tasks(
        self,
        limit: Optional[int] = None,
        where_conditions: Optional[List] = None,
        properties: Optional[List[str]] = None,
        search_filters: Optional[List[Dict[str, Any]]] = None,
        search_sorts: Optional[List[Dict[str, Any]]] = None,
        allow_search: bool = True,
        **kwargs,
    ) -> List[Dict]:
        normalized_conditions = _normalize_filter_conditions(where_conditions)
        hubspot = self.handler.connect()

        requested_properties = properties or []
        default_properties = self._get_default_task_columns()
        columns = requested_properties or default_properties
        hubspot_properties = _build_hubspot_properties(columns)

        api_kwargs = {**kwargs, "properties": hubspot_properties}
        if limit is not None:
            api_kwargs["limit"] = limit
        else:
            api_kwargs.pop("limit", None)

        # Tasks use the objects API
        if allow_search and (search_filters or search_sorts or normalized_conditions):
            filters = search_filters
            if filters is None and normalized_conditions:
                filters = _build_hubspot_search_filters(normalized_conditions, self.SEARCHABLE_COLUMNS)
            if filters is not None or search_sorts is not None:
                search_results = self._search_tasks_by_conditions(
                    hubspot, filters, hubspot_properties, limit, search_sorts, columns
                )
                logger.info(f"Retrieved {len(search_results)} tasks from HubSpot via search API")
                return search_results

        tasks = self.handler._get_objects_all("tasks", **api_kwargs)
        tasks_dict = []
        for task in tasks:
            try:
                tasks_dict.append(self._task_to_dict(task, columns))
            except Exception as e:
                logger.warning(f"Error processing task {getattr(task, 'id', 'unknown')}: {str(e)}")
                continue

        logger.info(f"Retrieved {len(tasks_dict)} tasks from HubSpot")
        return tasks_dict

    def _search_tasks_by_conditions(
        self,
        hubspot: HubSpot,
        filters: Optional[List[Dict[str, Any]]],
        properties: List[str],
        limit: Optional[int],
        sorts: Optional[List[Dict[str, Any]]],
        columns: List[str],
    ) -> List[Dict[str, Any]]:
        return _execute_hubspot_search(
            hubspot.crm.objects.search_api,
            filters or [],
            properties,
            limit,
            lambda obj: self._task_to_dict(obj, columns),
            sorts=sorts,
            object_type="tasks",
        )

    def _task_to_dict(self, task: Any, columns: Optional[List[str]] = None) -> Dict[str, Any]:
        columns = columns or self._get_default_task_columns()
        return self._object_to_dict(task, columns)

    def create_tasks(self, tasks_data: List[Dict[Text, Any]]) -> None:
        if not tasks_data:
            raise ValueError("No task data provided for creation")

        logger.info(f"Attempting to create {len(tasks_data)} task(s)")
        hubspot = self.handler.connect()
        tasks_to_create = [HubSpotObjectInputCreate(properties=task) for task in tasks_data]
        batch_input = BatchInputSimplePublicObjectBatchInputForCreate(inputs=tasks_to_create)

        try:
            created_tasks = hubspot.crm.objects.tasks.batch_api.create(
                batch_input_simple_public_object_batch_input_for_create=batch_input
            )
            if not created_tasks or not hasattr(created_tasks, "results") or not created_tasks.results:
                raise Exception("Task creation returned no results")
            created_ids = [t.id for t in created_tasks.results]
            logger.info(f"Successfully created {len(created_ids)} task(s) with IDs: {created_ids}")
        except Exception as e:
            logger.error(f"Tasks creation failed: {str(e)}")
            raise Exception(f"Tasks creation failed {e}")

    def update_tasks(self, task_ids: List[Text], values_to_update: Dict[Text, Any]) -> None:
        hubspot = self.handler.connect()
        tasks_to_update = [HubSpotObjectBatchInput(id=tid, properties=values_to_update) for tid in task_ids]
        batch_input = BatchInputSimplePublicObjectBatchInput(inputs=tasks_to_update)
        try:
            updated = hubspot.crm.objects.tasks.batch_api.update(
                batch_input_simple_public_object_batch_input=batch_input
            )
            logger.info(f"Tasks with ID {[t.id for t in updated.results]} updated")
        except Exception as e:
            raise Exception(f"Tasks update failed {e}")

    def delete_tasks(self, task_ids: List[Text]) -> None:
        hubspot = self.handler.connect()
        tasks_to_delete = [HubSpotObjectId(id=tid) for tid in task_ids]
        batch_input = BatchInputSimplePublicObjectId(inputs=tasks_to_delete)
        try:
            hubspot.crm.objects.tasks.batch_api.archive(batch_input_simple_public_object_id=batch_input)
            logger.info("Tasks deleted")
        except Exception as e:
            raise Exception(f"Tasks deletion failed {e}")


class CallsTable(HubSpotAPIResource):
    """HubSpot Calls table for phone/video call logs."""

    SEARCHABLE_COLUMNS = {"hs_call_title", "hs_call_direction", "hs_call_disposition", "hs_call_status", "id"}

    def meta_get_tables(self, table_name: str) -> Dict[str, Any]:
        row_count = None
        try:
            self.handler.connect()
            row_count = self.handler._estimate_table_rows("calls")
        except Exception as e:
            logger.warning(f"Could not estimate HubSpot calls row count: {e}")

        return {
            "TABLE_NAME": "calls",
            "TABLE_TYPE": "BASE TABLE",
            "TABLE_DESCRIPTION": "HubSpot call logs including direction, duration, outcome and notes",
            "ROW_COUNT": row_count,
        }

    def meta_get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        return self.handler._get_default_meta_columns("calls")

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[str] = None,
        search_filters: Optional[List[Dict[str, Any]]] = None,
        search_sorts: Optional[List[Dict[str, Any]]] = None,
        allow_search: bool = True,
    ) -> pd.DataFrame:
        calls_df = pd.json_normalize(
            self.get_calls(
                limit=limit,
                where_conditions=conditions,
                properties=targets,
                search_filters=search_filters,
                search_sorts=search_sorts,
                allow_search=allow_search,
            )
        )
        if calls_df.empty:
            calls_df = pd.DataFrame(columns=targets or self._get_default_call_columns())
        return calls_df

    def add(self, call_data: List[dict]):
        self.create_calls(call_data)

    def modify(self, conditions: List[FilterCondition], values: Dict) -> None:
        normalized_conditions = _normalize_filter_conditions(conditions)
        calls_df = pd.json_normalize(self.get_calls(limit=200, where_conditions=normalized_conditions))

        if calls_df.empty:
            raise ValueError("No calls retrieved from HubSpot to evaluate update conditions.")

        executor_conditions = _normalize_conditions_for_executor(normalized_conditions)
        update_query_executor = UPDATEQueryExecutor(calls_df, executor_conditions)
        filtered_df = update_query_executor.execute_query()

        if filtered_df.empty:
            raise ValueError(f"No calls found matching WHERE conditions: {conditions}.")

        call_ids = filtered_df["id"].astype(str).tolist()
        logger.info(f"Updating {len(call_ids)} call(s) matching WHERE conditions")
        self.update_calls(call_ids, values)

    def remove(self, conditions: List[FilterCondition]) -> None:
        normalized_conditions = _normalize_filter_conditions(conditions)
        calls_df = pd.json_normalize(self.get_calls(limit=200, where_conditions=normalized_conditions))

        if calls_df.empty:
            raise ValueError("No calls retrieved from HubSpot to evaluate delete conditions.")

        executor_conditions = _normalize_conditions_for_executor(normalized_conditions)
        delete_query_executor = DELETEQueryExecutor(calls_df, executor_conditions)
        filtered_df = delete_query_executor.execute_query()

        if filtered_df.empty:
            raise ValueError(f"No calls found matching WHERE conditions: {conditions}.")

        call_ids = filtered_df["id"].astype(str).tolist()
        logger.info(f"Deleting {len(call_ids)} call(s) matching WHERE conditions")
        self.delete_calls(call_ids)

    def get_columns(self) -> List[Text]:
        return self._get_default_call_columns()

    @staticmethod
    def _get_default_call_columns() -> List[str]:
        return [
            "id",
            "hs_call_title",
            "hs_call_body",
            "hs_call_direction",
            "hs_call_disposition",
            "hs_call_duration",
            "hs_call_status",
            "hubspot_owner_id",
            "hs_timestamp",
            "createdate",
            "lastmodifieddate",
        ]

    def get_calls(
        self,
        limit: Optional[int] = None,
        where_conditions: Optional[List] = None,
        properties: Optional[List[str]] = None,
        search_filters: Optional[List[Dict[str, Any]]] = None,
        search_sorts: Optional[List[Dict[str, Any]]] = None,
        allow_search: bool = True,
        **kwargs,
    ) -> List[Dict]:
        normalized_conditions = _normalize_filter_conditions(where_conditions)
        hubspot = self.handler.connect()

        requested_properties = properties or []
        default_properties = self._get_default_call_columns()
        columns = requested_properties or default_properties
        hubspot_properties = _build_hubspot_properties(columns)

        api_kwargs = {**kwargs, "properties": hubspot_properties}
        if limit is not None:
            api_kwargs["limit"] = limit
        else:
            api_kwargs.pop("limit", None)

        if allow_search and (search_filters or search_sorts or normalized_conditions):
            filters = search_filters
            if filters is None and normalized_conditions:
                filters = _build_hubspot_search_filters(normalized_conditions, self.SEARCHABLE_COLUMNS)
            if filters is not None or search_sorts is not None:
                search_results = self._search_calls_by_conditions(
                    hubspot, filters, hubspot_properties, limit, search_sorts, columns
                )
                logger.info(f"Retrieved {len(search_results)} calls from HubSpot via search API")
                return search_results

        calls = self.handler._get_objects_all("calls", **api_kwargs)
        calls_dict = []
        for call in calls:
            try:
                calls_dict.append(self._call_to_dict(call, columns))
            except Exception as e:
                logger.warning(f"Error processing call {getattr(call, 'id', 'unknown')}: {str(e)}")
                continue

        logger.info(f"Retrieved {len(calls_dict)} calls from HubSpot")
        return calls_dict

    def _search_calls_by_conditions(
        self,
        hubspot: HubSpot,
        filters: Optional[List[Dict[str, Any]]],
        properties: List[str],
        limit: Optional[int],
        sorts: Optional[List[Dict[str, Any]]],
        columns: List[str],
    ) -> List[Dict[str, Any]]:
        return _execute_hubspot_search(
            hubspot.crm.objects.search_api,
            filters or [],
            properties,
            limit,
            lambda obj: self._call_to_dict(obj, columns),
            sorts=sorts,
            object_type="calls",
        )

    def _call_to_dict(self, call: Any, columns: Optional[List[str]] = None) -> Dict[str, Any]:
        columns = columns or self._get_default_call_columns()
        return self._object_to_dict(call, columns)

    def create_calls(self, calls_data: List[Dict[Text, Any]]) -> None:
        if not calls_data:
            raise ValueError("No call data provided for creation")

        logger.info(f"Attempting to create {len(calls_data)} call(s)")
        hubspot = self.handler.connect()
        calls_to_create = [HubSpotObjectInputCreate(properties=call) for call in calls_data]
        batch_input = BatchInputSimplePublicObjectBatchInputForCreate(inputs=calls_to_create)

        try:
            created_calls = hubspot.crm.objects.calls.batch_api.create(
                batch_input_simple_public_object_batch_input_for_create=batch_input
            )
            if not created_calls or not hasattr(created_calls, "results") or not created_calls.results:
                raise Exception("Call creation returned no results")
            created_ids = [c.id for c in created_calls.results]
            logger.info(f"Successfully created {len(created_ids)} call(s) with IDs: {created_ids}")
        except Exception as e:
            logger.error(f"Calls creation failed: {str(e)}")
            raise Exception(f"Calls creation failed {e}")

    def update_calls(self, call_ids: List[Text], values_to_update: Dict[Text, Any]) -> None:
        hubspot = self.handler.connect()
        calls_to_update = [HubSpotObjectBatchInput(id=cid, properties=values_to_update) for cid in call_ids]
        batch_input = BatchInputSimplePublicObjectBatchInput(inputs=calls_to_update)
        try:
            updated = hubspot.crm.objects.calls.batch_api.update(
                batch_input_simple_public_object_batch_input=batch_input
            )
            logger.info(f"Calls with ID {[c.id for c in updated.results]} updated")
        except Exception as e:
            raise Exception(f"Calls update failed {e}")

    def delete_calls(self, call_ids: List[Text]) -> None:
        hubspot = self.handler.connect()
        calls_to_delete = [HubSpotObjectId(id=cid) for cid in call_ids]
        batch_input = BatchInputSimplePublicObjectId(inputs=calls_to_delete)
        try:
            hubspot.crm.objects.calls.batch_api.archive(batch_input_simple_public_object_id=batch_input)
            logger.info("Calls deleted")
        except Exception as e:
            raise Exception(f"Calls deletion failed {e}")


class EmailsTable(HubSpotAPIResource):
    """HubSpot Emails table for email engagement logs."""

    SEARCHABLE_COLUMNS = {"hs_email_subject", "hs_email_direction", "hs_email_status", "id"}

    def meta_get_tables(self, table_name: str) -> Dict[str, Any]:
        row_count = None
        try:
            self.handler.connect()
            row_count = self.handler._estimate_table_rows("emails")
        except Exception as e:
            logger.warning(f"Could not estimate HubSpot emails row count: {e}")

        return {
            "TABLE_NAME": "emails",
            "TABLE_TYPE": "BASE TABLE",
            "TABLE_DESCRIPTION": "HubSpot email logs including subject, direction, status and content",
            "ROW_COUNT": row_count,
        }

    def meta_get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        return self.handler._get_default_meta_columns("emails")

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[str] = None,
        search_filters: Optional[List[Dict[str, Any]]] = None,
        search_sorts: Optional[List[Dict[str, Any]]] = None,
        allow_search: bool = True,
    ) -> pd.DataFrame:
        emails_df = pd.json_normalize(
            self.get_emails(
                limit=limit,
                where_conditions=conditions,
                properties=targets,
                search_filters=search_filters,
                search_sorts=search_sorts,
                allow_search=allow_search,
            )
        )
        if emails_df.empty:
            emails_df = pd.DataFrame(columns=targets or self._get_default_email_columns())
        return emails_df

    def add(self, email_data: List[dict]):
        self.create_emails(email_data)

    def modify(self, conditions: List[FilterCondition], values: Dict) -> None:
        normalized_conditions = _normalize_filter_conditions(conditions)
        emails_df = pd.json_normalize(self.get_emails(limit=200, where_conditions=normalized_conditions))

        if emails_df.empty:
            raise ValueError("No emails retrieved from HubSpot to evaluate update conditions.")

        executor_conditions = _normalize_conditions_for_executor(normalized_conditions)
        update_query_executor = UPDATEQueryExecutor(emails_df, executor_conditions)
        filtered_df = update_query_executor.execute_query()

        if filtered_df.empty:
            raise ValueError(f"No emails found matching WHERE conditions: {conditions}.")

        email_ids = filtered_df["id"].astype(str).tolist()
        logger.info(f"Updating {len(email_ids)} email(s) matching WHERE conditions")
        self.update_emails(email_ids, values)

    def remove(self, conditions: List[FilterCondition]) -> None:
        normalized_conditions = _normalize_filter_conditions(conditions)
        emails_df = pd.json_normalize(self.get_emails(limit=200, where_conditions=normalized_conditions))

        if emails_df.empty:
            raise ValueError("No emails retrieved from HubSpot to evaluate delete conditions.")

        executor_conditions = _normalize_conditions_for_executor(normalized_conditions)
        delete_query_executor = DELETEQueryExecutor(emails_df, executor_conditions)
        filtered_df = delete_query_executor.execute_query()

        if filtered_df.empty:
            raise ValueError(f"No emails found matching WHERE conditions: {conditions}.")

        email_ids = filtered_df["id"].astype(str).tolist()
        logger.info(f"Deleting {len(email_ids)} email(s) matching WHERE conditions")
        self.delete_emails(email_ids)

    def get_columns(self) -> List[Text]:
        return self._get_default_email_columns()

    @staticmethod
    def _get_default_email_columns() -> List[str]:
        return [
            "id",
            "hs_email_subject",
            "hs_email_text",
            "hs_email_direction",
            "hs_email_status",
            "hs_email_sender_email",
            "hs_email_to_email",
            "hubspot_owner_id",
            "hs_timestamp",
            "createdate",
            "lastmodifieddate",
        ]

    def get_emails(
        self,
        limit: Optional[int] = None,
        where_conditions: Optional[List] = None,
        properties: Optional[List[str]] = None,
        search_filters: Optional[List[Dict[str, Any]]] = None,
        search_sorts: Optional[List[Dict[str, Any]]] = None,
        allow_search: bool = True,
        **kwargs,
    ) -> List[Dict]:
        normalized_conditions = _normalize_filter_conditions(where_conditions)
        hubspot = self.handler.connect()

        requested_properties = properties or []
        default_properties = self._get_default_email_columns()
        columns = requested_properties or default_properties
        hubspot_properties = _build_hubspot_properties(columns)

        api_kwargs = {**kwargs, "properties": hubspot_properties}
        if limit is not None:
            api_kwargs["limit"] = limit
        else:
            api_kwargs.pop("limit", None)

        if allow_search and (search_filters or search_sorts or normalized_conditions):
            filters = search_filters
            if filters is None and normalized_conditions:
                filters = _build_hubspot_search_filters(normalized_conditions, self.SEARCHABLE_COLUMNS)
            if filters is not None or search_sorts is not None:
                search_results = self._search_emails_by_conditions(
                    hubspot, filters, hubspot_properties, limit, search_sorts, columns
                )
                logger.info(f"Retrieved {len(search_results)} emails from HubSpot via search API")
                return search_results

        emails = self.handler._get_objects_all("emails", **api_kwargs)
        emails_dict = []
        for email in emails:
            try:
                emails_dict.append(self._email_to_dict(email, columns))
            except Exception as e:
                logger.warning(f"Error processing email {getattr(email, 'id', 'unknown')}: {str(e)}")
                continue

        logger.info(f"Retrieved {len(emails_dict)} emails from HubSpot")
        return emails_dict

    def _search_emails_by_conditions(
        self,
        hubspot: HubSpot,
        filters: Optional[List[Dict[str, Any]]],
        properties: List[str],
        limit: Optional[int],
        sorts: Optional[List[Dict[str, Any]]],
        columns: List[str],
    ) -> List[Dict[str, Any]]:
        return _execute_hubspot_search(
            hubspot.crm.objects.search_api,
            filters or [],
            properties,
            limit,
            lambda obj: self._email_to_dict(obj, columns),
            sorts=sorts,
            object_type="emails",
        )

    def _email_to_dict(self, email: Any, columns: Optional[List[str]] = None) -> Dict[str, Any]:
        columns = columns or self._get_default_email_columns()
        return self._object_to_dict(email, columns)

    def create_emails(self, emails_data: List[Dict[Text, Any]]) -> None:
        if not emails_data:
            raise ValueError("No email data provided for creation")

        logger.info(f"Attempting to create {len(emails_data)} email(s)")
        hubspot = self.handler.connect()
        emails_to_create = [HubSpotObjectInputCreate(properties=email) for email in emails_data]
        batch_input = BatchInputSimplePublicObjectBatchInputForCreate(inputs=emails_to_create)

        try:
            created_emails = hubspot.crm.objects.emails.batch_api.create(
                batch_input_simple_public_object_batch_input_for_create=batch_input
            )
            if not created_emails or not hasattr(created_emails, "results") or not created_emails.results:
                raise Exception("Email creation returned no results")
            created_ids = [e.id for e in created_emails.results]
            logger.info(f"Successfully created {len(created_ids)} email(s) with IDs: {created_ids}")
        except Exception as e:
            logger.error(f"Emails creation failed: {str(e)}")
            raise Exception(f"Emails creation failed {e}")

    def update_emails(self, email_ids: List[Text], values_to_update: Dict[Text, Any]) -> None:
        hubspot = self.handler.connect()
        emails_to_update = [HubSpotObjectBatchInput(id=eid, properties=values_to_update) for eid in email_ids]
        batch_input = BatchInputSimplePublicObjectBatchInput(inputs=emails_to_update)
        try:
            updated = hubspot.crm.objects.emails.batch_api.update(
                batch_input_simple_public_object_batch_input=batch_input
            )
            logger.info(f"Emails with ID {[e.id for e in updated.results]} updated")
        except Exception as e:
            raise Exception(f"Emails update failed {e}")

    def delete_emails(self, email_ids: List[Text]) -> None:
        hubspot = self.handler.connect()
        emails_to_delete = [HubSpotObjectId(id=eid) for eid in email_ids]
        batch_input = BatchInputSimplePublicObjectId(inputs=emails_to_delete)
        try:
            hubspot.crm.objects.emails.batch_api.archive(batch_input_simple_public_object_id=batch_input)
            logger.info("Emails deleted")
        except Exception as e:
            raise Exception(f"Emails deletion failed {e}")


class MeetingsTable(HubSpotAPIResource):
    """HubSpot Meetings table for meeting logs and scheduled meetings."""

    SEARCHABLE_COLUMNS = {"hs_meeting_title", "hs_meeting_outcome", "id"}

    def meta_get_tables(self, table_name: str) -> Dict[str, Any]:
        row_count = None
        try:
            self.handler.connect()
            row_count = self.handler._estimate_table_rows("meetings")
        except Exception as e:
            logger.warning(f"Could not estimate HubSpot meetings row count: {e}")

        return {
            "TABLE_NAME": "meetings",
            "TABLE_TYPE": "BASE TABLE",
            "TABLE_DESCRIPTION": "HubSpot meeting logs including title, location, outcome and timing",
            "ROW_COUNT": row_count,
        }

    def meta_get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        return self.handler._get_default_meta_columns("meetings")

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[str] = None,
        search_filters: Optional[List[Dict[str, Any]]] = None,
        search_sorts: Optional[List[Dict[str, Any]]] = None,
        allow_search: bool = True,
    ) -> pd.DataFrame:
        meetings_df = pd.json_normalize(
            self.get_meetings(
                limit=limit,
                where_conditions=conditions,
                properties=targets,
                search_filters=search_filters,
                search_sorts=search_sorts,
                allow_search=allow_search,
            )
        )
        if meetings_df.empty:
            meetings_df = pd.DataFrame(columns=targets or self._get_default_meeting_columns())
        return meetings_df

    def add(self, meeting_data: List[dict]):
        self.create_meetings(meeting_data)

    def modify(self, conditions: List[FilterCondition], values: Dict) -> None:
        normalized_conditions = _normalize_filter_conditions(conditions)
        meetings_df = pd.json_normalize(self.get_meetings(limit=200, where_conditions=normalized_conditions))

        if meetings_df.empty:
            raise ValueError("No meetings retrieved from HubSpot to evaluate update conditions.")

        executor_conditions = _normalize_conditions_for_executor(normalized_conditions)
        update_query_executor = UPDATEQueryExecutor(meetings_df, executor_conditions)
        filtered_df = update_query_executor.execute_query()

        if filtered_df.empty:
            raise ValueError(f"No meetings found matching WHERE conditions: {conditions}.")

        meeting_ids = filtered_df["id"].astype(str).tolist()
        logger.info(f"Updating {len(meeting_ids)} meeting(s) matching WHERE conditions")
        self.update_meetings(meeting_ids, values)

    def remove(self, conditions: List[FilterCondition]) -> None:
        normalized_conditions = _normalize_filter_conditions(conditions)
        meetings_df = pd.json_normalize(self.get_meetings(limit=200, where_conditions=normalized_conditions))

        if meetings_df.empty:
            raise ValueError("No meetings retrieved from HubSpot to evaluate delete conditions.")

        executor_conditions = _normalize_conditions_for_executor(normalized_conditions)
        delete_query_executor = DELETEQueryExecutor(meetings_df, executor_conditions)
        filtered_df = delete_query_executor.execute_query()

        if filtered_df.empty:
            raise ValueError(f"No meetings found matching WHERE conditions: {conditions}.")

        meeting_ids = filtered_df["id"].astype(str).tolist()
        logger.info(f"Deleting {len(meeting_ids)} meeting(s) matching WHERE conditions")
        self.delete_meetings(meeting_ids)

    def get_columns(self) -> List[Text]:
        return self._get_default_meeting_columns()

    @staticmethod
    def _get_default_meeting_columns() -> List[str]:
        return [
            "id",
            "hs_meeting_title",
            "hs_meeting_body",
            "hs_meeting_location",
            "hs_meeting_outcome",
            "hs_meeting_start_time",
            "hs_meeting_end_time",
            "hubspot_owner_id",
            "hs_timestamp",
            "createdate",
            "lastmodifieddate",
        ]

    def get_meetings(
        self,
        limit: Optional[int] = None,
        where_conditions: Optional[List] = None,
        properties: Optional[List[str]] = None,
        search_filters: Optional[List[Dict[str, Any]]] = None,
        search_sorts: Optional[List[Dict[str, Any]]] = None,
        allow_search: bool = True,
        **kwargs,
    ) -> List[Dict]:
        normalized_conditions = _normalize_filter_conditions(where_conditions)
        hubspot = self.handler.connect()

        requested_properties = properties or []
        default_properties = self._get_default_meeting_columns()
        columns = requested_properties or default_properties
        hubspot_properties = _build_hubspot_properties(columns)

        api_kwargs = {**kwargs, "properties": hubspot_properties}
        if limit is not None:
            api_kwargs["limit"] = limit
        else:
            api_kwargs.pop("limit", None)

        if allow_search and (search_filters or search_sorts or normalized_conditions):
            filters = search_filters
            if filters is None and normalized_conditions:
                filters = _build_hubspot_search_filters(normalized_conditions, self.SEARCHABLE_COLUMNS)
            if filters is not None or search_sorts is not None:
                search_results = self._search_meetings_by_conditions(
                    hubspot, filters, hubspot_properties, limit, search_sorts, columns
                )
                logger.info(f"Retrieved {len(search_results)} meetings from HubSpot via search API")
                return search_results

        meetings = self.handler._get_objects_all("meetings", **api_kwargs)
        meetings_dict = []
        for meeting in meetings:
            try:
                meetings_dict.append(self._meeting_to_dict(meeting, columns))
            except Exception as e:
                logger.warning(f"Error processing meeting {getattr(meeting, 'id', 'unknown')}: {str(e)}")
                continue

        logger.info(f"Retrieved {len(meetings_dict)} meetings from HubSpot")
        return meetings_dict

    def _search_meetings_by_conditions(
        self,
        hubspot: HubSpot,
        filters: Optional[List[Dict[str, Any]]],
        properties: List[str],
        limit: Optional[int],
        sorts: Optional[List[Dict[str, Any]]],
        columns: List[str],
    ) -> List[Dict[str, Any]]:
        return _execute_hubspot_search(
            hubspot.crm.objects.search_api,
            filters or [],
            properties,
            limit,
            lambda obj: self._meeting_to_dict(obj, columns),
            sorts=sorts,
            object_type="meetings",
        )

    def _meeting_to_dict(self, meeting: Any, columns: Optional[List[str]] = None) -> Dict[str, Any]:
        columns = columns or self._get_default_meeting_columns()
        return self._object_to_dict(meeting, columns)

    def create_meetings(self, meetings_data: List[Dict[Text, Any]]) -> None:
        if not meetings_data:
            raise ValueError("No meeting data provided for creation")

        logger.info(f"Attempting to create {len(meetings_data)} meeting(s)")
        hubspot = self.handler.connect()
        meetings_to_create = [HubSpotObjectInputCreate(properties=meeting) for meeting in meetings_data]
        batch_input = BatchInputSimplePublicObjectBatchInputForCreate(inputs=meetings_to_create)

        try:
            created_meetings = hubspot.crm.objects.meetings.batch_api.create(
                batch_input_simple_public_object_batch_input_for_create=batch_input
            )
            if not created_meetings or not hasattr(created_meetings, "results") or not created_meetings.results:
                raise Exception("Meeting creation returned no results")
            created_ids = [m.id for m in created_meetings.results]
            logger.info(f"Successfully created {len(created_ids)} meeting(s) with IDs: {created_ids}")
        except Exception as e:
            logger.error(f"Meetings creation failed: {str(e)}")
            raise Exception(f"Meetings creation failed {e}")

    def update_meetings(self, meeting_ids: List[Text], values_to_update: Dict[Text, Any]) -> None:
        hubspot = self.handler.connect()
        meetings_to_update = [HubSpotObjectBatchInput(id=mid, properties=values_to_update) for mid in meeting_ids]
        batch_input = BatchInputSimplePublicObjectBatchInput(inputs=meetings_to_update)
        try:
            updated = hubspot.crm.objects.meetings.batch_api.update(
                batch_input_simple_public_object_batch_input=batch_input
            )
            logger.info(f"Meetings with ID {[m.id for m in updated.results]} updated")
        except Exception as e:
            raise Exception(f"Meetings update failed {e}")

    def delete_meetings(self, meeting_ids: List[Text]) -> None:
        hubspot = self.handler.connect()
        meetings_to_delete = [HubSpotObjectId(id=mid) for mid in meeting_ids]
        batch_input = BatchInputSimplePublicObjectId(inputs=meetings_to_delete)
        try:
            hubspot.crm.objects.meetings.batch_api.archive(batch_input_simple_public_object_id=batch_input)
            logger.info("Meetings deleted")
        except Exception as e:
            raise Exception(f"Meetings deletion failed {e}")


class NotesTable(HubSpotAPIResource):
    """HubSpot Notes table for timeline notes on records."""

    SEARCHABLE_COLUMNS = {"id"}

    def meta_get_tables(self, table_name: str) -> Dict[str, Any]:
        row_count = None
        try:
            self.handler.connect()
            row_count = self.handler._estimate_table_rows("notes")
        except Exception as e:
            logger.warning(f"Could not estimate HubSpot notes row count: {e}")

        return {
            "TABLE_NAME": "notes",
            "TABLE_TYPE": "BASE TABLE",
            "TABLE_DESCRIPTION": "HubSpot notes for timeline entries on records",
            "ROW_COUNT": row_count,
        }

    def meta_get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        return self.handler._get_default_meta_columns("notes")

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[str] = None,
        search_filters: Optional[List[Dict[str, Any]]] = None,
        search_sorts: Optional[List[Dict[str, Any]]] = None,
        allow_search: bool = True,
    ) -> pd.DataFrame:
        notes_df = pd.json_normalize(
            self.get_notes(
                limit=limit,
                where_conditions=conditions,
                properties=targets,
                search_filters=search_filters,
                search_sorts=search_sorts,
                allow_search=allow_search,
            )
        )
        if notes_df.empty:
            notes_df = pd.DataFrame(columns=targets or self._get_default_note_columns())
        return notes_df

    def add(self, note_data: List[dict]):
        self.create_notes(note_data)

    def modify(self, conditions: List[FilterCondition], values: Dict) -> None:
        normalized_conditions = _normalize_filter_conditions(conditions)
        notes_df = pd.json_normalize(self.get_notes(limit=200, where_conditions=normalized_conditions))

        if notes_df.empty:
            raise ValueError("No notes retrieved from HubSpot to evaluate update conditions.")

        executor_conditions = _normalize_conditions_for_executor(normalized_conditions)
        update_query_executor = UPDATEQueryExecutor(notes_df, executor_conditions)
        filtered_df = update_query_executor.execute_query()

        if filtered_df.empty:
            raise ValueError(f"No notes found matching WHERE conditions: {conditions}.")

        note_ids = filtered_df["id"].astype(str).tolist()
        logger.info(f"Updating {len(note_ids)} note(s) matching WHERE conditions")
        self.update_notes(note_ids, values)

    def remove(self, conditions: List[FilterCondition]) -> None:
        normalized_conditions = _normalize_filter_conditions(conditions)
        notes_df = pd.json_normalize(self.get_notes(limit=200, where_conditions=normalized_conditions))

        if notes_df.empty:
            raise ValueError("No notes retrieved from HubSpot to evaluate delete conditions.")

        executor_conditions = _normalize_conditions_for_executor(normalized_conditions)
        delete_query_executor = DELETEQueryExecutor(notes_df, executor_conditions)
        filtered_df = delete_query_executor.execute_query()

        if filtered_df.empty:
            raise ValueError(f"No notes found matching WHERE conditions: {conditions}.")

        note_ids = filtered_df["id"].astype(str).tolist()
        logger.info(f"Deleting {len(note_ids)} note(s) matching WHERE conditions")
        self.delete_notes(note_ids)

    def get_columns(self) -> List[Text]:
        return self._get_default_note_columns()

    @staticmethod
    def _get_default_note_columns() -> List[str]:
        return ["id", "hs_note_body", "hubspot_owner_id", "hs_timestamp", "createdate", "lastmodifieddate"]

    def get_notes(
        self,
        limit: Optional[int] = None,
        where_conditions: Optional[List] = None,
        properties: Optional[List[str]] = None,
        search_filters: Optional[List[Dict[str, Any]]] = None,
        search_sorts: Optional[List[Dict[str, Any]]] = None,
        allow_search: bool = True,
        **kwargs,
    ) -> List[Dict]:
        normalized_conditions = _normalize_filter_conditions(where_conditions)
        hubspot = self.handler.connect()

        requested_properties = properties or []
        default_properties = self._get_default_note_columns()
        columns = requested_properties or default_properties
        hubspot_properties = _build_hubspot_properties(columns)

        api_kwargs = {**kwargs, "properties": hubspot_properties}
        if limit is not None:
            api_kwargs["limit"] = limit
        else:
            api_kwargs.pop("limit", None)

        if allow_search and (search_filters or search_sorts or normalized_conditions):
            filters = search_filters
            if filters is None and normalized_conditions:
                filters = _build_hubspot_search_filters(normalized_conditions, self.SEARCHABLE_COLUMNS)
            if filters is not None or search_sorts is not None:
                search_results = self._search_notes_by_conditions(
                    hubspot, filters, hubspot_properties, limit, search_sorts, columns
                )
                logger.info(f"Retrieved {len(search_results)} notes from HubSpot via search API")
                return search_results

        notes = self.handler._get_objects_all("notes", **api_kwargs)
        notes_dict = []
        for note in notes:
            try:
                notes_dict.append(self._note_to_dict(note, columns))
            except Exception as e:
                logger.warning(f"Error processing note {getattr(note, 'id', 'unknown')}: {str(e)}")
                continue

        logger.info(f"Retrieved {len(notes_dict)} notes from HubSpot")
        return notes_dict

    def _search_notes_by_conditions(
        self,
        hubspot: HubSpot,
        filters: Optional[List[Dict[str, Any]]],
        properties: List[str],
        limit: Optional[int],
        sorts: Optional[List[Dict[str, Any]]],
        columns: List[str],
    ) -> List[Dict[str, Any]]:
        return _execute_hubspot_search(
            hubspot.crm.objects.search_api,
            filters or [],
            properties,
            limit,
            lambda obj: self._note_to_dict(obj, columns),
            sorts=sorts,
            object_type="notes",
        )

    def _note_to_dict(self, note: Any, columns: Optional[List[str]] = None) -> Dict[str, Any]:
        columns = columns or self._get_default_note_columns()
        return self._object_to_dict(note, columns)

    def create_notes(self, notes_data: List[Dict[Text, Any]]) -> None:
        if not notes_data:
            raise ValueError("No note data provided for creation")

        logger.info(f"Attempting to create {len(notes_data)} note(s)")
        hubspot = self.handler.connect()
        notes_to_create = [HubSpotObjectInputCreate(properties=note) for note in notes_data]
        batch_input = BatchInputSimplePublicObjectBatchInputForCreate(inputs=notes_to_create)

        try:
            created_notes = hubspot.crm.objects.notes.batch_api.create(
                batch_input_simple_public_object_batch_input_for_create=batch_input
            )
            if not created_notes or not hasattr(created_notes, "results") or not created_notes.results:
                raise Exception("Note creation returned no results")
            created_ids = [n.id for n in created_notes.results]
            logger.info(f"Successfully created {len(created_ids)} note(s) with IDs: {created_ids}")
        except Exception as e:
            logger.error(f"Notes creation failed: {str(e)}")
            raise Exception(f"Notes creation failed {e}")

    def update_notes(self, note_ids: List[Text], values_to_update: Dict[Text, Any]) -> None:
        hubspot = self.handler.connect()
        notes_to_update = [HubSpotObjectBatchInput(id=nid, properties=values_to_update) for nid in note_ids]
        batch_input = BatchInputSimplePublicObjectBatchInput(inputs=notes_to_update)
        try:
            updated = hubspot.crm.objects.notes.batch_api.update(
                batch_input_simple_public_object_batch_input=batch_input
            )
            logger.info(f"Notes with ID {[n.id for n in updated.results]} updated")
        except Exception as e:
            raise Exception(f"Notes update failed {e}")

    def delete_notes(self, note_ids: List[Text]) -> None:
        hubspot = self.handler.connect()
        notes_to_delete = [HubSpotObjectId(id=nid) for nid in note_ids]
        batch_input = BatchInputSimplePublicObjectId(inputs=notes_to_delete)
        try:
            hubspot.crm.objects.notes.batch_api.archive(batch_input_simple_public_object_id=batch_input)
            logger.info("Notes deleted")
        except Exception as e:
            raise Exception(f"Notes deletion failed {e}")
