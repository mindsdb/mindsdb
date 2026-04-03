from typing import List, Dict, Any, Optional, Tuple, Set, Iterable
import calendar
import re
from datetime import date, datetime, time, timedelta

import pandas as pd
from hubspot import HubSpot
from hubspot.crm.associations.models import BatchInputPublicObjectId, PublicObjectId
from hubspot.crm.contacts.models import (
    BatchReadInputSimplePublicObjectId,
    SimplePublicObjectId as ContactObjectId,
)
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
from mindsdb.integrations.handlers.hubspot_handler.hubspot_association_utils import (
    get_association_targets_for_object,
    get_primary_association_columns,
    enrich_object_with_associations,
)

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


def _parse_interval_value(interval_expr: Any) -> Optional[Tuple[float, str]]:
    if interval_expr is None:
        return None

    raw = None
    if isinstance(interval_expr, sql_ast.Interval):
        args = getattr(interval_expr, "args", []) or []
        if len(args) >= 2:
            value = args[0].value if isinstance(args[0], sql_ast.Constant) else str(args[0])
            unit = args[1].value if isinstance(args[1], sql_ast.Constant) else str(args[1])
            raw = f"{value} {unit}"
        elif len(args) == 1:
            raw = args[0].value if isinstance(args[0], sql_ast.Constant) else str(args[0])
    elif isinstance(interval_expr, sql_ast.Constant):
        raw = interval_expr.value
    elif isinstance(interval_expr, sql_ast.UnaryOperation):
        op = getattr(interval_expr, "op", None)
        if op == "-" and interval_expr.args:
            parsed = _parse_interval_value(interval_expr.args[0])
            if parsed is None:
                return None
            value, unit = parsed
            return (-value, unit)
    else:
        raw = str(interval_expr)

    if raw is None:
        return None

    match = re.search(r"(?i)interval\\s+'?([0-9]+(?:\\.[0-9]+)?)'?\\s+([a-zA-Z]+)", str(raw))
    if not match:
        match = re.search(r"(?i)^\\s*'?([0-9]+(?:\\.[0-9]+)?)'?\\s+([a-zA-Z]+)\\s*$", str(raw))
    if not match:
        return None

    value = float(match.group(1))
    unit = match.group(2).lower()
    if unit.endswith("s"):
        unit = unit[:-1]
    return value, unit


def _add_months(dt_value: Any, months: int) -> Any:
    if not isinstance(dt_value, (date, datetime)):
        return dt_value
    year = dt_value.year + (dt_value.month - 1 + months) // 12
    month = (dt_value.month - 1 + months) % 12 + 1
    day = min(dt_value.day, calendar.monthrange(year, month)[1])
    if isinstance(dt_value, datetime):
        return dt_value.replace(year=year, month=month, day=day)
    return dt_value.replace(year=year, month=month, day=day)


def _apply_interval(base: Any, interval: Tuple[float, str]) -> Any:
    value, unit = interval
    if not isinstance(base, (date, datetime)):
        return base

    if unit == "year":
        return _add_months(base, int(round(value * 12)))
    if unit == "month":
        return _add_months(base, int(round(value)))

    if unit in {"day", "hour", "minute", "second", "week"}:
        if isinstance(base, date) and not isinstance(base, datetime) and unit in {"hour", "minute", "second"}:
            base = datetime.combine(base, time.min)

        seconds = value
        if unit == "week":
            seconds = value * 7
            unit = "day"
        if unit == "day":
            return base + timedelta(days=seconds)
        if unit == "hour":
            return base + timedelta(hours=seconds)
        if unit == "minute":
            return base + timedelta(minutes=seconds)
        if unit == "second":
            return base + timedelta(seconds=seconds)

    return base


def _evaluate_function_value(node: sql_ast.Function) -> Optional[Any]:
    func = getattr(node, "op", None) or getattr(node, "name", None)
    if not func:
        return None

    func = str(func).lower()
    if func in {"curdate", "current_date"}:
        return date.today()
    if func in {"now", "current_timestamp"}:
        return datetime.now()
    if func in {"date_sub", "date_add"} and len(node.args) == 2:
        base = _evaluate_value_node(node.args[0])
        interval = _parse_interval_value(node.args[1])
        if base is None or interval is None:
            return None
        if func == "date_sub":
            interval = (-interval[0], interval[1])
        return _apply_interval(base, interval)

    return None


def _evaluate_value_node(node: ASTNode) -> Optional[Any]:
    if isinstance(node, sql_ast.Constant):
        return node.value
    if isinstance(node, sql_ast.Identifier):
        ident = node.parts[-1].lower() if node.parts else ""
        if ident in {"curdate", "current_date"}:
            return date.today()
        if ident in {"now", "current_timestamp"}:
            return datetime.now()
        return None
    if isinstance(node, sql_ast.Interval):
        return _parse_interval_value(node)
    if isinstance(node, sql_ast.Tuple):
        return [item.value if isinstance(item, sql_ast.Constant) else _evaluate_value_node(item) for item in node.items]
    if isinstance(node, sql_ast.Function):
        return _evaluate_function_value(node)
    if isinstance(node, sql_ast.UnaryOperation):
        op = getattr(node, "op", None)
        if op == "-" and node.args:
            value = _evaluate_value_node(node.args[0])
            if value is None:
                return None
            if isinstance(value, (int, float)):
                return -value
            if isinstance(value, tuple) and len(value) == 2:
                return (-value[0], value[1])
        return None
    if isinstance(node, sql_ast.BinaryOperation):
        op = getattr(node, "op", None)
        if not op or len(node.args) != 2:
            return None
        left = _evaluate_value_node(node.args[0])
        right = _evaluate_value_node(node.args[1])
        if left is None or right is None:
            return None
        op = op.lower()
        if op in {"+", "-"}:
            if isinstance(left, (date, datetime)) and isinstance(right, tuple):
                interval = right
                if op == "-":
                    interval = (-interval[0], interval[1])
                return _apply_interval(left, interval)
            if isinstance(right, (date, datetime)) and isinstance(left, tuple) and op == "+":
                return _apply_interval(right, left)
            if isinstance(left, (int, float)) and isinstance(right, (int, float)):
                return left + right if op == "+" else left - right
        if op == "*" and isinstance(left, (int, float)) and isinstance(right, (int, float)):
            return left * right
        if op == "/" and isinstance(left, (int, float)) and isinstance(right, (int, float)) and right != 0:
            return left / right
    return None


def _extract_comparison_conditions_with_functions(binary_op: ASTNode) -> List[List[Any]]:
    conditions: List[List[Any]] = []

    def _extract_identifier(node: ASTNode) -> Optional[sql_ast.Identifier]:
        if isinstance(node, sql_ast.Identifier):
            return node
        if isinstance(node, sql_ast.Function):
            func = getattr(node, "op", None) or getattr(node, "name", None)
            if func and str(func).lower() in {"lower", "upper"} and node.args:
                if isinstance(node.args[0], sql_ast.Identifier):
                    return node.args[0]
        return None

    def _invert_comparison(op: str) -> Optional[str]:
        inverse_ops = {
            "<": ">",
            "<=": ">=",
            ">": "<",
            ">=": "<=",
            "lt": "gt",
            "lte": "gte",
            "gt": "lt",
            "gte": "lte",
        }
        if op in inverse_ops:
            return inverse_ops[op]
        if op in {"=", "==", "eq", "!=", "<>", "neq"}:
            return op
        return None

    def _extract(node: ASTNode, **kwargs):
        if isinstance(node, sql_ast.BinaryOperation):
            op = node.op.lower()
            if op == "and":
                return

            arg1, arg2 = node.args
            identifier = _extract_identifier(arg1)
            if identifier is None:
                identifier = _extract_identifier(arg2)
                if identifier is None:
                    logger.debug(f"Skipping unsupported condition arg1: {arg1}")
                    return
                value = _evaluate_value_node(arg1)
                if value is None:
                    logger.debug(f"Skipping unsupported condition arg1: {arg1}")
                    return
                inverted_op = _invert_comparison(op)
                if inverted_op is None:
                    logger.debug(f"Skipping unsupported condition op swap: {op}")
                    return
                conditions.append([inverted_op, identifier.parts[-1], value])
                return

            value = _evaluate_value_node(arg2)
            if value is None:
                logger.debug(f"Skipping unsupported condition arg2: {arg2}")
                return

            conditions.append([op, identifier.parts[-1], value])
        if isinstance(node, sql_ast.BetweenOperation):
            var, up, down = node.args
            if not isinstance(var, sql_ast.Identifier):
                logger.debug(f"Skipping unsupported between condition: {node}")
                return

            up_value = _evaluate_value_node(up)
            down_value = _evaluate_value_node(down)
            if up_value is None or down_value is None:
                logger.debug(f"Skipping unsupported between condition: {node}")
                return

            op = node.op.lower()
            conditions.append([op, var.parts[-1], (up_value, down_value)])

    query_traversal(binary_op, _extract)
    return conditions


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
        ("primary_company_id", "VARCHAR", "Primary associated company ID"),
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
        ("primary_company_id", "VARCHAR", "Primary associated company ID"),
        ("primary_contact_id", "VARCHAR", "Primary associated contact ID"),
        ("dealstage", "VARCHAR", "Deal stage"),
        ("dealstage_label", "VARCHAR", "Deal stage label"),
        ("pipeline", "VARCHAR", "Sales pipeline"),
        ("pipeline_label", "VARCHAR", "Pipeline label"),
        ("closedate", "DATE", "Expected close date"),
        ("hubspot_owner_id", "VARCHAR", "Owner ID"),
        ("owner_name", "VARCHAR", "Owner name"),
        ("owner_email", "VARCHAR", "Owner email"),
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
        ("primary_company_id", "VARCHAR", "Primary associated company ID"),
        ("primary_contact_id", "VARCHAR", "Primary associated contact ID"),
        ("primary_deal_id", "VARCHAR", "Primary associated deal ID"),
        ("createdate", "TIMESTAMP", "Creation date"),
        ("lastmodifieddate", "TIMESTAMP", "Last modification date"),
    ],
    # Association tables definitions for many-to-many relationships
    # Reference: https://developers.hubspot.com/docs/api-reference/crm-associations-v4/guide#associate-records-without-a-label
    "company_contacts": [
        ("company_id", "VARCHAR", "Company ID"),
        ("contact_id", "VARCHAR", "Contact ID"),
        ("association_type", "VARCHAR", "Association type"),
        ("association_label", "VARCHAR", "Association label"),
    ],
    "company_deals": [
        ("company_id", "VARCHAR", "Company ID"),
        ("deal_id", "VARCHAR", "Deal ID"),
        ("association_type", "VARCHAR", "Association type"),
        ("association_label", "VARCHAR", "Association label"),
    ],
    "company_tickets": [
        ("company_id", "VARCHAR", "Company ID"),
        ("ticket_id", "VARCHAR", "Ticket ID"),
        ("association_type", "VARCHAR", "Association type"),
        ("association_label", "VARCHAR", "Association label"),
    ],
    "contact_companies": [
        ("contact_id", "VARCHAR", "Contact ID"),
        ("company_id", "VARCHAR", "Company ID"),
        ("association_type", "VARCHAR", "Association type"),
        ("association_label", "VARCHAR", "Association label"),
    ],
    "contact_deals": [
        ("contact_id", "VARCHAR", "Contact ID"),
        ("deal_id", "VARCHAR", "Deal ID"),
        ("association_type", "VARCHAR", "Association type"),
        ("association_label", "VARCHAR", "Association label"),
    ],
    "contact_tickets": [
        ("contact_id", "VARCHAR", "Contact ID"),
        ("ticket_id", "VARCHAR", "Ticket ID"),
        ("association_type", "VARCHAR", "Association type"),
        ("association_label", "VARCHAR", "Association label"),
    ],
    "deal_companies": [
        ("deal_id", "VARCHAR", "Deal ID"),
        ("company_id", "VARCHAR", "Company ID"),
        ("association_type", "VARCHAR", "Association type"),
        ("association_label", "VARCHAR", "Association label"),
    ],
    "deal_contacts": [
        ("deal_id", "VARCHAR", "Deal ID"),
        ("contact_id", "VARCHAR", "Contact ID"),
        ("association_type", "VARCHAR", "Association type"),
        ("association_label", "VARCHAR", "Association label"),
    ],
    "ticket_companies": [
        ("ticket_id", "VARCHAR", "Ticket ID"),
        ("company_id", "VARCHAR", "Company ID"),
        ("association_type", "VARCHAR", "Association type"),
        ("association_label", "VARCHAR", "Association label"),
    ],
    "ticket_contacts": [
        ("ticket_id", "VARCHAR", "Ticket ID"),
        ("contact_id", "VARCHAR", "Contact ID"),
        ("association_type", "VARCHAR", "Association type"),
        ("association_label", "VARCHAR", "Association label"),
    ],
    "ticket_deals": [
        ("ticket_id", "VARCHAR", "Ticket ID"),
        ("deal_id", "VARCHAR", "Deal ID"),
        ("association_type", "VARCHAR", "Association type"),
        ("association_label", "VARCHAR", "Association label"),
    ],
    # Reference: https://developers.hubspot.com/docs/api-reference/crm-tasks-v3/guide
    "tasks": [
        ("hs_task_subject", "VARCHAR", "Task subject"),
        ("hs_task_body", "TEXT", "Task body/description"),
        ("hs_task_status", "VARCHAR", "Task status"),
        ("hs_task_priority", "VARCHAR", "Task priority"),
        ("hs_task_type", "VARCHAR", "Task type"),
        ("hs_timestamp", "TIMESTAMP", "Due date"),
        ("hubspot_owner_id", "VARCHAR", "Owner ID"),
        ("primary_company_id", "VARCHAR", "Primary associated company ID"),
        ("primary_contact_id", "VARCHAR", "Primary associated contact ID"),
        ("primary_deal_id", "VARCHAR", "Primary associated deal ID"),
        ("createdate", "TIMESTAMP", "Creation date"),
        ("lastmodifieddate", "TIMESTAMP", "Last modification date"),
    ],
    # Reference: https://developers.hubspot.com/docs/api-reference/crm-calls-v3/guide
    "calls": [
        ("hs_call_title", "VARCHAR", "Call title"),
        ("hs_call_body", "TEXT", "Call notes/description"),
        ("hs_call_direction", "VARCHAR", "Call direction (INBOUND/OUTBOUND)"),
        ("hs_call_disposition", "VARCHAR", "Call outcome"),
        ("hs_call_duration", "INTEGER", "Call duration in milliseconds"),
        ("hs_call_status", "VARCHAR", "Call status"),
        ("hubspot_owner_id", "VARCHAR", "Owner ID"),
        ("hs_timestamp", "TIMESTAMP", "Call timestamp"),
        ("primary_company_id", "VARCHAR", "Primary associated company ID"),
        ("primary_contact_id", "VARCHAR", "Primary associated contact ID"),
        ("primary_deal_id", "VARCHAR", "Primary associated deal ID"),
        ("createdate", "TIMESTAMP", "Creation date"),
        ("lastmodifieddate", "TIMESTAMP", "Last modification date"),
    ],
    # Reference: https://developers.hubspot.com/docs/api-reference/crm-emails-v3/guide
    "emails": [
        ("hs_email_subject", "VARCHAR", "Email subject"),
        ("hs_email_text", "TEXT", "Email body text"),
        ("hs_email_direction", "VARCHAR", "Email direction (INCOMING/FORWARDED/EMAIL)"),
        ("hs_email_status", "VARCHAR", "Email status"),
        ("hs_email_sender_email", "VARCHAR", "Sender email address"),
        ("hs_email_to_email", "VARCHAR", "Recipient email address"),
        ("hubspot_owner_id", "VARCHAR", "Owner ID"),
        ("hs_timestamp", "TIMESTAMP", "Email timestamp"),
        ("primary_company_id", "VARCHAR", "Primary associated company ID"),
        ("primary_contact_id", "VARCHAR", "Primary associated contact ID"),
        ("primary_deal_id", "VARCHAR", "Primary associated deal ID"),
        ("createdate", "TIMESTAMP", "Creation date"),
        ("lastmodifieddate", "TIMESTAMP", "Last modification date"),
    ],
    # Reference: https://developers.hubspot.com/docs/api-reference/crm-meetings-v3/guide
    "meetings": [
        ("hs_meeting_title", "VARCHAR", "Meeting title"),
        ("hs_meeting_body", "TEXT", "Meeting description"),
        ("hs_meeting_location", "VARCHAR", "Meeting location"),
        ("hs_meeting_outcome", "VARCHAR", "Meeting outcome"),
        ("hs_meeting_start_time", "TIMESTAMP", "Meeting start time"),
        ("hs_meeting_end_time", "TIMESTAMP", "Meeting end time"),
        ("hubspot_owner_id", "VARCHAR", "Owner ID"),
        ("hs_timestamp", "TIMESTAMP", "Meeting timestamp"),
        ("primary_company_id", "VARCHAR", "Primary associated company ID"),
        ("primary_contact_id", "VARCHAR", "Primary associated contact ID"),
        ("primary_deal_id", "VARCHAR", "Primary associated deal ID"),
        ("createdate", "TIMESTAMP", "Creation date"),
        ("lastmodifieddate", "TIMESTAMP", "Last modification date"),
    ],
    # Reference: https://developers.hubspot.com/docs/api-reference/crm-notes-v3/guide
    "notes": [
        ("hs_note_body", "TEXT", "Note content"),
        ("hubspot_owner_id", "VARCHAR", "Owner ID"),
        ("hs_timestamp", "TIMESTAMP", "Note timestamp"),
        ("primary_company_id", "VARCHAR", "Primary associated company ID"),
        ("primary_contact_id", "VARCHAR", "Primary associated contact ID"),
        ("primary_deal_id", "VARCHAR", "Primary associated deal ID"),
        ("createdate", "TIMESTAMP", "Creation date"),
        ("lastmodifieddate", "TIMESTAMP", "Last modification date"),
    ],
    # Reference: https://developers.hubspot.com/docs/api-reference/crm-crm-owners-v3/guide#crm-api-owners
    "owners": [
        ("id", "VARCHAR", "Owner ID"),
        ("email", "VARCHAR", "Owner email"),
        ("first_name", "VARCHAR", "First name"),
        ("last_name", "VARCHAR", "Last name"),
        ("full_name", "VARCHAR", "Full name"),
        ("user_id", "VARCHAR", "User ID"),
        ("teams", "TEXT", "Teams"),
        ("created_at", "TIMESTAMP", "Created at"),
        ("updated_at", "TIMESTAMP", "Updated at"),
        ("archived", "BOOLEAN", "Archived"),
    ],
    "deal_stages": [
        ("pipeline_id", "VARCHAR", "Pipeline ID"),
        ("pipeline_label", "VARCHAR", "Pipeline label"),
        ("stage_id", "VARCHAR", "Stage ID"),
        ("stage_label", "VARCHAR", "Stage label"),
        ("stage_order", "INTEGER", "Stage display order"),
        ("stage_probability", "DECIMAL", "Stage probability"),
        ("stage_archived", "BOOLEAN", "Stage archived"),
    ],
    "leads": [
        ("hs_lead_name", "VARCHAR", "Lead name"),
        ("hs_lead_type", "VARCHAR", "Lead type"),
        ("hs_lead_label", "VARCHAR", "Lead label/status"),
        ("hubspot_owner_id", "VARCHAR", "Owner ID"),
        ("hs_timestamp", "TIMESTAMP", "Lead timestamp"),
        ("primary_contact_id", "VARCHAR", "Primary associated contact ID"),
        ("primary_company_id", "VARCHAR", "Primary associated company ID"),
        ("createdate", "TIMESTAMP", "Creation date"),
        ("lastmodifieddate", "TIMESTAMP", "Last modification date"),
    ],
}


def _get_attr_value(obj: Any, *names: str) -> Any:
    for name in names:
        if isinstance(obj, dict) and name in obj:
            return obj[name]
        if hasattr(obj, name):
            return getattr(obj, name)
    return None


def _as_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    return str(value)


# Format owner teams into a comma-separated string
def _format_owner_teams(teams: Any) -> Optional[str]:
    if not teams:
        return None
    if isinstance(teams, (list, tuple)):
        parts: List[str] = []
        for team in teams:
            team_id = _get_attr_value(team, "id", "team_id")
            team_name = _get_attr_value(team, "name")
            if team_name and team_id:
                parts.append(f"{team_name} ({team_id})")
            elif team_name:
                parts.append(str(team_name))
            elif team_id:
                parts.append(str(team_id))
        return ", ".join(parts) if parts else None
    return str(teams)


# Convert HubSpot owner object to a dictionary row
def _owner_to_row(owner: Any) -> Dict[str, Any]:
    owner_id = _as_str(_get_attr_value(owner, "id"))
    email = _get_attr_value(owner, "email")
    first_name = _get_attr_value(owner, "first_name", "firstName")
    last_name = _get_attr_value(owner, "last_name", "lastName")
    user_id = _as_str(_get_attr_value(owner, "user_id", "userId"))
    created_at = _get_attr_value(owner, "created_at", "createdAt")
    updated_at = _get_attr_value(owner, "updated_at", "updatedAt")
    archived = _get_attr_value(owner, "archived")
    teams = _format_owner_teams(_get_attr_value(owner, "teams"))

    name_parts = [part for part in (first_name, last_name) if part]
    full_name = " ".join(name_parts) if name_parts else (email or None)

    return {
        "id": owner_id,
        "email": email,
        "first_name": first_name,
        "last_name": last_name,
        "full_name": full_name,
        "user_id": user_id,
        "teams": teams,
        "created_at": created_at,
        "updated_at": updated_at,
        "archived": archived,
    }


def _fetch_owner_pages(handler, archived: bool = False) -> List[Dict[str, Any]]:
    hubspot = handler.connect()
    results: List[Dict[str, Any]] = []
    after = None

    while True:
        response = hubspot.crm.owners.owners_api.get_page(limit=500, after=after, archived=archived)
        owners = getattr(response, "results", None) or getattr(response, "owners", None) or []
        for owner in owners:
            results.append(_owner_to_row(owner))

        paging = getattr(response, "paging", None)
        next_page = getattr(paging, "next", None) if paging else None
        after = getattr(next_page, "after", None) if next_page else None
        if after is None:
            break

    return results


def _fetch_owner_rows(handler, include_archived: bool = True) -> List[Dict[str, Any]]:
    rows = _fetch_owner_pages(handler, archived=False)
    if include_archived:
        rows.extend(_fetch_owner_pages(handler, archived=True))

    deduped: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        owner_id = row.get("id")
        if not owner_id:
            continue
        deduped.setdefault(owner_id, row)

    return list(deduped.values()) if deduped else rows


def _get_owner_rows(handler) -> List[Dict[str, Any]]:
    cache_key = "_hubspot_owner_rows_cache"
    cached = getattr(handler, cache_key, None)
    if cached is None:
        cached = _fetch_owner_rows(handler, include_archived=True)
        setattr(handler, cache_key, cached)
    return cached


def _get_owner_map(handler) -> Dict[str, Dict[str, Any]]:
    cache_key = "_hubspot_owner_map_cache"
    cached = getattr(handler, cache_key, None)
    if cached is None:
        rows = _get_owner_rows(handler)
        cached = {row["id"]: row for row in rows if row.get("id")}
        setattr(handler, cache_key, cached)
    return cached


def _fetch_deal_stage_rows(handler) -> List[Dict[str, Any]]:
    hubspot = handler.connect()
    response = hubspot.crm.pipelines.pipelines_api.get_all("deals")
    pipelines = getattr(response, "results", None) or response or []
    rows: List[Dict[str, Any]] = []

    for pipeline in pipelines:
        pipeline_id = _as_str(_get_attr_value(pipeline, "id"))
        pipeline_label = _get_attr_value(pipeline, "label")
        stages = _get_attr_value(pipeline, "stages") or []

        for stage in stages:
            stage_id = _as_str(_get_attr_value(stage, "id"))
            stage_label = _get_attr_value(stage, "label")
            stage_order = _get_attr_value(stage, "display_order", "displayOrder")
            stage_archived = _get_attr_value(stage, "archived")
            metadata = _get_attr_value(stage, "metadata") or {}
            stage_probability = metadata.get("probability") if isinstance(metadata, dict) else None

            rows.append(
                {
                    "pipeline_id": pipeline_id,
                    "pipeline_label": pipeline_label,
                    "stage_id": stage_id,
                    "stage_label": stage_label,
                    "stage_order": stage_order,
                    "stage_probability": stage_probability,
                    "stage_archived": stage_archived,
                }
            )

    return rows


def _get_deal_stage_rows(handler) -> List[Dict[str, Any]]:
    cache_key = "_hubspot_deal_stage_rows_cache"
    cached = getattr(handler, cache_key, None)
    if cached is None:
        cached = _fetch_deal_stage_rows(handler)
        setattr(handler, cache_key, cached)
    return cached


# Get deal stage maps: (pipeline_id, stage_id) -> row and stage_id -> row
def _get_deal_stage_maps(
    handler,
) -> Tuple[Dict[Tuple[str, str], Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    cache_key = "_hubspot_deal_stage_map_cache"
    cached = getattr(handler, cache_key, None)
    if cached is None:
        rows = _get_deal_stage_rows(handler)
        pair_map: Dict[Tuple[str, str], Dict[str, Any]] = {}
        stage_map: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            pipeline_id = row.get("pipeline_id")
            stage_id = row.get("stage_id")
            if pipeline_id and stage_id:
                pair_map[(pipeline_id, stage_id)] = row
            if stage_id and stage_id not in stage_map:
                stage_map[stage_id] = row
        cached = (pair_map, stage_map)
        setattr(handler, cache_key, cached)
    return cached


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
            values = [v for v in values if v is not None]
            if not values:
                logger.debug(
                    f"No valid (non-None) values in IN clause for column '{column}', falling back to post-filter"
                )
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


def _prepare_association_request(object_type: str, columns: List[str]) -> Tuple[List[str], List[str]]:
    assoc_columns = set(get_primary_association_columns(object_type))
    if not assoc_columns:
        return [], columns

    needs_associations = bool(assoc_columns.intersection(columns))
    if not needs_associations:
        return [], columns

    association_targets = get_association_targets_for_object(object_type)
    hubspot_columns = [col for col in columns if col not in assoc_columns]
    return association_targets, hubspot_columns


HUBSPOT_IN_MAX = 100


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
    Automatically chunks oversized IN filters (HubSpot max: 100 values).
    """
    logger.debug(f"[_execute_hubspot_search] called — object_type={object_type}, limit={limit}, filters={filters}")
    for i, f in enumerate(filters or []):
        if f.get("operator") in {"IN", "NOT_IN"} and len(f.get("values", [])) > HUBSPOT_IN_MAX:
            values = f["values"]
            chunks = [values[j : j + HUBSPOT_IN_MAX] for j in range(0, len(values), HUBSPOT_IN_MAX)]
            # When no explicit limit is provided, cap the number of chunks we process.
            # Without this cap, a caller that passes id IN [10000 ids] with limit=None
            # would fire 100+ sequential HubSpot API calls and run indefinitely.
            MAX_CHUNKS_WITHOUT_LIMIT = 10
            effective_limit = limit
            if limit is None and len(chunks) > MAX_CHUNKS_WITHOUT_LIMIT:
                effective_limit = MAX_CHUNKS_WITHOUT_LIMIT * HUBSPOT_IN_MAX
            collected: List[Dict[str, Any]] = []
            for chunk in chunks:
                if effective_limit is not None and len(collected) >= effective_limit:
                    break
                chunk_limit = effective_limit - len(collected) if effective_limit is not None else None
                chunked_filters = filters[:i] + [{**f, "values": chunk}] + filters[i + 1 :]
                collected.extend(
                    _execute_hubspot_search(
                        search_api,
                        chunked_filters,
                        properties,
                        chunk_limit,
                        to_dict_fn,
                        sorts,
                        object_type,
                    )
                )
            return collected

    collected: List[Dict[str, Any]] = []
    remaining = limit if limit is not None else float("inf")
    after = None
    page_num = 0

    while remaining > 0:
        page_num += 1
        page_limit = min(int(remaining) if remaining != float("inf") else 200, 200)
        logger.debug(
            f"[_execute_hubspot_search] page {page_num} — fetching up to {page_limit} results (after={after}, collected={len(collected)})"
        )
        search_request = {
            "limit": page_limit,
        }

        if properties:
            search_request["properties"] = properties

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

        logger.debug(f"[_execute_hubspot_search] page {page_num} — got {len(results)} results, after={after}")
        if after is None:
            break

        if remaining != float("inf"):
            remaining = limit - len(collected)

    logger.debug(f"[_execute_hubspot_search] done — total collected={len(collected)}")
    return collected


class HubSpotAPIResource(APIResource):
    """
    Base class for HubSpot table resources with custom select handling.

    Overrides the default select() method to properly handle server-side filtering
    and avoid double-filtering issues with AST nodes.
    """

    # Reference: https://developers.hubspot.com/docs/api-reference/search/guide
    SEARCHABLE_COLUMNS: Set[str] = set()

    # Aggregate function names → pandas equivalents
    _AGG_FUNC_MAP: Dict[str, str] = {
        "sum": "sum",
        "count": "count",
        "avg": "mean",
        "mean": "mean",
        "max": "max",
        "min": "min",
    }

    def select(self, query: ASTNode) -> pd.DataFrame:
        """Select data, applying WHERE, GROUP BY, ORDER BY, LIMIT and function evaluation."""
        
        conditions, order_by, result_limit = self._extract_query_params(query)
        group_by_cols = self._get_group_by_columns(query)
        # Targets include columns referenced inside functions and GROUP BY
        targets = self._get_targets(query)
        fetch_targets = list(dict.fromkeys(targets + group_by_cols))
        normalized_conditions = _normalize_filter_conditions(conditions)

        self._validate_query_columns(fetch_targets, normalized_conditions, order_by)

        for condition in normalized_conditions:
            if len(condition) >= 3 and condition[0] == "in":
                in_vals = condition[2] if isinstance(condition[2], list) else [condition[2]]
                if not in_vals or all(v is None for v in in_vals):
                    return pd.DataFrame(columns=fetch_targets or self.get_columns())

        if self.SEARCHABLE_COLUMNS:
            filters = (
                _build_hubspot_search_filters(normalized_conditions, self.SEARCHABLE_COLUMNS)
                if normalized_conditions
                else None
            )
            # Don't push ORDER BY to search when GROUP BY is present (need raw rows)
            sorts = (
                _build_hubspot_search_sorts(order_by, self.SEARCHABLE_COLUMNS)
                if order_by and not group_by_cols
                else None
            )
            use_search = filters is not None or sorts is not None
        else:
            filters = None
            sorts = None
            use_search = False

        fetch_columns = self._get_fetch_columns(
            targets=fetch_targets,
            normalized_conditions=normalized_conditions,
            order_by=order_by if not group_by_cols else [],
            use_search=use_search,
        )

        result = self.list(
            conditions=conditions if not use_search else None,
            limit=result_limit if not group_by_cols else None,
            sort=order_by if not use_search and not group_by_cols else None,
            targets=fetch_columns,
            search_filters=filters,
            search_sorts=sorts,
            allow_search=use_search,
        )

        # Post-filter for non-search queries
        if not use_search and normalized_conditions and not result.empty:
            result = self._apply_post_filter(result, normalized_conditions)

        # GROUP BY + aggregation
        if group_by_cols and not result.empty:
            result = self._apply_aggregation(result, query, group_by_cols)

        # ORDER BY (after aggregation so we can sort on aggregate columns)
        if order_by and not result.empty:
            result = self._apply_post_sort(result, order_by)

        # LIMIT (applied after aggregation/sort)
        if result_limit is not None and not result.empty:
            result = result.head(result_limit)

        return self._apply_column_selection(result, query.targets or [])

    def _extract_query_params(self, query: ASTNode) -> Tuple[List, List, Optional[int]]:
        """Extract conditions, order_by, and limit from query AST."""
        if query.where:
            try:
                conditions = extract_comparison_conditions(query.where)
            except NotImplementedError:
                conditions = _extract_comparison_conditions_with_functions(query.where)
        else:
            conditions = []

        order_by = []
        if query.order_by:

            def _extract_order_column(field: ASTNode) -> Optional[str]:
                if isinstance(field, sql_ast.Identifier):
                    return field.parts[-1]
                if isinstance(field, sql_ast.Function):
                    func = getattr(field, "op", None) or getattr(field, "name", None)
                    if func and str(func).lower() in {"lower", "upper"} and field.args:
                        if isinstance(field.args[0], sql_ast.Identifier):
                            return field.args[0].parts[-1]
                if hasattr(field, "args") and field.args:
                    last_arg = field.args[-1]
                    if isinstance(last_arg, sql_ast.Identifier):
                        return last_arg.parts[-1]
                return None

            for col in query.order_by:
                ascending = True
                if hasattr(col, "direction") and col.direction:
                    ascending = col.direction.upper() != "DESC"
                elif hasattr(col, "ascending"):
                    ascending = col.ascending
                column_name = _extract_order_column(col.field)
                if not column_name:
                    logger.debug(f"Skipping unsupported order by field: {col.field}")
                    continue
                order_by.append(SortColumn(column_name, ascending))

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
            except Exception:
                continue

        return df[mask].reset_index(drop=True)

    def _apply_post_sort(self, df: pd.DataFrame, sort: List[SortColumn]) -> pd.DataFrame:
        sort_columns = []
        sort_ascending = []
        for sort_item in sort:
            column = to_internal_property(sort_item.column)
            if column not in df.columns:
                continue
            sort_columns.append(column)
            sort_ascending.append(sort_item.ascending)

        if not sort_columns:
            return df

        try:
            return df.sort_values(by=sort_columns, ascending=sort_ascending).reset_index(drop=True)
        except Exception:
            return df

    def _apply_column_selection(self, df: pd.DataFrame, targets) -> pd.DataFrame:
        """Apply column selection, resolving AST target nodes and aliases."""
        if not targets or df.empty:
            return df

        def _alias_str(alias) -> Optional[str]:
            """Convert an alias (str or Identifier) to a plain string."""
            if alias is None:
                return None
            if isinstance(alias, str):
                return alias
            if isinstance(alias, sql_ast.Identifier):
                return alias.parts[-1]
            return str(alias)

        selected: List[str] = []
        for target in targets:
            if isinstance(target, sql_ast.Star):
                return df
            # AST node with an alias — the alias becomes the output column name
            alias = _alias_str(getattr(target, "alias", None))
            if alias and alias in df.columns:
                selected.append(alias)
                continue
            # Plain identifier
            if isinstance(target, sql_ast.Identifier):
                col = to_internal_property(target.parts[-1])
                if col in df.columns:
                    selected.append(col)
                continue
            # Function without alias — resolved agg column
            if isinstance(target, sql_ast.Function):
                func_name = (getattr(target, "op", None) or getattr(target, "name", "")).lower()
                inner_cols = self._extract_target_columns(target)
                candidate = f"{func_name}({inner_cols[0]})" if inner_cols else func_name
                if candidate in df.columns:
                    selected.append(candidate)
                elif inner_cols and inner_cols[0] in df.columns:
                    selected.append(inner_cols[0])
                continue
            # Plain string
            if isinstance(target, str) and target in df.columns:
                selected.append(target)

        selected = list(dict.fromkeys(selected))
        if selected:
            return df[selected]
        return df

    def _validate_query_columns(
        self,
        targets: List[str],
        normalized_conditions: List[List[Any]],
        order_by: List[SortColumn],
    ) -> None:
        # Names that are SQL aggregate/scalar functions — skip validation
        _FUNC_NAMES = {"sum", "count", "avg", "mean", "max", "min", "date_trunc", "lower", "upper", "coalesce"}

        requested = set()
        for col in targets or []:
            if col.lower() not in _FUNC_NAMES:
                requested.add(col)

        for condition in normalized_conditions:
            if len(condition) >= 2:
                col = condition[1]
                if col.lower() not in _FUNC_NAMES:
                    requested.add(col)

        for sort_item in order_by or []:
            col = to_internal_property(sort_item.column)
            if col.lower() not in _FUNC_NAMES:
                requested.add(col)

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

    def _get_group_by_columns(self, query: ASTNode) -> List[str]:
        """Extract GROUP BY column names from query AST."""
        if not query.group_by:
            return []
        cols = []
        for item in query.group_by:
            if isinstance(item, sql_ast.Identifier):
                cols.append(to_internal_property(item.parts[-1]))
            elif isinstance(item, sql_ast.Function):
                # e.g. DATE_TRUNC('month', closedate) — include the inner column
                inner_cols = self._extract_target_columns(item)
                cols.extend(inner_cols)
        return list(dict.fromkeys(cols))

    def _apply_aggregation(self, df: pd.DataFrame, query: ASTNode, group_by_cols: List[str]) -> pd.DataFrame:
        """Apply GROUP BY + aggregation to a DataFrame based on query targets."""
        if not group_by_cols:
            return df

        # Collect (input_col, agg_func, output_alias) triples from SELECT targets
        agg_specs: List[Tuple[str, str, str]] = []
        for target in query.targets or []:
            if not isinstance(target, sql_ast.Function):
                continue
            func_name = (getattr(target, "op", None) or getattr(target, "name", "")).lower()
            pandas_agg = self._AGG_FUNC_MAP.get(func_name)
            if pandas_agg is None:
                continue
            inner_cols = self._extract_target_columns(target)
            raw_alias = getattr(target, "alias", None)
            alias = (
                raw_alias.parts[-1]
                if isinstance(raw_alias, sql_ast.Identifier)
                else str(raw_alias)
                if raw_alias is not None and not isinstance(raw_alias, str)
                else raw_alias
            )
            if inner_cols:
                col = inner_cols[0]
                out_name = alias or f"{func_name}({col})"
                agg_specs.append((col, pandas_agg, out_name))
            elif func_name == "count":
                # COUNT(*) — use first group_by col as proxy
                out_name = alias or "count"
                agg_specs.append((group_by_cols[0], "count", out_name))

        if not agg_specs:
            # No aggregate functions — just deduplicate by group keys
            return df[group_by_cols].drop_duplicates().reset_index(drop=True)

        # Validate group-by columns exist
        valid_group_cols = [c for c in group_by_cols if c in df.columns]
        if not valid_group_cols:
            return df

        agg_dict: Dict[str, List[str]] = {}
        for col, pandas_agg, _ in agg_specs:
            if col in df.columns:
                agg_dict.setdefault(col, []).append(pandas_agg)

        if not agg_dict:
            return df[valid_group_cols].drop_duplicates().reset_index(drop=True)

        grouped = df.groupby(valid_group_cols, as_index=False).agg(agg_dict)
        # Flatten multi-level columns from groupby + agg
        if isinstance(grouped.columns, pd.MultiIndex):
            grouped.columns = ["_".join(filter(None, c)) for c in grouped.columns]

        # Rename output columns to requested aliases
        rename_map: Dict[str, str] = {}
        for col, pandas_agg, out_name in agg_specs:
            # pandas names the result column as col_agg in multi-level flattened case
            candidate = f"{col}_{pandas_agg}"
            if candidate in grouped.columns and candidate != out_name:
                rename_map[candidate] = out_name
            elif col in grouped.columns and col != out_name:
                rename_map[col] = out_name
        if rename_map:
            grouped = grouped.rename(columns=rename_map)

        return grouped.reset_index(drop=True)

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


class OwnersTable(HubSpotAPIResource):
    """HubSpot owners table."""

    SEARCHABLE_COLUMNS: Set[str] = set()

    def meta_get_tables(self, table_name: str) -> Dict[str, Any]:
        return {
            "TABLE_NAME": "owners",
            "TABLE_TYPE": "BASE TABLE",
            "TABLE_DESCRIPTION": "HubSpot owners with names and emails",
            "ROW_COUNT": None,
        }

    def meta_get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        return self.handler._get_default_meta_columns("owners")

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
        owners = self.get_owners(limit=limit)
        owners_df = pd.DataFrame(owners)
        if owners_df.empty:
            owners_df = pd.DataFrame(columns=targets or self._get_default_owner_columns())
        return owners_df

    def add(self, data: List[dict]) -> None:
        raise NotImplementedError("Creating owners via INSERT is not supported.")

    def modify(self, conditions: List[FilterCondition], values: Dict) -> None:
        raise NotImplementedError("Updating owners via UPDATE is not supported.")

    def remove(self, conditions: List[FilterCondition]) -> None:
        raise NotImplementedError("Deleting owners via DELETE is not supported.")

    def get_columns(self) -> List[str]:
        return self._get_default_owner_columns()

    @staticmethod
    def _get_default_owner_columns() -> List[str]:
        return [
            "id",
            "email",
            "first_name",
            "last_name",
            "full_name",
            "user_id",
            "teams",
            "created_at",
            "updated_at",
            "archived",
        ]

    def get_owners(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        owners = _get_owner_rows(self.handler)
        if limit is not None:
            return owners[:limit]
        return owners


class DealStagesTable(HubSpotAPIResource):
    """HubSpot deal pipeline stages table."""

    SEARCHABLE_COLUMNS: Set[str] = set()

    def meta_get_tables(self, table_name: str) -> Dict[str, Any]:
        return {
            "TABLE_NAME": "deal_stages",
            "TABLE_TYPE": "BASE TABLE",
            "TABLE_DESCRIPTION": "HubSpot deal pipeline stages with human-readable labels",
            "ROW_COUNT": None,
        }

    def meta_get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        return self.handler._get_default_meta_columns("deal_stages")

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
        stages = self.get_deal_stages(limit=limit)
        stages_df = pd.DataFrame(stages)
        if stages_df.empty:
            stages_df = pd.DataFrame(columns=targets or self._get_default_deal_stage_columns())
        return stages_df

    def add(self, data: List[dict]) -> None:
        raise NotImplementedError("Creating deal stages via INSERT is not supported.")

    def modify(self, conditions: List[FilterCondition], values: Dict) -> None:
        raise NotImplementedError("Updating deal stages via UPDATE is not supported.")

    def remove(self, conditions: List[FilterCondition]) -> None:
        raise NotImplementedError("Deleting deal stages via DELETE is not supported.")

    def get_columns(self) -> List[str]:
        return self._get_default_deal_stage_columns()

    @staticmethod
    def _get_default_deal_stage_columns() -> List[str]:
        return [
            "pipeline_id",
            "pipeline_label",
            "stage_id",
            "stage_label",
            "stage_order",
            "stage_probability",
            "stage_archived",
        ]

    def get_deal_stages(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        stages = _get_deal_stage_rows(self.handler)
        if limit is not None:
            return stages[:limit]
        return stages


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
        except Exception:
            pass

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

    def get_columns(self) -> List[str]:
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
        MAX_SCAN_ROWS = 10_000
        effective_limit = min(limit, MAX_SCAN_ROWS) if limit is not None else MAX_SCAN_ROWS
        for company in companies:
            try:
                companies_dict.append(self._company_to_dict(company, columns))
                if len(companies_dict) >= effective_limit:
                    break
            except Exception:
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

    def create_companies(self, companies_data: List[Dict[str, Any]]) -> None:
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

    def update_companies(self, company_ids: List[str], values_to_update: Dict[str, Any]) -> None:
        hubspot = self.handler.connect()
        companies_to_update = [HubSpotObjectBatchInput(id=cid, properties=values_to_update) for cid in company_ids]
        batch_input = BatchInputSimplePublicObjectBatchInput(inputs=companies_to_update)
        try:
            updated = hubspot.crm.companies.batch_api.update(batch_input_simple_public_object_batch_input=batch_input)
            logger.info(f"Companies with ID {[c.id for c in updated.results]} updated")
        except Exception as e:
            raise Exception(f"Companies update failed {e}")

    def delete_companies(self, company_ids: List[str]) -> None:
        hubspot = self.handler.connect()
        companies_to_delete = [HubSpotObjectId(id=cid) for cid in company_ids]
        batch_input = BatchInputSimplePublicObjectId(inputs=companies_to_delete)
        try:
            hubspot.crm.companies.batch_api.archive(batch_input_simple_public_object_id=batch_input)
            logger.info("Companies deleted")
        except Exception as e:
            raise Exception(f"Companies deletion failed {e}")


def _extract_association_condition(conditions: List[List[Any]], column: str) -> Optional[List[str]]:
    """
    Return a list of non-None string values if conditions contain an
    eq/in filter on *column*, otherwise return None.
    """
    for condition in conditions:
        if len(condition) >= 3 and condition[1] == column:
            op, val = condition[0], condition[2]
            if op == "eq" and val is not None:
                return [str(val)]
            if op == "in":
                vals = val if isinstance(val, list) else [val]
                valid = [str(v) for v in vals if v is not None]
                if valid:
                    return valid
    return None


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
        except Exception:
            pass

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

    def get_columns(self) -> List[str]:
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
            "primary_company_id",
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
        logger.debug(
            f"[ContactsTable] get_contacts() called — limit={limit}, conditions={where_conditions}, properties={properties}"
        )
        normalized_conditions = _normalize_filter_conditions(where_conditions)
        hubspot = self.handler.connect()
        requested_properties = properties or []
        default_properties = self._get_default_contact_columns()
        columns = requested_properties or default_properties
        association_targets, hubspot_columns = _prepare_association_request("contacts", columns)
        hubspot_properties = _build_hubspot_properties(hubspot_columns)
        logger.debug(
            f"[ContactsTable] get_contacts() — association_targets={association_targets}, hubspot_columns={hubspot_columns}"
        )

        # Optimization: if filtering by primary_company_id, bypass full contact
        # scan and use the associations API to fetch only the relevant contacts.
        company_ids = _extract_association_condition(normalized_conditions, "primary_company_id")
        if company_ids:
            logger.debug(
                f"[ContactsTable] get_contacts() — company_ids filter detected ({len(company_ids)} ids), using associations API"
            )
            return self._get_contacts_by_company_ids(
                hubspot, company_ids, hubspot_columns, hubspot_properties, columns, limit
            )

        # Guard: fetching association columns (primary_company_id) without any
        # WHERE filter means MindsDB's join executor is doing a full table scan
        # to resolve a direct FK join — e.g.:
        #   FROM contacts c JOIN companies co ON c.primary_company_id = co.id
        # MindsDB injects a large LIMIT (e.g. 20010) for its in-memory join
        # buffer, so we also block large-limit scans, not just limit=None.
        # Small explicit limits (≤ MAX_ASSOCIATION_SCAN) are allowed so that
        # a user can still browse a few contacts with their association columns.
        # Return empty immediately. Users should rewrite the query using the
        # appropriate association table, e.g.:
        #   FROM companies co
        #   JOIN company_contacts cc ON cc.company_id = co.id
        #   JOIN contacts c ON c.id = cc.contact_id
        MAX_ASSOCIATION_SCAN = 500
        if not normalized_conditions and limit is not None and limit > MAX_ASSOCIATION_SCAN:
            msg = (
                "Direct FK joins between HubSpot objects using foreign key columns "
                "(e.g. primary_company_id) are not supported. The HubSpot API represents "
                "relationships through association tables.\n\n"
                "Please rewrite your query using the association table, e.g.:\n"
                "SELECT c.firstname, c.lastname, c.email\n"
                "FROM companies co\n"
                "JOIN company_contacts cc ON cc.company_id = co.id\n"
                "JOIN contacts c ON c.id = cc.contact_id\n"
                "WHERE co.name = 'HubSpot'"
            )
            raise ValueError(msg)

        logger.debug(
            f"[ContactsTable] get_contacts() — proceeding with scan/search. allow_search={allow_search}, search_filters={search_filters}, normalized_conditions={normalized_conditions}"
        )
        api_kwargs = {**kwargs, "properties": hubspot_properties}
        if limit is not None:
            api_kwargs["limit"] = limit
        else:
            api_kwargs.pop("limit", None)
        if association_targets:
            api_kwargs["associations"] = association_targets

        if allow_search and (search_filters or search_sorts or normalized_conditions):
            filters = search_filters
            if filters is None and normalized_conditions:
                filters = _build_hubspot_search_filters(normalized_conditions, self.SEARCHABLE_COLUMNS)
            if filters is not None or search_sorts is not None:
                if association_targets:
                    logger.debug("HubSpot search API does not include associations for contacts.")
                logger.debug(
                    f"[ContactsTable] get_contacts() — calling _search_contacts_by_conditions with filters={filters}, limit={limit}"
                )
                search_results = self._search_contacts_by_conditions(
                    hubspot,
                    filters,
                    hubspot_properties,
                    limit,
                    search_sorts,
                    hubspot_columns,
                    association_targets,
                )
                logger.info(f"Retrieved {len(search_results)} contacts from HubSpot via search API")
                return search_results

        logger.debug(
            f"[ContactsTable] get_contacts() — falling back to full scan (get_all), effective_limit={limit if limit is not None else 10_000}"
        )
        contacts = hubspot.crm.contacts.get_all(**api_kwargs)
        contacts_dict = []
        # Without an explicit LIMIT MindsDB's join executor does not propagate the
        # outer LIMIT to sub-table queries, leading to unbounded full-table scans.
        # Cap the scan so that JOIN queries don't run indefinitely.  Queries that
        # genuinely need more than MAX_SCAN_ROWS rows should supply an explicit LIMIT.
        # Cap full scans at MAX_SCAN_ROWS regardless of whether limit was explicit.
        # MindsDB's in-memory join executor injects large limits (e.g. 20010, 20000)
        # for its join buffer, causing multi-minute full table scans.  Applying min()
        # ensures those injected limits are treated the same as limit=None.
        MAX_SCAN_ROWS = 10_000
        effective_limit = min(limit, MAX_SCAN_ROWS) if limit is not None else MAX_SCAN_ROWS
        logger.debug(f"[ContactsTable] get_contacts() — full scan capped at {effective_limit}")
        try:
            for contact in contacts:
                row = self._contact_to_dict(contact, hubspot_columns, association_targets)
                contacts_dict.append(row)
                if effective_limit is not None and len(contacts_dict) >= effective_limit:
                    logger.debug(
                        f"[ContactsTable] get_contacts() — reached effective_limit={effective_limit}, stopping scan"
                    )
                    break
        except Exception as e:
            logger.error(f"Failed to iterate HubSpot contacts: {str(e)}")
            raise

        logger.info(f"Retrieved {len(contacts_dict)} contacts from HubSpot")
        return contacts_dict

    def _get_contacts_by_company_ids(
        self,
        hubspot: HubSpot,
        company_ids: List[str],
        hubspot_columns: List[str],
        hubspot_properties: List[str],
        columns: List[str],
        limit: Optional[int],
    ) -> List[Dict[str, Any]]:
        """
        Fetch contacts that are associated with the given company IDs using
        HubSpot's batch associations API + batch read, instead of scanning
        all contacts (~100x faster for large accounts).
        """
        logger.debug(
            f"[ContactsTable] _get_contacts_by_company_ids() called — company_ids={company_ids}, limit={limit}"
        )
        BATCH = 100
        # contact_id -> company_id (first company that referenced this contact)
        contact_company_map: Dict[str, str] = {}

        for i in range(0, len(company_ids), BATCH):
            chunk = company_ids[i : i + BATCH]
            try:
                resp = hubspot.crm.associations.batch_api.read(
                    "companies",
                    "contacts",
                    BatchInputPublicObjectId(inputs=[PublicObjectId(id=cid) for cid in chunk]),
                )
                for multi in resp.results or []:
                    from_id = str(
                        (multi._from or {}).get("id", "")
                        if isinstance(multi._from, dict)
                        else getattr(multi._from, "id", "")
                    )
                    for assoc in multi.to or []:
                        cid = str(assoc.id)
                        if cid not in contact_company_map:
                            contact_company_map[cid] = from_id
            except Exception:
                pass

        if not contact_company_map:
            logger.debug(
                "[ContactsTable] _get_contacts_by_company_ids() — no contacts found for given company_ids, returning []"
            )
            return []

        logger.debug(
            f"[ContactsTable] _get_contacts_by_company_ids() — found {len(contact_company_map)} contact ids from associations API"
        )
        all_contact_ids = list(contact_company_map.keys())
        if limit is not None:
            all_contact_ids = all_contact_ids[:limit]

        contacts_dict = []
        for i in range(0, len(all_contact_ids), BATCH):
            batch_ids = all_contact_ids[i : i + BATCH]
            try:
                resp = hubspot.crm.contacts.batch_api.read(
                    batch_read_input_simple_public_object_id=BatchReadInputSimplePublicObjectId(
                        properties=hubspot_properties,
                        inputs=[ContactObjectId(id=cid) for cid in batch_ids],
                    )
                )
                for contact in resp.results or []:
                    row = self._contact_to_dict(contact, hubspot_columns, None)
                    row["primary_company_id"] = contact_company_map.get(str(contact.id))
                    contacts_dict.append(row)
            except Exception as e:
                logger.error(f"Failed to batch read contacts: {e}")
                raise

        logger.info(f"Retrieved {len(contacts_dict)} contacts via company associations API")
        return contacts_dict

    def _search_contacts_by_conditions(
        self,
        hubspot: HubSpot,
        filters: Optional[List[Dict[str, Any]]],
        properties: List[str],
        limit: Optional[int],
        sorts: Optional[List[Dict[str, Any]]],
        columns: List[str],
        association_targets: List[str],
    ) -> List[Dict[str, Any]]:
        logger.debug(
            f"[ContactsTable] _search_contacts_by_conditions() called — filters={filters}, limit={limit}, sorts={sorts}"
        )
        return _execute_hubspot_search(
            hubspot.crm.contacts.search_api,
            filters or [],
            properties,
            limit,
            lambda obj: self._contact_to_dict(obj, columns, association_targets),
            sorts=sorts,
        )

    def _contact_to_dict(
        self,
        contact: Any,
        columns: Optional[List[str]] = None,
        association_targets: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        columns = columns or self._get_default_contact_columns()
        try:
            row = self._object_to_dict(contact, columns)
            if association_targets:
                row = enrich_object_with_associations(contact, "contacts", row)
            return row
        except Exception:
            assoc_columns = get_primary_association_columns("contacts") if association_targets else []
            return {
                "id": getattr(contact, "id", None),
                **{col: None for col in columns if col != "id"},
                **{col: None for col in assoc_columns},
            }

    def create_contacts(self, contacts_data: List[Dict[str, Any]]) -> None:
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

    def update_contacts(self, contact_ids: List[str], values_to_update: Dict[str, Any]) -> None:
        hubspot = self.handler.connect()
        contacts_to_update = [HubSpotObjectBatchInput(id=cid, properties=values_to_update) for cid in contact_ids]
        batch_input = BatchInputSimplePublicObjectBatchInput(inputs=contacts_to_update)
        try:
            updated = hubspot.crm.contacts.batch_api.update(batch_input_simple_public_object_batch_input=batch_input)
            logger.info(f"Contacts with ID {[c.id for c in updated.results]} updated")
        except Exception as e:
            raise Exception(f"Contacts update failed {e}")

    def delete_contacts(self, contact_ids: List[str]) -> None:
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
    ASSOCIATION_COLUMNS = {"primary_company_id", "primary_contact_id"}
    # Additional columns that require fetching extra data and better mapping
    OWNER_COLUMNS = {"owner_name", "owner_email"}
    STAGE_COLUMNS = {"dealstage_label", "pipeline_label"}
    VIRTUAL_COLUMNS = OWNER_COLUMNS | STAGE_COLUMNS

    def meta_get_tables(self, table_name: str) -> Dict[str, Any]:
        row_count = None
        try:
            self.handler.connect()
            row_count = self.handler._estimate_table_rows("deals")
        except Exception:
            pass

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

    def get_columns(self) -> List[str]:
        return self._get_default_deal_columns()

    @staticmethod
    def _get_default_deal_columns() -> List[str]:
        return [
            "id",
            "dealname",
            "amount",
            "primary_company_id",
            "primary_contact_id",
            "pipeline",
            "pipeline_label",
            "closedate",
            "dealstage",
            "dealstage_label",
            "hubspot_owner_id",
            "owner_name",
            "owner_email",
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

    def _needs_owner_details(self, columns: List[str]) -> bool:
        return bool(self.OWNER_COLUMNS.intersection(columns))

    def _needs_stage_details(self, columns: List[str]) -> bool:
        return bool(self.STAGE_COLUMNS.intersection(columns))

    def _add_virtual_dependencies(self, columns: List[str]) -> List[str]:
        normalized = list(dict.fromkeys(columns))
        if self._needs_owner_details(normalized) and "hubspot_owner_id" not in normalized:
            normalized.append("hubspot_owner_id")
        if self._needs_stage_details(normalized):
            if "dealstage" not in normalized:
                normalized.append("dealstage")
            if "pipeline" not in normalized:
                normalized.append("pipeline")
        return normalized

    def _strip_virtual_columns(self, columns: List[str]) -> List[str]:
        return [col for col in columns if col not in self.VIRTUAL_COLUMNS]

    def _get_fetch_columns(
        self,
        targets: List[str],
        normalized_conditions: List[List[Any]],
        order_by: List[SortColumn],
        use_search: bool,
    ) -> List[str]:
        base_columns = super()._get_fetch_columns(targets, normalized_conditions, order_by, use_search)

        if targets:
            if "dealstage" in targets and "dealstage_label" not in base_columns:
                base_columns.append("dealstage_label")
            if "pipeline" in targets and "pipeline_label" not in base_columns:
                base_columns.append("pipeline_label")
            if "hubspot_owner_id" in targets and "owner_name" not in base_columns:
                base_columns.append("owner_name")

        return list(dict.fromkeys(base_columns))

    def _apply_column_selection(self, df: pd.DataFrame, targets: List[str]) -> pd.DataFrame:
        if df.empty or not targets:
            return df

        df = df.copy()

        # Try to use the enriched labels/names for better readability
        # TODO: check for better way to handle this without modifying original columns
        if "dealstage" in targets and "dealstage_label" in df.columns and "dealstage_label" not in targets:
            df["dealstage"] = df["dealstage_label"].combine_first(df["dealstage"])

        if "pipeline" in targets and "pipeline_label" in df.columns and "pipeline_label" not in targets:
            df["pipeline"] = df["pipeline_label"].combine_first(df["pipeline"])

        if "hubspot_owner_id" in targets and "owner_name" in df.columns and "owner_name" not in targets:
            df["hubspot_owner_id"] = df["owner_name"].combine_first(df["hubspot_owner_id"])

        return super()._apply_column_selection(df, targets)

    def _enrich_deal_rows(self, rows: List[Dict[str, Any]], columns: List[str]) -> None:
        if not rows:
            return

        needs_owner = self._needs_owner_details(columns)
        needs_stage = self._needs_stage_details(columns)
        if not needs_owner and not needs_stage:
            return

        owner_map = _get_owner_map(self.handler) if needs_owner else None
        stage_pair_map: Dict[Tuple[str, str], Dict[str, Any]] = {}
        stage_map: Dict[str, Dict[str, Any]] = {}
        if needs_stage:
            stage_pair_map, stage_map = _get_deal_stage_maps(self.handler)

        for row in rows:
            if needs_owner:
                owner_id = _as_str(row.get("hubspot_owner_id"))
                owner = owner_map.get(owner_id) if owner_id else None
                row["owner_name"] = owner.get("full_name") if owner else None
                row["owner_email"] = owner.get("email") if owner else None

            if needs_stage:
                pipeline_id = _as_str(row.get("pipeline"))
                stage_id = _as_str(row.get("dealstage"))
                stage_info = None
                if pipeline_id and stage_id:
                    stage_info = stage_pair_map.get((pipeline_id, stage_id))
                if stage_info is None and stage_id:
                    stage_info = stage_map.get(stage_id)
                row["pipeline_label"] = stage_info.get("pipeline_label") if stage_info else None
                row["dealstage_label"] = stage_info.get("stage_label") if stage_info else None

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
        columns = self._add_virtual_dependencies(requested_properties or default_properties)
        needs_owner = self._needs_owner_details(columns)
        needs_stage = self._needs_stage_details(columns)
        association_targets, hubspot_columns = _prepare_association_request("deals", columns)
        hubspot_columns = self._strip_virtual_columns(hubspot_columns)
        hubspot_properties = _build_hubspot_properties(hubspot_columns)

        MAX_ASSOCIATION_SCAN = 500
        if not normalized_conditions and limit is not None and limit > MAX_ASSOCIATION_SCAN:
            msg = (
                "Direct FK joins on HubSpot deals using foreign key columns "
                "(e.g. primary_company_id, primary_contact_id) are not supported. "
                "The HubSpot API represents relationships through association tables.\n\n"
                "Please rewrite your query using the appropriate association table, e.g.:\n"
                "FROM companies co\n"
                "JOIN company_deals cd ON cd.company_id = co.id\n"
                "JOIN deals d ON d.id = cd.deal_id"
            )
            raise ValueError(msg)

        api_kwargs = {**kwargs, "properties": hubspot_properties}
        if limit is not None:
            api_kwargs["limit"] = limit
        else:
            api_kwargs.pop("limit", None)
        if association_targets:
            api_kwargs["associations"] = association_targets

        if allow_search and (search_filters or search_sorts or normalized_conditions):
            filters = search_filters
            if filters is None and normalized_conditions:
                filters = _build_hubspot_search_filters(normalized_conditions, self.SEARCHABLE_COLUMNS)
            if filters is not None or search_sorts is not None:
                if association_targets:
                    logger.debug("HubSpot search API does not include associations for deals.")
                search_results = self._search_deals_by_conditions(
                    hubspot,
                    filters,
                    hubspot_properties,
                    limit,
                    search_sorts,
                    hubspot_columns,
                    association_targets,
                )
                if needs_owner or needs_stage:
                    self._enrich_deal_rows(search_results, columns)
                logger.info(f"Retrieved {len(search_results)} deals from HubSpot via search API")
                return search_results

        deals = hubspot.crm.deals.get_all(**api_kwargs)
        deals_dict = []
        MAX_SCAN_ROWS = 10_000
        effective_limit = min(limit, MAX_SCAN_ROWS) if limit is not None else MAX_SCAN_ROWS
        logger.debug(f"[DealsTable] get_deals() — full scan capped at {effective_limit}")
        for deal in deals:
            try:
                row = self._deal_to_dict(deal, hubspot_columns, association_targets)
                deals_dict.append(row)
                if len(deals_dict) >= effective_limit:
                    logger.debug(f"[DealsTable] get_deals() — reached effective_limit={effective_limit}, stopping scan")
                    break
            except Exception as e:
                logger.error(f"Error processing deal {getattr(deal, 'id', 'unknown')}: {str(e)}")
                raise ValueError(f"Failed to process deal {getattr(deal, 'id', 'unknown')}.") from e

        if needs_owner or needs_stage:
            self._enrich_deal_rows(deals_dict, columns)
        logger.info(f"Retrieved {len(deals_dict)} deals from HubSpot")
        return deals_dict

    def _search_deals_by_conditions(
        self,
        hubspot: HubSpot,
        filters: Optional[List[Dict[str, Any]]],
        properties: List[str],
        limit: Optional[int],
        sorts: Optional[List[Dict[str, Any]]],
        hubspot_columns: List[str],
        association_targets: List[str],
    ) -> List[Dict[str, Any]]:
        return _execute_hubspot_search(
            hubspot.crm.deals.search_api,
            filters or [],
            properties,
            limit,
            lambda obj: self._deal_to_dict(obj, hubspot_columns, association_targets),
            sorts=sorts,
        )

    def _deal_to_dict(
        self,
        deal: Any,
        columns: Optional[List[str]] = None,
        association_targets: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        columns = columns or self._get_default_deal_columns()
        row = self._object_to_dict(deal, columns)
        if association_targets:
            row = enrich_object_with_associations(deal, "deals", row)
        return row

    def create_deals(self, deals_data: List[Dict[str, Any]]) -> None:
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

    def update_deals(self, deal_ids: List[str], values_to_update: Dict[str, Any]) -> None:
        hubspot = self.handler.connect()
        deals_to_update = [HubSpotObjectBatchInput(id=did, properties=values_to_update) for did in deal_ids]
        batch_input = BatchInputSimplePublicObjectBatchInput(inputs=deals_to_update)
        try:
            updated = hubspot.crm.deals.batch_api.update(batch_input_simple_public_object_batch_input=batch_input)
            logger.info(f"Deals with ID {[d.id for d in updated.results]} updated")
        except Exception as e:
            raise Exception(f"Deals update failed {e}")

    def delete_deals(self, deal_ids: List[str]) -> None:
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
    ASSOCIATION_COLUMNS = {"primary_company_id", "primary_contact_id", "primary_deal_id"}

    def meta_get_tables(self, table_name: str) -> Dict[str, Any]:
        row_count = None
        try:
            self.handler.connect()
            row_count = self.handler._estimate_table_rows("tickets")
        except Exception:
            pass

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

    def get_columns(self) -> List[str]:
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
            "primary_company_id",
            "primary_contact_id",
            "primary_deal_id",
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
        association_targets, hubspot_columns = _prepare_association_request("tickets", columns)
        hubspot_properties = _build_hubspot_properties(hubspot_columns)

        MAX_ASSOCIATION_SCAN = 500
        if not normalized_conditions and limit is not None and limit > MAX_ASSOCIATION_SCAN:
            msg = (
                "Direct FK joins on HubSpot tickets using foreign key columns "
                "(e.g. primary_company_id, primary_contact_id, primary_deal_id) are not supported. "
                "The HubSpot API represents relationships through association tables.\n\n"
                "Please rewrite your query using the appropriate association table, e.g.:\n"
                "FROM companies co\n"
                "JOIN company_tickets ct ON ct.company_id = co.id\n"
                "JOIN tickets t ON t.id = ct.ticket_id"
            )
            raise ValueError(msg)

        api_kwargs = {**kwargs, "properties": hubspot_properties}
        if limit is not None:
            api_kwargs["limit"] = limit
        else:
            api_kwargs.pop("limit", None)
        if association_targets:
            api_kwargs["associations"] = association_targets

        if allow_search and (search_filters or search_sorts or normalized_conditions):
            filters = search_filters
            if filters is None and normalized_conditions:
                filters = _build_hubspot_search_filters(normalized_conditions, self.SEARCHABLE_COLUMNS)
            if filters is not None or search_sorts is not None:
                if association_targets:
                    logger.debug("HubSpot search API does not include associations for tickets.")
                search_results = self._search_tickets_by_conditions(
                    hubspot,
                    filters,
                    hubspot_properties,
                    limit,
                    search_sorts,
                    hubspot_columns,
                    association_targets,
                )
                logger.info(f"Retrieved {len(search_results)} tickets from HubSpot via search API")
                return search_results

        tickets = hubspot.crm.tickets.get_all(**api_kwargs)
        tickets_dict = []
        MAX_SCAN_ROWS = 10_000
        effective_limit = min(limit, MAX_SCAN_ROWS) if limit is not None else MAX_SCAN_ROWS
        for ticket in tickets:
            try:
                row = self._ticket_to_dict(ticket, hubspot_columns, association_targets)
                tickets_dict.append(row)
                if len(tickets_dict) >= effective_limit:
                    break
            except Exception:
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
        association_targets: List[str],
    ) -> List[Dict[str, Any]]:
        return _execute_hubspot_search(
            hubspot.crm.tickets.search_api,
            filters or [],
            properties,
            limit,
            lambda obj: self._ticket_to_dict(obj, columns, association_targets),
            sorts=sorts,
        )

    def _ticket_to_dict(
        self,
        ticket: Any,
        columns: Optional[List[str]] = None,
        association_targets: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        columns = columns or self._get_default_ticket_columns()
        row = self._object_to_dict(ticket, columns)
        if association_targets:
            row = enrich_object_with_associations(ticket, "tickets", row)
        return row

    def create_tickets(self, tickets_data: List[Dict[str, Any]]) -> None:
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

    def update_tickets(self, ticket_ids: List[str], values_to_update: Dict[str, Any]) -> None:
        hubspot = self.handler.connect()
        tickets_to_update = [HubSpotObjectBatchInput(id=tid, properties=values_to_update) for tid in ticket_ids]
        batch_input = BatchInputSimplePublicObjectBatchInput(inputs=tickets_to_update)
        try:
            updated = hubspot.crm.tickets.batch_api.update(batch_input_simple_public_object_batch_input=batch_input)
            logger.info(f"Tickets with ID {[t.id for t in updated.results]} updated")
        except Exception as e:
            raise Exception(f"Tickets update failed {e}")

    def delete_tickets(self, ticket_ids: List[str]) -> None:
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
    ASSOCIATION_COLUMNS = {"primary_company_id", "primary_contact_id", "primary_deal_id"}

    def meta_get_tables(self, table_name: str) -> Dict[str, Any]:
        row_count = None
        try:
            self.handler.connect()
            row_count = self.handler._estimate_table_rows("tasks")
        except Exception:
            pass

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

    def get_columns(self) -> List[str]:
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
            "primary_company_id",
            "primary_contact_id",
            "primary_deal_id",
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
        association_targets, hubspot_columns = _prepare_association_request("tasks", columns)
        hubspot_properties = _build_hubspot_properties(hubspot_columns)

        MAX_ASSOCIATION_SCAN = 500
        if not normalized_conditions and limit is not None and limit > MAX_ASSOCIATION_SCAN:
            msg = (
                "Direct FK joins on HubSpot tasks using foreign key columns "
                "(e.g. primary_contact_id, primary_company_id, primary_deal_id) are not supported. "
                "The HubSpot API represents relationships through association tables.\n\n"
                "Please rewrite your query using the appropriate association table."
            )
            raise ValueError(msg)

        api_kwargs = {**kwargs, "properties": hubspot_properties}
        if limit is not None:
            api_kwargs["limit"] = limit
        else:
            api_kwargs.pop("limit", None)
        if association_targets:
            api_kwargs["associations"] = association_targets

        # Tasks use the objects API
        if allow_search and (search_filters or search_sorts or normalized_conditions):
            filters = search_filters
            if filters is None and normalized_conditions:
                filters = _build_hubspot_search_filters(normalized_conditions, self.SEARCHABLE_COLUMNS)
            if filters is not None or search_sorts is not None:
                if association_targets:
                    logger.debug("HubSpot search API does not include associations for tasks.")
                search_results = self._search_tasks_by_conditions(
                    hubspot,
                    filters,
                    hubspot_properties,
                    limit,
                    search_sorts,
                    hubspot_columns,
                    association_targets,
                )
                logger.info(f"Retrieved {len(search_results)} tasks from HubSpot via search API")
                return search_results

        tasks = self.handler._get_objects_all("tasks", **api_kwargs)
        tasks_dict = []
        MAX_SCAN_ROWS = 10_000
        effective_limit = min(limit, MAX_SCAN_ROWS) if limit is not None else MAX_SCAN_ROWS
        for task in tasks:
            try:
                row = self._task_to_dict(task, hubspot_columns, association_targets)
                tasks_dict.append(row)
                if len(tasks_dict) >= effective_limit:
                    break
            except Exception:
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
        association_targets: List[str],
    ) -> List[Dict[str, Any]]:
        return _execute_hubspot_search(
            hubspot.crm.objects.search_api,
            filters or [],
            properties,
            limit,
            lambda obj: self._task_to_dict(obj, columns, association_targets),
            sorts=sorts,
            object_type="tasks",
        )

    def _task_to_dict(
        self,
        task: Any,
        columns: Optional[List[str]] = None,
        association_targets: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        columns = columns or self._get_default_task_columns()
        row = self._object_to_dict(task, columns)
        if association_targets:
            row = enrich_object_with_associations(task, "tasks", row)
        return row

    def create_tasks(self, tasks_data: List[Dict[str, Any]]) -> None:
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

    def update_tasks(self, task_ids: List[str], values_to_update: Dict[str, Any]) -> None:
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

    def delete_tasks(self, task_ids: List[str]) -> None:
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
    ASSOCIATION_COLUMNS = {"primary_company_id", "primary_contact_id", "primary_deal_id"}

    def meta_get_tables(self, table_name: str) -> Dict[str, Any]:
        row_count = None
        try:
            self.handler.connect()
            row_count = self.handler._estimate_table_rows("calls")
        except Exception:
            pass

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

    def get_columns(self) -> List[str]:
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
            "primary_company_id",
            "primary_contact_id",
            "primary_deal_id",
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
        association_targets, hubspot_columns = _prepare_association_request("calls", columns)
        hubspot_properties = _build_hubspot_properties(hubspot_columns)

        MAX_ASSOCIATION_SCAN = 500
        if not normalized_conditions and limit is not None and limit > MAX_ASSOCIATION_SCAN:
            msg = (
                "Direct FK joins on HubSpot calls using foreign key columns "
                "(e.g. primary_contact_id, primary_company_id, primary_deal_id) are not supported. "
                "The HubSpot API represents relationships through association tables.\n\n"
                "Please rewrite your query using the appropriate association table."
            )
            raise ValueError(msg)

        api_kwargs = {**kwargs, "properties": hubspot_properties}
        if limit is not None:
            api_kwargs["limit"] = limit
        else:
            api_kwargs.pop("limit", None)
        if association_targets:
            api_kwargs["associations"] = association_targets

        if allow_search and (search_filters or search_sorts or normalized_conditions):
            filters = search_filters
            if filters is None and normalized_conditions:
                filters = _build_hubspot_search_filters(normalized_conditions, self.SEARCHABLE_COLUMNS)
            if filters is not None or search_sorts is not None:
                if association_targets:
                    logger.debug("HubSpot search API does not include associations for calls.")
                search_results = self._search_calls_by_conditions(
                    hubspot,
                    filters,
                    hubspot_properties,
                    limit,
                    search_sorts,
                    hubspot_columns,
                    association_targets,
                )
                logger.info(f"Retrieved {len(search_results)} calls from HubSpot via search API")
                return search_results

        calls = self.handler._get_objects_all("calls", **api_kwargs)
        calls_dict = []
        MAX_SCAN_ROWS = 10_000
        effective_limit = min(limit, MAX_SCAN_ROWS) if limit is not None else MAX_SCAN_ROWS
        for call in calls:
            try:
                row = self._call_to_dict(call, hubspot_columns, association_targets)
                calls_dict.append(row)
                if len(calls_dict) >= effective_limit:
                    break
            except Exception:
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
        association_targets: List[str],
    ) -> List[Dict[str, Any]]:
        return _execute_hubspot_search(
            hubspot.crm.objects.search_api,
            filters or [],
            properties,
            limit,
            lambda obj: self._call_to_dict(obj, columns, association_targets),
            sorts=sorts,
            object_type="calls",
        )

    def _call_to_dict(
        self,
        call: Any,
        columns: Optional[List[str]] = None,
        association_targets: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        columns = columns or self._get_default_call_columns()
        row = self._object_to_dict(call, columns)
        if association_targets:
            row = enrich_object_with_associations(call, "calls", row)
        return row

    def create_calls(self, calls_data: List[Dict[str, Any]]) -> None:
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

    def update_calls(self, call_ids: List[str], values_to_update: Dict[str, Any]) -> None:
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

    def delete_calls(self, call_ids: List[str]) -> None:
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
    ASSOCIATION_COLUMNS = {"primary_company_id", "primary_contact_id", "primary_deal_id"}

    def meta_get_tables(self, table_name: str) -> Dict[str, Any]:
        row_count = None
        try:
            self.handler.connect()
            row_count = self.handler._estimate_table_rows("emails")
        except Exception:
            pass

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

    def get_columns(self) -> List[str]:
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
            "primary_company_id",
            "primary_contact_id",
            "primary_deal_id",
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
        association_targets, hubspot_columns = _prepare_association_request("emails", columns)
        hubspot_properties = _build_hubspot_properties(hubspot_columns)

        MAX_ASSOCIATION_SCAN = 500
        if not normalized_conditions and limit is not None and limit > MAX_ASSOCIATION_SCAN:
            msg = (
                "Direct FK joins on HubSpot emails using foreign key columns "
                "(e.g. primary_contact_id, primary_company_id, primary_deal_id) are not supported. "
                "The HubSpot API represents relationships through association tables.\n\n"
                "Please rewrite your query using the appropriate association table."
            )
            raise ValueError(msg)

        api_kwargs = {**kwargs, "properties": hubspot_properties}
        if limit is not None:
            api_kwargs["limit"] = limit
        else:
            api_kwargs.pop("limit", None)
        if association_targets:
            api_kwargs["associations"] = association_targets

        if allow_search and (search_filters or search_sorts or normalized_conditions):
            filters = search_filters
            if filters is None and normalized_conditions:
                filters = _build_hubspot_search_filters(normalized_conditions, self.SEARCHABLE_COLUMNS)
            if filters is not None or search_sorts is not None:
                if association_targets:
                    logger.debug("HubSpot search API does not include associations for emails.")
                search_results = self._search_emails_by_conditions(
                    hubspot,
                    filters,
                    hubspot_properties,
                    limit,
                    search_sorts,
                    hubspot_columns,
                    association_targets,
                )
                logger.info(f"Retrieved {len(search_results)} emails from HubSpot via search API")
                return search_results

        emails = self.handler._get_objects_all("emails", **api_kwargs)
        emails_dict = []
        MAX_SCAN_ROWS = 10_000
        effective_limit = min(limit, MAX_SCAN_ROWS) if limit is not None else MAX_SCAN_ROWS
        for email in emails:
            try:
                row = self._email_to_dict(email, hubspot_columns, association_targets)
                emails_dict.append(row)
                if len(emails_dict) >= effective_limit:
                    break
            except Exception:
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
        association_targets: List[str],
    ) -> List[Dict[str, Any]]:
        return _execute_hubspot_search(
            hubspot.crm.objects.search_api,
            filters or [],
            properties,
            limit,
            lambda obj: self._email_to_dict(obj, columns, association_targets),
            sorts=sorts,
            object_type="emails",
        )

    def _email_to_dict(
        self,
        email: Any,
        columns: Optional[List[str]] = None,
        association_targets: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        columns = columns or self._get_default_email_columns()
        row = self._object_to_dict(email, columns)
        if association_targets:
            row = enrich_object_with_associations(email, "emails", row)
        return row

    def create_emails(self, emails_data: List[Dict[str, Any]]) -> None:
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

    def update_emails(self, email_ids: List[str], values_to_update: Dict[str, Any]) -> None:
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

    def delete_emails(self, email_ids: List[str]) -> None:
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
    ASSOCIATION_COLUMNS = {"primary_company_id", "primary_contact_id", "primary_deal_id"}

    def meta_get_tables(self, table_name: str) -> Dict[str, Any]:
        row_count = None
        try:
            self.handler.connect()
            row_count = self.handler._estimate_table_rows("meetings")
        except Exception:
            pass

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

    def get_columns(self) -> List[str]:
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
            "primary_company_id",
            "primary_contact_id",
            "primary_deal_id",
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
        association_targets, hubspot_columns = _prepare_association_request("meetings", columns)
        hubspot_properties = _build_hubspot_properties(hubspot_columns)

        MAX_ASSOCIATION_SCAN = 500
        if not normalized_conditions and limit is not None and limit > MAX_ASSOCIATION_SCAN:
            msg = (
                "Direct FK joins on HubSpot meetings using foreign key columns "
                "(e.g. primary_contact_id, primary_company_id, primary_deal_id) are not supported. "
                "The HubSpot API represents relationships through association tables.\n\n"
                "Please rewrite your query using the appropriate association table."
            )
            raise ValueError(msg)

        api_kwargs = {**kwargs, "properties": hubspot_properties}
        if limit is not None:
            api_kwargs["limit"] = limit
        else:
            api_kwargs.pop("limit", None)
        if association_targets:
            api_kwargs["associations"] = association_targets

        if allow_search and (search_filters or search_sorts or normalized_conditions):
            filters = search_filters
            if filters is None and normalized_conditions:
                filters = _build_hubspot_search_filters(normalized_conditions, self.SEARCHABLE_COLUMNS)
            if filters is not None or search_sorts is not None:
                if association_targets:
                    logger.debug("HubSpot search API does not include associations for meetings.")
                search_results = self._search_meetings_by_conditions(
                    hubspot,
                    filters,
                    hubspot_properties,
                    limit,
                    search_sorts,
                    hubspot_columns,
                    association_targets,
                )
                logger.info(f"Retrieved {len(search_results)} meetings from HubSpot via search API")
                return search_results

        meetings = self.handler._get_objects_all("meetings", **api_kwargs)
        meetings_dict = []
        MAX_SCAN_ROWS = 10_000
        effective_limit = min(limit, MAX_SCAN_ROWS) if limit is not None else MAX_SCAN_ROWS
        for meeting in meetings:
            try:
                row = self._meeting_to_dict(meeting, hubspot_columns, association_targets)
                meetings_dict.append(row)
                if len(meetings_dict) >= effective_limit:
                    break
            except Exception:
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
        association_targets: List[str],
    ) -> List[Dict[str, Any]]:
        return _execute_hubspot_search(
            hubspot.crm.objects.search_api,
            filters or [],
            properties,
            limit,
            lambda obj: self._meeting_to_dict(obj, columns, association_targets),
            sorts=sorts,
            object_type="meetings",
        )

    def _meeting_to_dict(
        self,
        meeting: Any,
        columns: Optional[List[str]] = None,
        association_targets: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        columns = columns or self._get_default_meeting_columns()
        row = self._object_to_dict(meeting, columns)
        if association_targets:
            row = enrich_object_with_associations(meeting, "meetings", row)
        return row

    def create_meetings(self, meetings_data: List[Dict[str, Any]]) -> None:
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

    def update_meetings(self, meeting_ids: List[str], values_to_update: Dict[str, Any]) -> None:
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

    def delete_meetings(self, meeting_ids: List[str]) -> None:
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
    ASSOCIATION_COLUMNS = {"primary_company_id", "primary_contact_id", "primary_deal_id"}

    def meta_get_tables(self, table_name: str) -> Dict[str, Any]:
        row_count = None
        try:
            self.handler.connect()
            row_count = self.handler._estimate_table_rows("notes")
        except Exception:
            pass

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

    def get_columns(self) -> List[str]:
        return self._get_default_note_columns()

    @staticmethod
    def _get_default_note_columns() -> List[str]:
        return [
            "id",
            "hs_note_body",
            "hubspot_owner_id",
            "hs_timestamp",
            "createdate",
            "lastmodifieddate",
            "primary_company_id",
            "primary_contact_id",
            "primary_deal_id",
        ]

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
        association_targets, hubspot_columns = _prepare_association_request("notes", columns)
        hubspot_properties = _build_hubspot_properties(hubspot_columns)

        MAX_ASSOCIATION_SCAN = 500
        if not normalized_conditions and limit is not None and limit > MAX_ASSOCIATION_SCAN:
            msg = (
                "Direct FK joins on HubSpot notes using foreign key columns "
                "(e.g. primary_contact_id, primary_company_id, primary_deal_id) are not supported. "
                "The HubSpot API represents relationships through association tables.\n\n"
                "Please rewrite your query using the appropriate association table."
            )
            raise ValueError(msg)

        api_kwargs = {**kwargs, "properties": hubspot_properties}
        if limit is not None:
            api_kwargs["limit"] = limit
        else:
            api_kwargs.pop("limit", None)
        if association_targets:
            api_kwargs["associations"] = association_targets

        if allow_search and (search_filters or search_sorts or normalized_conditions):
            filters = search_filters
            if filters is None and normalized_conditions:
                filters = _build_hubspot_search_filters(normalized_conditions, self.SEARCHABLE_COLUMNS)
            if filters is not None or search_sorts is not None:
                if association_targets:
                    logger.debug("HubSpot search API does not include associations for notes.")
                search_results = self._search_notes_by_conditions(
                    hubspot,
                    filters,
                    hubspot_properties,
                    limit,
                    search_sorts,
                    hubspot_columns,
                    association_targets,
                )
                logger.info(f"Retrieved {len(search_results)} notes from HubSpot via search API")
                return search_results

        notes = self.handler._get_objects_all("notes", **api_kwargs)
        notes_dict = []
        MAX_SCAN_ROWS = 10_000
        effective_limit = min(limit, MAX_SCAN_ROWS) if limit is not None else MAX_SCAN_ROWS
        for note in notes:
            try:
                row = self._note_to_dict(note, hubspot_columns, association_targets)
                notes_dict.append(row)
                if len(notes_dict) >= effective_limit:
                    break
            except Exception:
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
        association_targets: List[str],
    ) -> List[Dict[str, Any]]:
        return _execute_hubspot_search(
            hubspot.crm.objects.search_api,
            filters or [],
            properties,
            limit,
            lambda obj: self._note_to_dict(obj, columns, association_targets),
            sorts=sorts,
            object_type="notes",
        )

    def _note_to_dict(
        self,
        note: Any,
        columns: Optional[List[str]] = None,
        association_targets: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        columns = columns or self._get_default_note_columns()
        row = self._object_to_dict(note, columns)
        if association_targets:
            row = enrich_object_with_associations(note, "notes", row)
        return row

    def create_notes(self, notes_data: List[Dict[str, Any]]) -> None:
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

    def update_notes(self, note_ids: List[str], values_to_update: Dict[str, Any]) -> None:
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

    def delete_notes(self, note_ids: List[str]) -> None:
        hubspot = self.handler.connect()
        notes_to_delete = [HubSpotObjectId(id=nid) for nid in note_ids]
        batch_input = BatchInputSimplePublicObjectId(inputs=notes_to_delete)
        try:
            hubspot.crm.objects.notes.batch_api.archive(batch_input_simple_public_object_id=batch_input)
            logger.info("Notes deleted")
        except Exception as e:
            raise Exception(f"Notes deletion failed {e}")


class LeadsTable(HubSpotAPIResource):
    """HubSpot Leads table for prospective customer records."""

    # Reference: https://developers.hubspot.com/docs/api-reference/crm-leads-v3/guide
    SEARCHABLE_COLUMNS: Set[str] = {"hs_lead_name", "hs_lead_type", "hs_lead_label", "id"}
    ASSOCIATION_COLUMNS = {"primary_contact_id", "primary_company_id"}

    def meta_get_tables(self, table_name: str) -> Dict[str, Any]:
        row_count = None
        try:
            self.handler.connect()
            row_count = self.handler._estimate_table_rows("leads")
        except Exception:
            pass
        return {
            "TABLE_NAME": "leads",
            "TABLE_TYPE": "BASE TABLE",
            "TABLE_DESCRIPTION": "HubSpot leads representing prospective customer records",
            "ROW_COUNT": row_count,
        }

    def meta_get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        return self.handler._get_default_meta_columns("leads")

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
        leads_df = pd.json_normalize(
            self.get_leads(
                limit=limit,
                where_conditions=conditions,
                properties=targets,
                search_filters=search_filters,
                search_sorts=search_sorts,
                allow_search=allow_search,
            )
        )
        if leads_df.empty:
            leads_df = pd.DataFrame(columns=targets or self._get_default_lead_columns())
        return leads_df

    def add(self, lead_data: List[dict]):
        self.create_leads(lead_data)

    def modify(self, conditions: List[FilterCondition], values: Dict) -> None:
        normalized_conditions = _normalize_filter_conditions(conditions)
        leads_df = pd.json_normalize(self.get_leads(limit=200, where_conditions=normalized_conditions))

        if leads_df.empty:
            raise ValueError("No leads retrieved from HubSpot to evaluate update conditions.")

        executor_conditions = _normalize_conditions_for_executor(normalized_conditions)
        update_query_executor = UPDATEQueryExecutor(leads_df, executor_conditions)
        filtered_df = update_query_executor.execute_query()

        if filtered_df.empty:
            raise ValueError(f"No leads found matching WHERE conditions: {conditions}.")

        lead_ids = filtered_df["id"].astype(str).tolist()
        logger.info(f"Updating {len(lead_ids)} lead(s) matching WHERE conditions")
        self.update_leads(lead_ids, values)

    def remove(self, conditions: List[FilterCondition]) -> None:
        normalized_conditions = _normalize_filter_conditions(conditions)
        leads_df = pd.json_normalize(self.get_leads(limit=200, where_conditions=normalized_conditions))

        if leads_df.empty:
            raise ValueError("No leads retrieved from HubSpot to evaluate delete conditions.")

        executor_conditions = _normalize_conditions_for_executor(normalized_conditions)
        delete_query_executor = DELETEQueryExecutor(leads_df, executor_conditions)
        filtered_df = delete_query_executor.execute_query()

        if filtered_df.empty:
            raise ValueError(f"No leads found matching WHERE conditions: {conditions}.")

        lead_ids = filtered_df["id"].astype(str).tolist()
        logger.info(f"Deleting {len(lead_ids)} lead(s) matching WHERE conditions")
        self.delete_leads(lead_ids)

    def get_columns(self) -> List[str]:
        return self._get_default_lead_columns()

    @staticmethod
    def _get_default_lead_columns() -> List[str]:
        return [
            "id",
            "hs_lead_name",
            "hs_lead_type",
            "hs_lead_label",
            "hubspot_owner_id",
            "hs_timestamp",
            "primary_contact_id",
            "primary_company_id",
            "createdate",
            "lastmodifieddate",
        ]

    def get_leads(
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
        default_properties = self._get_default_lead_columns()
        columns = requested_properties or default_properties
        association_targets, hubspot_columns = _prepare_association_request("leads", columns)
        hubspot_properties = _build_hubspot_properties(hubspot_columns)

        MAX_ASSOCIATION_SCAN = 500
        if not normalized_conditions and limit is not None and limit > MAX_ASSOCIATION_SCAN:
            msg = (
                "Direct FK joins on HubSpot leads using foreign key columns "
                "(e.g. primary_contact_id, primary_company_id) are not supported. "
                "The HubSpot API represents relationships through association tables.\n\n"
                "Please rewrite your query using the appropriate association table."
            )
            raise ValueError(msg)

        api_kwargs = {**kwargs, "properties": hubspot_properties}
        if limit is not None:
            api_kwargs["limit"] = limit
        else:
            api_kwargs.pop("limit", None)
        if association_targets:
            api_kwargs["associations"] = association_targets

        if allow_search and (search_filters or search_sorts or normalized_conditions):
            filters = search_filters
            if filters is None and normalized_conditions:
                filters = _build_hubspot_search_filters(normalized_conditions, self.SEARCHABLE_COLUMNS)
            if filters is not None or search_sorts is not None:
                if association_targets:
                    logger.debug("HubSpot search API does not include associations for leads.")
                search_results = self._search_leads_by_conditions(
                    hubspot,
                    filters,
                    hubspot_properties,
                    limit,
                    search_sorts,
                    hubspot_columns,
                    association_targets,
                )
                logger.info(f"Retrieved {len(search_results)} leads from HubSpot via search API")
                return search_results

        leads = self.handler._get_objects_all("leads", **api_kwargs)
        leads_dict = []
        MAX_SCAN_ROWS = 10_000
        effective_limit = min(limit, MAX_SCAN_ROWS) if limit is not None else MAX_SCAN_ROWS
        for lead in leads:
            try:
                row = self._lead_to_dict(lead, hubspot_columns, association_targets)
                leads_dict.append(row)
                if len(leads_dict) >= effective_limit:
                    break
            except Exception:
                continue

        logger.info(f"Retrieved {len(leads_dict)} leads from HubSpot")
        return leads_dict

    def _search_leads_by_conditions(
        self,
        hubspot: HubSpot,
        filters: Optional[List[Dict[str, Any]]],
        properties: List[str],
        limit: Optional[int],
        sorts: Optional[List[Dict[str, Any]]],
        columns: List[str],
        association_targets: List[str],
    ) -> List[Dict[str, Any]]:
        return _execute_hubspot_search(
            hubspot.crm.objects.search_api,
            filters or [],
            properties,
            limit,
            lambda obj: self._lead_to_dict(obj, columns, association_targets),
            sorts=sorts,
            object_type="leads",
        )

    def _lead_to_dict(
        self,
        lead: Any,
        columns: Optional[List[str]] = None,
        association_targets: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        columns = columns or self._get_default_lead_columns()
        row = self._object_to_dict(lead, columns)
        if association_targets:
            row = enrich_object_with_associations(lead, "leads", row)
        return row

    def create_leads(self, leads_data: List[Dict[str, Any]]) -> None:
        if not leads_data:
            raise ValueError("No lead data provided for creation")

        logger.info(f"Attempting to create {len(leads_data)} lead(s)")
        hubspot = self.handler.connect()
        leads_to_create = [HubSpotObjectInputCreate(properties=lead) for lead in leads_data]
        batch_input = BatchInputSimplePublicObjectBatchInputForCreate(inputs=leads_to_create)

        try:
            created_leads = hubspot.crm.objects.leads.batch_api.create(
                batch_input_simple_public_object_batch_input_for_create=batch_input
            )
            if not created_leads or not hasattr(created_leads, "results") or not created_leads.results:
                raise Exception("Lead creation returned no results")
            created_ids = [lead.id for lead in created_leads.results]
            logger.info(f"Successfully created {len(created_ids)} lead(s) with IDs: {created_ids}")
        except Exception as e:
            logger.error(f"Leads creation failed: {str(e)}")
            raise Exception(f"Leads creation failed {e}")

    def update_leads(self, lead_ids: List[str], values_to_update: Dict[str, Any]) -> None:
        hubspot = self.handler.connect()
        leads_to_update = [HubSpotObjectBatchInput(id=lid, properties=values_to_update) for lid in lead_ids]
        batch_input = BatchInputSimplePublicObjectBatchInput(inputs=leads_to_update)
        try:
            updated = hubspot.crm.objects.leads.batch_api.update(
                batch_input_simple_public_object_batch_input=batch_input
            )
            logger.info(f"Leads with ID {[lead.id for lead in updated.results]} updated")
        except Exception as e:
            raise Exception(f"Leads update failed {e}")

    def delete_leads(self, lead_ids: List[str]) -> None:
        hubspot = self.handler.connect()
        leads_to_delete = [HubSpotObjectId(id=lid) for lid in lead_ids]
        batch_input = BatchInputSimplePublicObjectId(inputs=leads_to_delete)
        try:
            hubspot.crm.objects.leads.batch_api.archive(batch_input_simple_public_object_id=batch_input)
            logger.info("Leads deleted")
        except Exception as e:
            raise Exception(f"Leads deletion failed {e}")
