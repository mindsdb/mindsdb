import pandas as pd
from typing import List, Dict, Tuple
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.handlers.query_utilities import (
    SELECTQueryParser,
    SELECTQueryExecutor,
)
from mindsdb.utilities import log
from mindsdb_sql_parser import ast
from urllib.parse import quote

logger = log.getLogger(__name__)


class FreshdeskAgentsTable(APITable):
    """Freshdesk Agents Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the freshdesk list agents API

        Parameters
        ----------
        query : ast.Select
            Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Freshdesk agents
        """

        select_statement_parser = SELECTQueryParser(query, "agents", self.get_columns())
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        subset_where_conditions, filter_conditions = self.get_conditions(where_conditions)

        df = self.get_freshdesk_agents(filter_conditions)

        select_statement_executor = SELECTQueryExecutor(
            df, selected_columns, subset_where_conditions, order_by_conditions, result_limit
        )
        df = select_statement_executor.execute_query()
        return df

    def get_conditions(self, where_conditions) -> Tuple:
        subset_where_conditions = []
        filter_conditions = {}

        for op, arg1, arg2 in where_conditions:
            if arg1 in self.get_columns():
                if arg1 in self.get_api_filter_columns() and op == "=":
                    filter_conditions[self.get_api_filter_columns()[arg1]] = arg2
                else:
                    subset_where_conditions.append([op, arg1, arg2])
        return subset_where_conditions, filter_conditions

    def get_freshdesk_agents(self, api_filters):
        agents = self.handler.freshdesk_client.agents.list_agents(**api_filters)
        response = []

        for agent in agents:
            response.append(self.agent_to_dict(agent))

        return pd.json_normalize(response, sep="_").reindex(columns=self.get_columns(), fill_value=None)

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses"""
        return [
            "available",
            "occasional",
            "id",
            "ticket_scope",
            "created_at",
            "updated_at",
            "last_active_at",
            "available_since",
            "type",
            "deactivated",
            "signature",
            "focus_mode",
            "contact_active",
            "contact_email",
            "contact_job_title",
            "contact_language",
            "contact_last_login_at",
            "contact_mobile",
            "contact_name",
            "contact_phone",
            "contact_time_zone",
            "contact_created_at",
            "contact_updated_at",
        ]

    def get_api_filter_columns(self) -> Dict[str, str]:
        """Gets all columns that can be used to filter through the API directly"""
        return {
            "contact_email": "email",
            "contact_mobile": "mobile",
            "contact_phone": "phone",
            "contact_state": "state",
        }

    def agent_to_dict(self, agent):
        dict = {col: getattr(agent, col, None) for col in self.get_columns()}
        dict["contact"] = getattr(agent, "contact", None)
        return dict


class FreshdeskTicketsTable(APITable):
    """Freshdesk Tickets Table implementation"""

    PRIORITY_MAP = {"low": 1, "medium": 2, "high": 3, "urgent": 4}
    STATUS_MAP = {"open": 2, "pending": 3, "resolved": 4, "closed": 5}

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the freshdesk list tickets API

        Parameters
        ----------
        query : ast.Select
            Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Freshdesk tickets
        """

        select_statement_parser = SELECTQueryParser(query, "tickets", self.get_columns())

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        subset_where_conditions, filter_conditions = self.get_conditions(where_conditions)

        df = self.get_freshdesk_tickets(filter_conditions)

        select_statement_executor = SELECTQueryExecutor(
            df, selected_columns, subset_where_conditions, order_by_conditions, result_limit
        )

        df = select_statement_executor.execute_query()
        return df

    def get_conditions(self, where_conditions) -> Tuple:
        subset_where_conditions = []
        search_conditions = []

        for op, arg1, val in where_conditions:
            if arg1 in self.get_api_filter_columns() and op in self.get_operator_map().keys():
                if arg1 == "priority" and isinstance(val, str):
                    val = self.PRIORITY_MAP.get(val.lower(), val)
                if arg1 == "status" and isinstance(val, str):
                    val = self.STATUS_MAP.get(val.lower(), val)
                search_conditions.append((op, arg1, val))
            else:
                subset_where_conditions.append([op, arg1, val])

        return subset_where_conditions, search_conditions

    def get_freshdesk_tickets(self, filter_conditions):
        if len(filter_conditions) > 0:
            tickets = self.handler.freshdesk_client.tickets.filter_tickets(
                query=self.build_freshdesk_api_filter_query(filter_conditions)
            )
        else:
            tickets = self.handler.freshdesk_client.tickets.list_tickets(filter_name=None)

        response = []

        for ticket in tickets:
            response.append(self.ticket_to_dict(ticket))

        return pd.json_normalize(response, sep="_").reindex(columns=self.get_columns(), fill_value=None)

    def build_freshdesk_api_filter_query(self, conditions):
        """
        Build Freshdesk API filter query string, quoting strings and mapping enums.
        """

        op_map = self.get_operator_map()
        parts = []

        for op, field, value in conditions:
            freshdesk_operator = op_map.get(op)
            if freshdesk_operator is None:
                raise ValueError(f"Unsupported operator: {op}")

            if isinstance(value, str):
                escaped_value = value.replace("'", "'")
                value_str = f"'{escaped_value}'"
            else:
                value_str = str(value)

            parts.append(f"{field}:{value_str}")

        query_string = " AND ".join(parts)
        return quote(query_string)

    def get_operator_map(self):
        """Mapping of sql where operators to freshdesk API query operators"""
        return {
            "=": ":",
            ">": ":>",
            "<": ":<",
            ">=": ":>",
            "<=": ":<",
        }

    def ticket_to_dict(self, ticket):
        return {col: getattr(ticket, col, None) for col in self.get_columns()}

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses"""
        return [
            "attachments",
            "cc_emails",
            "company_id",
            "custom_fields",
            "deleted",
            "description",
            "description_text",
            "due_by",
            "email",
            "email_config_id",
            "facebook_id",
            "fr_due_by",
            "fr_escalated",
            "fwd_emails",
            "group_id",
            "id",
            "is_escalated",
            "name",
            "phone",
            "priority",
            "product_id",
            "reply_cc_emails",
            "requester_id",
            "responder_id",
            "source",
            "spam",
            "status",
            "subject",
            "tags",
            "to_emails",
            "twitter_id",
            "type",
            "created_at",
            "updated_at",
        ]

    def get_api_filter_columns(self) -> Dict[str, str]:
        """Gets all columns that can be used to filter through the API directly"""

        return {
            "status": "status",
            "priority": "priority",
            "type": "type",
            "group_id": "group_id",
            "agent_id": "agent_id",
            "created_at": "created_at",
            "updated_at": "updated_at",
            "fr_due_by": "fr_due_by",
        }
