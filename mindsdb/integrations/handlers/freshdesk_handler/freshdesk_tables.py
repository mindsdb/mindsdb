import pandas as pd
import re
from typing import List
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.handlers.query_utilities import (
    SELECTQueryParser,
    SELECTQueryExecutor,
)
from mindsdb.utilities import log
from mindsdb_sql_parser import ast

logger = log.getLogger(__name__)


class FreshdeskAgentsTable(APITable):
    """Freshdesk Agents Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Freshdesk agents API
        Parameters
        ----------
        query : ast.Select
            Given SQL SELECT query
        Returns
        -------
        pd.DataFrame
            Freshdesk agents data
        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'agents',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = (
            select_statement_parser.parse_query()
        )

        # Build API parameters from WHERE conditions
        api_params = {}
        for op, arg1, arg2 in where_conditions:
            if arg1 in self.get_columns():
                if op == '=':
                    # For specific agent by ID
                    if arg1 == 'id':
                        if not str(arg2).isdigit():
                            raise ValueError("Agent ID must be an integer.")
                        api_params['id'] = int(arg2)
                elif op in ['>', '<', '>=', '<=']:
                    # Freshdesk supports some date filtering
                    if arg1 in ['created_at', 'updated_at']:
                        # Validate date format (ISO8601)
                        if not re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$", str(arg2)):
                            raise ValueError(f"Invalid date format for {arg1}: {arg2}. Expected ISO8601 format (YYYY-MM-DDTHH:MM:SSZ)")
                        api_params[f'{arg1}__{op}'] = arg2
                else:
                    raise NotImplementedError(f"Operator '{op}' is not supported for column '{arg1}'")

        # Fetch data from Freshdesk API
        try:
            if 'id' in api_params:
                # Get specific agent
                endpoint = f"/api/v2/agents/{api_params['id']}"
                response_data = self.handler.call_freshdesk_api(endpoint, paginate=False)
                agents = [response_data] if response_data else []
            else:
                # Get all agents with automatic pagination
                endpoint = "/api/v2/agents"
                agents = self.handler.call_freshdesk_api(endpoint, params=api_params, paginate=True)

                # Limit results if specified
                if result_limit is not None and len(agents) > result_limit:
                    agents = agents[:result_limit]

        except Exception as e:
            logger.error(f"Error fetching agents from Freshdesk: {e}")
            agents = []

        # Convert to DataFrame - build from data first, then reindex for efficiency
        df = pd.DataFrame(agents)
        df = df.reindex(columns=self.get_columns())

        # Apply query execution (filtering, ordering, limiting)
        select_statement_executor = SELECTQueryExecutor(
            df,
            selected_columns,
            {},  # We already applied WHERE conditions via API
            order_by_conditions,
            result_limit
        )

        df = select_statement_executor.execute_query()
        return df

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses
        Returns
        -------
        List[str]
            List of columns
        """
        return [
            "id", "email", "mobile", "name", "phone", "active",
            "address", "auto_assign", "background_information", 
            "can_see_all_changes_from_associated_departments",
            "can_see_all_tickets_from_associated_departments",
            "created_at", "custom_fields", "department_ids",
            "deactivated", "focus_mode", "group_ids", "has_logged_in",
            "job_title", "language", "last_active_at", "last_login_at",
            "location_id", "occasional", "role_ids", "scoreboard_level_id",
            "scoreboard_points", "skill_ids", "signature", "ticket_scope",
            "time_format", "time_zone", "type", "updated_at", "work_numbers"
        ]


class FreshdeskTicketsTable(APITable):
    """Freshdesk Tickets Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Freshdesk tickets API
        Parameters
        ----------
        query : ast.Select
            Given SQL SELECT query
        Returns
        -------
        pd.DataFrame
            Freshdesk tickets data
        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'tickets',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = (
            select_statement_parser.parse_query()
        )

        # Build API parameters from WHERE conditions
        api_params = {}
        filter_params = {}

        for op, arg1, arg2 in where_conditions:
            if arg1 in self.get_columns():
                if op == '=':
                    if arg1 == 'id':
                        if not str(arg2).isdigit():
                            raise ValueError("Ticket ID must be an integer.")
                        api_params['id'] = int(arg2)
                    elif arg1 in ['status', 'priority', 'source', 'type']:
                        # Validate allowed values for these fields
                        allowed_values = {
                            'status': [2, 3, 4, 5],  # Open, Pending, Resolved, Closed
                            'priority': [1, 2, 3, 4],  # Low, Medium, High, Urgent
                            'source': [1, 2, 3, 7, 8, 9, 10],  # Email, Portal, Phone, Chat, etc.
                            'type': ["Question", "Incident", "Problem", "Feature Request"]
                        }
                        if arg1 in allowed_values and arg2 not in allowed_values[arg1]:
                            raise ValueError(f"Invalid value for {arg1}: {arg2}. Allowed values: {allowed_values[arg1]}")
                        filter_params[arg1] = arg2
                    elif arg1 == 'requester_id':
                        if not str(arg2).isdigit():
                            raise ValueError("Requester ID must be an integer.")
                        filter_params['requester_id'] = int(arg2)
                    elif arg1 == 'responder_id':
                        if not str(arg2).isdigit():
                            raise ValueError("Responder ID must be an integer.")
                        filter_params['agent_id'] = int(arg2)  # Freshdesk uses agent_id
                elif op in ['>', '<', '>=', '<=']:
                    # Date filtering
                    if arg1 in ['created_at', 'updated_at', 'due_by', 'fr_due_by']:
                        # Validate date format (ISO8601)
                        if not re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$", str(arg2)):
                            raise ValueError(f"Invalid date format for {arg1}: {arg2}. Expected ISO8601 format (YYYY-MM-DDTHH:MM:SSZ)")
                        filter_params[f'{arg1}__{op}'] = arg2
                else:
                    raise NotImplementedError(f"Operator '{op}' is not supported for column '{arg1}'")

        # Fetch data from Freshdesk API
        try:
            if 'id' in api_params:
                # Get specific ticket
                endpoint = f"/api/v2/tickets/{api_params['id']}"
                response_data = self.handler.call_freshdesk_api(endpoint, paginate=False)
                tickets = [response_data] if response_data else []
            else:
                # Get tickets with filters and automatic pagination
                endpoint = "/api/v2/tickets"
                tickets = self.handler.call_freshdesk_api(endpoint, params=filter_params, paginate=True)

                # Limit results if specified
                if result_limit is not None and len(tickets) > result_limit:
                    tickets = tickets[:result_limit]

        except Exception as e:
            logger.error(f"Error fetching tickets from Freshdesk: {e}")
            tickets = []

        # Convert to DataFrame - build from data first, then reindex for efficiency
        df = pd.DataFrame(tickets)
        df = df.reindex(columns=self.get_columns())

        # Apply query execution (filtering, ordering, limiting)
        select_statement_executor = SELECTQueryExecutor(
            df,
            selected_columns,
            {},  # We already applied WHERE conditions via API
            order_by_conditions,
            result_limit
        )

        df = select_statement_executor.execute_query()
        return df

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses
        Returns
        -------
        List[str]
            List of columns
        """
        return [
            "id", "subject", "description", "description_text", "status",
            "priority", "type", "source", "created_at", "updated_at",
            "requested_for_id", "requester_id", "responder_id", "group_id",
            "company_id", "product_id", "support_email", "to_emails",
            "cc_emails", "bcc_emails", "email_failure_count", "due_by",
            "fr_escalated", "spam", "is_escalated", "fr_due_by",
            "reply_cc_emails", "reply_bcc_emails", "custom_fields",
            "tags", "attachments", "sentiment_score", "satisfaction_rating",
            "urgency", "impact", "category", "sub_category", "item_category",
            "deleted", "initial_response_due_by", "next_response_due_by",
            "resolution_due_by", "nr_escalated", "ticket_cc_emails"
        ]
