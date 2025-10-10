import pandas as pd
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
                        api_params['id'] = arg2
                elif op in ['>', '<', '>=', '<=']:
                    # Freshdesk supports some date filtering
                    if arg1 in ['created_at', 'updated_at']:
                        api_params[f'{arg1}__{op}'] = arg2
                else:
                    raise NotImplementedError(f"Operator '{op}' is not supported for column '{arg1}'")

        # Fetch data from Freshdesk API
        try:
            if 'id' in api_params:
                # Get specific agent
                endpoint = f"/api/v2/agents/{api_params['id']}"
                response_data = self.handler.call_freshdesk_api(endpoint)
                agents = [response_data] if response_data else []
            else:
                # Get all agents with pagination
                endpoint = "/api/v2/agents"
                page = 1
                agents = []
                
                while len(agents) < result_limit:
                    params = {'page': page, 'per_page': min(100, result_limit - len(agents))}
                    page_data = self.handler.call_freshdesk_api(endpoint, params=params)
                    
                    if not page_data:
                        break
                        
                    agents.extend(page_data)
                    
                    # If we got less than requested, we've reached the end
                    if len(page_data) < params['per_page']:
                        break
                        
                    page += 1

        except Exception as e:
            logger.error(f"Error fetching agents from Freshdesk: {e}")
            agents = []

        # Convert to DataFrame
        df = pd.DataFrame(agents, columns=self.get_columns())
        
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
                        api_params['id'] = arg2
                    elif arg1 in ['status', 'priority', 'source', 'type']:
                        filter_params[arg1] = arg2
                    elif arg1 == 'requester_id':
                        filter_params['requester_id'] = arg2
                    elif arg1 == 'responder_id':
                        filter_params['agent_id'] = arg2  # Freshdesk uses agent_id
                elif op in ['>', '<', '>=', '<=']:
                    # Date filtering
                    if arg1 in ['created_at', 'updated_at', 'due_by', 'fr_due_by']:
                        filter_params[f'{arg1}__{op}'] = arg2
                else:
                    raise NotImplementedError(f"Operator '{op}' is not supported for column '{arg1}'")

        # Fetch data from Freshdesk API
        try:
            if 'id' in api_params:
                # Get specific ticket
                endpoint = f"/api/v2/tickets/{api_params['id']}"
                response_data = self.handler.call_freshdesk_api(endpoint)
                tickets = [response_data] if response_data else []
            else:
                # Get tickets with filters
                endpoint = "/api/v2/tickets"
                page = 1
                tickets = []
                
                while len(tickets) < result_limit:
                    params = {
                        'page': page, 
                        'per_page': min(100, result_limit - len(tickets)),
                        **filter_params
                    }
                    
                    page_data = self.handler.call_freshdesk_api(endpoint, params=params)
                    
                    if not page_data:
                        break
                        
                    tickets.extend(page_data)
                    
                    # If we got less than requested, we've reached the end
                    if len(page_data) < params['per_page']:
                        break
                        
                    page += 1

        except Exception as e:
            logger.error(f"Error fetching tickets from Freshdesk: {e}")
            tickets = []

        # Convert to DataFrame
        df = pd.DataFrame(tickets, columns=self.get_columns())
        
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
