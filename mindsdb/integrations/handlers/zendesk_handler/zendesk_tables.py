import pandas as pd
from typing import List
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.handlers.query_utilities import (
    SELECTQueryParser,
    SELECTQueryExecutor,
)
from mindsdb.utilities import log
from mindsdb_sql.parser import ast
import zenpy

logger = log.getLogger(__name__)


class ZendeskUsersTable(APITable):
    """Zendesk Users Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the zendesk list users API

        Parameters
        ----------
        query : ast.Select
            Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Zendesk users

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'users',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = (
            select_statement_parser.parse_query()
        )

        subset_where_conditions = {}
        for op, arg1, arg2 in where_conditions:
            if arg1 in self.get_columns():
                if op != '=':
                    raise NotImplementedError(f"Unknown op: {op}. Only '=' is supported.")
                subset_where_conditions[arg1] = arg2

        count = 0
        result = self.handler.zen_client.users(**subset_where_conditions)
        response = []
        if isinstance(result, zenpy.lib.generator.BaseResultGenerator):
            while count <= result_limit:
                response.append(result.next().to_dict())
                count += 1
        else:
            response.append(result.to_dict())

        df = pd.DataFrame(response, columns=self.get_columns())

        select_statement_executor = SELECTQueryExecutor(
            df,
            selected_columns,
            subset_where_conditions,
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
            "active", "alias", "chat_only", "created_at", "custom_role_id",
            "details", "email", "external_id", "id", "last_login_at",
            "locale", "locale_id", "moderator", "name", "notes",
            "only_private_comments", "organization_id", "phone", "photo",
            "restricted_agent", "role", "shared", "shared_agent",
            "signature", "suspended", "tags", "ticket_restriction",
            "time_zone", "two_factor_auth_enabled", "updated_at", "url",
            "verified", "iana_time_zone", "shared_phone_number", "role_type",
            "default_group_id", "report_csv"
        ]


class ZendeskTicketsTable(APITable):
    """Zendesk tickets Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the zendesk tickets API

        Parameters
        ----------
        query : ast.Select
            Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Ticket ID Data

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

        subset_where_conditions = {}
        for op, arg1, arg2 in where_conditions:
            if arg1 in self.get_columns():
                if op != '=':
                    raise NotImplementedError(f"Unknown op: {op}. Only '=' is supported.")
                subset_where_conditions[arg1] = arg2

        count = 0
        result = self.handler.zen_client.tickets(**subset_where_conditions)
        response = []
        if isinstance(result, zenpy.lib.generator.BaseResultGenerator):
            while count <= result_limit:
                response.append(result.next().to_dict())
                count += 1
        else:
            response.append(result.to_dict())

        df = pd.DataFrame(response, columns=self.get_columns())

        select_statement_executor = SELECTQueryExecutor(
            df,
            selected_columns,
            subset_where_conditions,
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
            "assignee_id", "brand_id", "collaborator_ids", "created_at",
            "custom_fields", "description", "due_at", "external_id",
            "fields", "forum_topic_id", "group_id", "has_incidents", "id",
            "organization_id", "priority", "problem_id", "raw_subject",
            "recipient", "requester_id", "sharing_agreement_ids", "status",
            "subject", "submitter_id", "tags", "type", "updated_at", "url",
            "generated_timestamp", "follower_ids", "email_cc_ids", "is_public",
            "custom_status_id", "followup_ids", "ticket_form_id",
            "allow_channelback", "allow_attachments", "from_messaging_channel",
            "satisfaction_rating.assignee_id", "satisfaction_rating.created_at",
            "satisfaction_rating.group_id", "satisfaction_rating.id",
            "satisfaction_rating.requester_id", "satisfaction_rating.score",
            "satisfaction_rating.ticket_id", "satisfaction_rating.updated_at",
            "satisfaction_rating.url", "via.channel", "via.source.rel"
        ]


class ZendeskTriggersTable(APITable):
    """Zendesk Triggers Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the zendesk triggers API

        Parameters
        ----------
        query : ast.Select
            Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Trigger Data

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'triggers',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = (
            select_statement_parser.parse_query()
        )

        subset_where_conditions = {}
        for op, arg1, arg2 in where_conditions:
            if arg1 in self.get_columns():
                if op != '=':
                    raise NotImplementedError(f"Unknown op: {op}. Only '=' is supported.")
                subset_where_conditions[arg1] = arg2

        count = 0
        result = self.handler.zen_client.triggers(**subset_where_conditions)
        response = []
        if isinstance(result, zenpy.lib.generator.BaseResultGenerator):
            while count <= result_limit:
                response.append(result.next().to_dict())
                count += 1
        else:
            response.append(result.to_dict())

        df = pd.DataFrame(response, columns=self.get_columns())

        select_statement_executor = SELECTQueryExecutor(
            df,
            selected_columns,
            subset_where_conditions,
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
            "actions", "active", "description", "id", "position", "title",
            "url", "updated_at", "created_at", "default", "raw_title",
            "category_id", "conditions.all", "conditions.any"
        ]


class ZendeskActivitiesTable(APITable):
    """Zendesk Activities Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the zendesk activities API

        Parameters
        ----------
        query : ast.Select
            Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Activity list Data

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'activities',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = (
            select_statement_parser.parse_query()
        )

        subset_where_conditions = {}
        for op, arg1, arg2 in where_conditions:
            if arg1 in self.get_columns():
                if op != '=':
                    raise NotImplementedError(f"Unknown op: {op}. Only '=' is supported.")
                subset_where_conditions[arg1] = arg2

        count = 0
        result = self.handler.zen_client.activities(**subset_where_conditions)
        response = []
        if isinstance(result, zenpy.lib.generator.BaseResultGenerator):
            while count <= result_limit:
                response.append(result.next().to_dict())
                count += 1
        else:
            response.append(result.to_dict())

        df = pd.DataFrame(response, columns=self.get_columns())

        select_statement_executor = SELECTQueryExecutor(
            df,
            selected_columns,
            subset_where_conditions,
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
            "created_at",
            "id",
            "title",
            "updated_at",
            "url",
            "verb",
            "user_id",
            "actor_id",
            "actor.id",
            "actor.url",
            "actor.name",
            "actor.email",
            "actor.created_at",
            "actor.updated_at",
            "actor.time_zone",
            "actor.iana_time_zone",
            "actor.phone",
            "actor.shared_phone_number",
            "actor.photo",
            "actor.locale_id",
            "actor.locale",
            "actor.organization_id",
            "actor.role",
            "actor.verified",
            "actor.external_id",
            "actor.tags",
            "actor.alias",
            "actor.active",
            "actor.shared",
            "actor.shared_agent",
            "actor.last_login_at",
            "actor.two_factor_auth_enabled",
            "actor.signature",
            "actor.details",
            "actor.notes",
            "actor.role_type",
            "actor.custom_role_id",
            "actor.moderator",
            "actor.ticket_restriction",
            "actor.only_private_comments",
            "actor.restricted_agent",
            "actor.suspended",
            "actor.default_group_id",
            "actor.report_csv",
            "user.active",
            "user.alias",
            "user.chat_only",
            "user.created_at",
            "user.custom_role_id",
            "user.details",
            "user.email",
            "user.external_id",
            "user.id",
            "user.last_login_at",
            "user.locale",
            "user.locale_id",
            "user.moderator",
            "user.name",
            "user.notes",
            "user.only_private_comments",
            "user.organization_id",
            "user.phone",
            "user.photo",
            "user.restricted_agent",
            "user.role",
            "user.shared",
            "user.shared_agent",
            "user.signature",
            "user.suspended",
            "user.tags",
            "user.ticket_restriction",
            "user.time_zone",
            "user.two_factor_auth_enabled",
            "user.updated_at",
            "user.url",
            "user.verified",
            "user.iana_time_zone",
            "user.shared_phone_number",
            "user.role_type",
            "user.default_group_id",
            "user.report_csv",
            "target.active",
            "target.content_type",
            "target.created_at",
            "target.id",
            "target.method",
            "target.password",
            "target.target_url",
            "target.title",
            "target.type",
            "target.url",
            "target.username",
            "target.ticket.assignee_id",
            "target.ticket.brand_id",
            "target.ticket.collaborator_ids",
            "target.ticket.created_at",
            "target.ticket.custom_fields",
            "target.ticket.description",
            "target.ticket.due_at",
            "target.ticket.external_id",
            "target.ticket.fields",
            "target.ticket.forum_topic_id",
            "target.ticket.group_id",
            "target.ticket.has_incidents",
            "target.ticket.id",
            "target.ticket.organization_id",
            "target.ticket.priority",
            "target.ticket.problem_id",
            "target.ticket.raw_subject",
            "target.ticket.recipient",
            "target.ticket.requester_id",
            "target.ticket.satisfaction_rating",
            "target.ticket.sharing_agreement_ids",
            "target.ticket.status",
            "target.ticket.subject",
            "target.ticket.submitter_id",
            "target.ticket.tags",
            "target.ticket.type",
            "target.ticket.updated_at",
            "target.ticket.url",
            "target.ticket.via"
        ]
