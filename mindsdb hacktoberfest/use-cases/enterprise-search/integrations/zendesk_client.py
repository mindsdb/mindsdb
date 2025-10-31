"""Zendesk API client."""

from typing import Dict, List

import mindsdb_sdk

from tools.search import SemanticSearchTool


class ZendeskClient:
    """Client for interacting with Zendesk API via MindsDB Knowledge Base."""

    def __init__(self, database="zendesk_datasource"):
        self.server = mindsdb_sdk.connect()
        self.kb = self.server.knowledge_bases.get("zendesk_kb")
        self.search_tool = SemanticSearchTool(kb_name="zendesk_kb")

    def search_tickets(
        self,
        content: str = None,
        filters: Dict = None,
        hybrid_search: bool = True,
        hybrid_search_alpha: float = 0.5,
        top_k: int = 5,
    ) -> List[Dict]:
        """
        Search Zendesk tickets using hybrid search.

        Args:
            content: Semantic search query
            filters: Metadata filters (e.g., {'status': 'open', 'priority': 'urgent'})
            hybrid_search: Enable hybrid search
            hybrid_search_alpha: Balance between semantic and keyword relevance
            top_k: Maximum number of results

        Returns:
            List of matching Zendesk tickets
        """
        return self.search_tool.search(
            content=content,
            filters=filters,
            top_k=top_k,
            hybrid_search=hybrid_search,
            hybrid_search_alpha=hybrid_search_alpha,
        )

    def search_tickets_by_status(
        self, content: str, status: str, additional_filters: Dict = None
    ) -> List[Dict]:
        """Search tickets by status with semantic content."""
        filters = {"status": status}
        if additional_filters:
            filters.update(additional_filters)
        return self.search_tickets(content=content, filters=filters)

    def search_tickets_by_priority(
        self, content: str, priority: str, additional_filters: Dict = None
    ) -> List[Dict]:
        """Search tickets by priority with semantic content."""
        filters = {"priority": priority}
        if additional_filters:
            filters.update(additional_filters)
        return self.search_tickets(content=content, filters=filters)

    def search_tickets_by_assignee(
        self, content: str, assignee_id: str, additional_filters: Dict = None
    ) -> List[Dict]:
        """Search tickets by assignee with semantic content."""
        filters = {"assignee_id": assignee_id}
        if additional_filters:
            filters.update(additional_filters)
        return self.search_tickets(content=content, filters=filters)

    def search_tickets_by_type(
        self, content: str, ticket_type: str, additional_filters: Dict = None
    ) -> List[Dict]:
        """Search tickets by type with semantic content."""
        filters = {"type": ticket_type}
        if additional_filters:
            filters.update(additional_filters)
        return self.search_tickets(content=content, filters=filters)

    def search_tickets_by_requester(
        self, content: str, requester_id: str, additional_filters: Dict = None
    ) -> List[Dict]:
        """Search tickets by requester with semantic content."""
        filters = {"requester_id": requester_id}
        if additional_filters:
            filters.update(additional_filters)
        return self.search_tickets(content=content, filters=filters)

    def search_tickets_by_tags(
        self, content: str, tags: str, additional_filters: Dict = None
    ) -> List[Dict]:
        """Search tickets by tags with semantic content."""
        filters = {"tags": tags}
        if additional_filters:
            filters.update(additional_filters)
        return self.search_tickets(content=content, filters=filters)

    # def query_tickets(self, query: dict = None) -> List[ZendeskTicket]:
    def query_tickets(self, query: dict = None):
        table = self.server.databases.zendesk_datasource.tables.tickets

        df = table.filter(**query).fetch() if query else table.fetch()

        records = df.to_dict(orient="records")

        # return [ZendeskTicket(**record) for record in records]
        return records
