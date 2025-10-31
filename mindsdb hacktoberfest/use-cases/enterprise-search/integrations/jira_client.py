from typing import Dict, List

import mindsdb_sdk

from models.jira_issue import JiraIssue
from tools.search import SemanticSearchTool


class JiraClient:
    """Client for interacting with JIRA API via MindsDB Knowledge Base."""

    def __init__(self):
        self.server = mindsdb_sdk.connect()
        self.jira_kb = self.server.knowledge_bases.get("jira_kb")
        self.search_tool = SemanticSearchTool(kb_name="jira_kb")

    def search_tickets(
        self,
        content: str = None,
        filters: Dict = None,
        hybrid_search: bool = True,
        hybrid_search_alpha: float = 0.5,
        top_k: int = 5,
    ) -> List[Dict]:
        """
        Search JIRA tickets using hybrid search.

        Args:
            content: Semantic search query
            filters: Metadata filters (e.g., {'status': 'In Progress', 'priority': 'High'})
            hybrid_search: Enable hybrid search
            hybrid_search_alpha: Balance between semantic and keyword relevance
            top_k: Maximum number of results

        Returns:
            List of matching JIRA tickets
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
        self, content: str, assignee: str, additional_filters: Dict = None
    ) -> List[Dict]:
        """Search tickets by assignee with semantic content."""
        filters = {"assignee": assignee}
        if additional_filters:
            filters.update(additional_filters)
        return self.search_tickets(content=content, filters=filters)

    def search_tickets_by_project(
        self, content: str, project_key: str, additional_filters: Dict = None
    ) -> List[Dict]:
        """Search tickets by project with semantic content."""
        filters = {"project_key": project_key}
        if additional_filters:
            filters.update(additional_filters)
        return self.search_tickets(content=content, filters=filters)

    def search_tickets_by_creator(
        self, content: str, creator: str, additional_filters: Dict = None
    ) -> List[Dict]:
        """Search tickets by creator with semantic content."""
        filters = {"creator": creator}
        if additional_filters:
            filters.update(additional_filters)
        return self.search_tickets(content=content, filters=filters)

    def query_tickets(self, query: dict = None) -> List[JiraIssue]:
        table = self.server.databases.jira_datasource.tables.issues

        df = table.filter(**query).fetch() if query else table.fetch()

        records = df.to_dict(orient="records")

        return [JiraIssue(**record) for record in records]
