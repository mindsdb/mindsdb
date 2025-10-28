from typing import List, Optional

from fastmcp import FastMCP

from integrations import confluence_client, jira_client, zendesk_client
from models.common import Filter
from models.confluence_page import ConfluencePage
from models.jira_issue import JiraIssue

mcp = FastMCP(name="Support Agent Assistant", port=5000)
jira_client = jira_client.JiraClient()
zendesk_client = zendesk_client.ZendeskClient()
confluence_client = confluence_client.ConfluenceClient()


@mcp.tool
def fetch_releavant_jira_issues(content: str, filters: dict = None):
    """Given a content, fetches the releavant JIRA issue chunks ranked based on relevance. You can use the relavant IDs after fetching to get the full issue details if needed."""
    res = jira_client.search_tickets(content=content, filters=filters)
    return res


@mcp.tool
def query_jira_issues(query: dict) -> List[JiraIssue]:
    """Fetches the JIRA issues based on the deterministic conditions"""
    res = jira_client.query_tickets(query=query)
    return res


@mcp.tool
def fetch_releavant_zendesk_tickets(content: str, filters: dict = None):
    """Given a content, fetches the releavant Zendesk ticket chunks ranked based on relevance. You can use the relavant IDs after fetching to get the full ticket details if needed."""
    res = zendesk_client.search_tickets(content=content, filters=filters)
    return res


@mcp.tool
# def query_zendesk_tickets(query: dict) -> List[ZendeskTicket]:
def query_zendesk_tickets(query: dict):
    """Fetches the Zendesk tickets based on the deterministic conditions"""
    res = zendesk_client.query_tickets(query=query)
    return res


@mcp.tool
def fetch_releavant_confluence_pages(
    content: str, filters: Optional[Filter] = Filter(filters={})
):
    """Given a content, fetches the relevant Confluence page chunks ranked based on relevance. You can use the relevant IDs after fetching to get the full page details if needed."""
    res = confluence_client.search_pages(content=content, filters=filters.filters)
    return res


@mcp.tool
def query_confluence_pages(query: dict) -> List[ConfluencePage]:
    """Fetches the Confluence pages based on the deterministic conditions"""
    res = confluence_client.query_pages(filters=query, as_models=True)
    return res


if __name__ == "__main__":
    mcp.run(transport="sse")
