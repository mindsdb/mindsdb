# mindsdb/integrations/handlers/jira_handler/jira_tables.py

from typing import Any, Dict, Iterable, List, Optional, Tuple

from atlassian import Jira
import pandas as pd
from requests.exceptions import HTTPError

from mindsdb.integrations.libs.api_handler import APIResource
from mindsdb.integrations.utilities.sql_utils import (
    FilterCondition,
    SortColumn,
    FilterOperator,
)
from mindsdb.utilities import log


logger = log.getLogger(__name__)

SERVER_COLUMNS = [
    "key",
    "name",
    "emailAddress",
    "displayName",
    "active",
    "timeZone",
    "locale",
    "lastLoginTime",
    "applicationRoles",
    "avatarUrls",
    "groups",
    "deleted",
    "expand",
]
CLOUD_COLUMNS = [
    "accountId",
    "accountType",
    "emailAddress",
    "displayName",
    "active",
    "timeZone",
    "locale",
    "applicationRoles",
    "avatarUrls",
    "groups",
]


class JiraTableBase(APIResource):
    """
    Base class for Jira tables.

    Provides a helper for converting API records to a DataFrame
    with a fixed set of columns.
    """

    def __init__(self, handler: Any) -> None:
        super().__init__(handler)
        self.handler = handler

    def to_dataframe(self, records: Optional[List[dict]]) -> pd.DataFrame:
        """
        Convert records to DataFrame with fixed columns, handling missing optional fields.

        Args:
            records: List of record dictionaries from Jira API, or None/empty list.

        Returns:
            DataFrame with all expected columns, missing fields filled with None.
        """
        if records:
            df = pd.DataFrame(records)
            df = df.reindex(columns=self.get_columns(), fill_value=None)
        else:
            df = pd.DataFrame([], columns=self.get_columns())
        return df


class JiraIssueFetcherMixin:
    """
    Utility mixin to share issue fetching
    logic between Jira issue-related tables:
    - issues
    - attachments
    - comments
    """

    PROJECT_FIELDS = {"project_id", "project_key", "project_name", "project"}

    def _fetch_issues(
        self,
        client: Jira,
        conditions: Optional[List[FilterCondition]],
        limit: Optional[int],
    ) -> List[dict]:
        issues: List[dict] = []
        conditions = conditions or []

        # Apply identifier or project-based filters
        for condition in conditions:
            if condition.column in ("id", "key", "issue_id", "issue_key"):
                fetched = self._fetch_by_identifier(client, condition)
                for issue in fetched:
                    issues.append(issue)
                condition.applied = True
            elif condition.column in self.PROJECT_FIELDS:
                project_ids = self._resolve_project_ids(
                    client, condition.column, condition.value
                )
                if len(project_ids) > 0:
                    self._fetch_by_projects(client, project_ids, limit, issues)
                    condition.applied = True

        if not issues:
            projects = self._get_all_projects(client)
            project_ids = []
            for project in projects:
                project_id = project.get("id")
                if project_id is not None:
                    project_ids.append(project_id)
            self._fetch_by_projects(client, project_ids, limit, issues)

        return issues

    def _fetch_by_identifier(
        self, client: Jira, condition: FilterCondition
    ) -> List[dict]:
        """
        Fetch issues by id or key. For IN, we still call get_issue for each identifier.
        """
        if isinstance(condition.value, (list, tuple, set)):
            values: Iterable = condition.value
        else:
            values = [condition.value]

        issues: List[dict] = []

        for identifier in values:
            if condition.op in (FilterOperator.EQUAL, FilterOperator.IN):
                issue = client.get_issue(identifier)
                if isinstance(issue, dict):
                    issues.append(issue)
                else:
                    logger.debug(
                        "Skipping non-dict issue result for identifier %s: %s",
                        identifier,
                        type(issue).__name__,
                    )
            else:
                raise ValueError(
                    f"Unsupported operator {condition.op} for column {condition.column}."
                )

        return issues

    def _fetch_by_projects(
        self,
        client: Jira,
        project_ids: Iterable[str],
        limit: Optional[int],
        current_issues: List[dict],
    ) -> None:
        """
        Fetch issues by project, appending them to current_issues, and respecting the global limit.
        """
        for project_id in project_ids:
            new_issues = self._get_project_issues_with_limit(
                client,
                project_id,
                limit=limit,
                current_issues=current_issues,
            )
            for issue in new_issues:
                current_issues.append(issue)
            if limit is not None and len(current_issues) >= limit:
                break

    def _resolve_project_ids(self, client: Jira, column: str, value: Any) -> List[str]:
        """
        Resolve project ids from project-id, project-key, or project-name based filter values.
        """
        projects = self._get_all_projects(client)

        if isinstance(value, (list, tuple, set)):
            values = value
        else:
            values = [value]

        resolved_ids: List[str] = []

        for val in values:
            if column == "project_id":
                resolved_ids.append(str(val))
            elif column in ("project_key", "project"):
                project = None
                for p in projects:
                    if p.get("key") == val:
                        project = p
                        break
                if project is not None:
                    resolved_ids.append(str(project.get("id")))
                else:
                    resolved_ids.append(str(val))
            elif column == "project_name":
                project = None
                for p in projects:
                    if p.get("name") == val:
                        project = p
                        break
                if project is not None:
                    resolved_ids.append(str(project.get("id")))
            else:
                resolved_ids.append(str(val))

        return resolved_ids

    def _get_all_projects(self, client: Jira) -> List[Dict]:
        """
        Cached list of all projects for the current handler connection.

        Normalizes different Jira client return shapes (list or dict with 'projects'/'values')
        and stores a list of project dicts in a cache attribute so the return type is always List[Dict].
        """
        if not hasattr(self, "_project_cache"):
            resp = client.get_all_projects()
            projects: List[Dict] = []

            if isinstance(resp, list):
                projects = resp
            elif isinstance(resp, dict):
                projects = resp.get("projects") or resp.get("values") or []
                if projects is None:
                    projects = []
            else:
                projects = []

            self._project_cache = list(projects)

        return self._project_cache

    def _get_issue_field(self, client: Jira, issue: Dict, field_key: str) -> Any:
        """
        Robust helper to fetch a specific field for an issue.

        If the field is not present in the issue's 'fields' dict, it will try
        to refetch the issue with get_issue() and update the cache.
        """
        fields = issue.get("fields") or {}
        if field_key in fields:
            if fields[field_key] is not None:
                return fields[field_key]

        issue_identifier = issue.get("id") or issue.get("key")
        if issue_identifier is None:
            logger.debug(
                "Issue identifier missing, cannot fetch field '%s' for issue: %s",
                field_key,
                issue,
            )
            return None

        try:
            logger.debug(
                "Fetching missing field '%s' for issue '%s'",
                field_key,
                issue_identifier,
            )
            refreshed_issue = client.get_issue(str(issue_identifier))
        except Exception as issue_error:
            logger.warning(
                "Unable to fetch %s for issue %s: %s",
                field_key,
                issue_identifier,
                issue_error,
            )
            return None

        refreshed_fields = refreshed_issue.get("fields", {})
        if "fields" not in issue or not isinstance(issue["fields"], dict):
            issue["fields"] = {}
        issue["fields"][field_key] = refreshed_fields.get(field_key)

        return refreshed_fields.get(field_key)

    def _get_project_issues_with_limit(
        self,
        client: Jira,
        project_id: str,
        limit: Optional[int] = None,
        current_issues: Optional[List[dict]] = None,
    ) -> List[dict]:
        """
        Helper to get issues from a project, respecting the global limit checkpoint.
        """
        if current_issues is None:
            current_issues = []

        if limit is not None:
            remaining = limit - len(current_issues)
            if remaining <= 0:
                return []
            issues = client.get_all_project_issues(project_id, limit=remaining)
        else:
            issues = client.get_all_project_issues(project_id)

        return issues


class JiraProjectsTable(JiraTableBase):
    """
    Projects table: provides project information for the Jira instance.
    """

    def list(
        self,
        conditions: Optional[List[FilterCondition]] = None,
        limit: Optional[int] = None,
        sort: Optional[List[SortColumn]] = None,
        targets: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        client: Jira = self.handler.connect()

        projects: List[Dict] = []
        conditions = conditions or []

        for condition in conditions:
            if condition.column in ("id", "key"):
                if condition.op == FilterOperator.EQUAL:
                    project = client.get_project(condition.value)
                    projects.append(project)
                elif condition.op == FilterOperator.IN:
                    for project_id in condition.value:
                        project = client.get_project(project_id)
                        projects.append(project)
                else:
                    raise ValueError(
                        f"Unsupported operator {condition.op} for column {condition.column}."
                    )
                condition.applied = True

        if not projects:
            all_projects = client.get_all_projects()
            if limit is not None:
                projects = all_projects[:limit]
            else:
                projects = all_projects

        return self.to_dataframe(projects)

    def get_columns(self) -> List[str]:
        return [
            "id",
            "key",
            "name",
            "projectTypeKey",
            "simplified",
            "style",
            "isPrivate",
            "entityId",
            "uuid",
        ]

    # META TABLES
    @staticmethod
    def meta_get_tables(table_name: str) -> Dict[str, str]:
        if table_name == "projects":
            return {
                "table_name": "projects",
                "table_schma": "jira",
                "table_type": "BASE TABLE",
                "table_description": "Jira projects available to the configured user.",
            }
        return {}

    @staticmethod
    def meta_get_columns(table_name: str) -> List[Dict]:
        if table_name == "projects":
            return [
                {
                    "table_name": "projects",
                    "column_name": "id",
                    "data_type": "TEXT",
                    "description": "Project ID",
                    "is_nullable": False,
                },
                {
                    "table_name": "projects",
                    "column_name": "key",
                    "data_type": "TEXT",
                    "description": "Project key",
                    "is_nullable": False,
                },
                {
                    "table_name": "projects",
                    "column_name": "name",
                    "data_type": "TEXT",
                    "description": "Project name",
                    "is_nullable": False,
                },
                {
                    "table_name": "projects",
                    "column_name": "projectTypeKey",
                    "data_type": "TEXT",
                    "description": "Type of the project",
                    "is_nullable": False,
                },
                {
                    "table_name": "projects",
                    "column_name": "simplified",
                    "data_type": "BOOLEAN",
                    "description": "Whether the project is simplified",
                    "is_nullable": False,
                },
                {
                    "table_name": "projects",
                    "column_name": "style",
                    "data_type": "TEXT",
                    "description": "Style of the project",
                    "is_nullable": False,
                },
                {
                    "table_name": "projects",
                    "column_name": "isPrivate",
                    "data_type": "BOOLEAN",
                    "description": "Whether the project is private",
                    "is_nullable": False,
                },
                {
                    "table_name": "projects",
                    "column_name": "entityId",
                    "data_type": "TEXT",
                    "description": "Entity ID of the project",
                    "is_nullable": False,
                },
                {
                    "table_name": "projects",
                    "column_name": "uuid",
                    "data_type": "TEXT",
                    "description": "UUID of the project",
                    "is_nullable": False,
                },
            ]
        return []


class JiraIssuesTable(JiraIssueFetcherMixin, JiraTableBase):
    """
    Issues table: provides normalized issue data across all projects.

    Designed for:
    - Direct querying of issues.
    - Feeding Knowledge Bases with summary, description, and comments.
    """

    def list(
        self,
        conditions: Optional[List[FilterCondition]] = None,
        limit: Optional[int] = None,
        sort: Optional[List[SortColumn]] = None,
        targets: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        client: Jira = self.handler.connect()

        issues = self._fetch_issues(client, conditions, limit)

        if issues:
            return self.normalize(issues)
        return self.to_dataframe(issues)

    def normalize(self, issues: List[dict]) -> pd.DataFrame:
        """
        Normalize Jira issues into a flat DataFrame schema suitable for SQL and KB usage.
        """
        issues_df = pd.json_normalize(issues)
        issues_df.rename(
            columns={
                "fields.project.id": "project_id",
                "fields.project.key": "project_key",
                "fields.project.name": "project_name",
                "fields.issuetype.name": "issue_type",
                "fields.labels": "labels",
                "fields.components": "components",
                "fields.summary": "summary",
                "fields.description": "description",
                "fields.priority.name": "priority",
                "fields.creator.displayName": "creator",
                "fields.creator.accountId": "creator_account_id",
                "fields.reporter.displayName": "reporter",
                "fields.reporter.accountId": "reporter_account_id",
                "fields.assignee.displayName": "assignee",
                "fields.assignee.accountId": "assignee_account_id",
                "fields.status.name": "status",
                "fields.status.statusCategory.name": "status_category",
                "fields.statuscategorychangedate": "status_category_change_date",
                "fields.duedate": "due_date",
                "fields.created": "created",
                "fields.updated": "updated",
            },
            inplace=True,
            errors="ignore",
        )

        # Flatten list-like fields so type inference keeps them as text columns.
        if "labels" in issues_df.columns:
            issues_df["labels"] = issues_df["labels"].apply(self._join_simple_list)
        if "components" in issues_df.columns:
            issues_df["components"] = issues_df["components"].apply(
                self._join_component_names
            )

        issues_df = issues_df.reindex(columns=self.get_columns(), fill_value=None)

        return issues_df

    def get_columns(self) -> List[str]:
        return [
            "id",
            "key",
            "project_id",
            "project_key",
            "project_name",
            "project",
            "issue_type",
            "summary",
            "description",
            "priority",
            "creator",
            "creator_account_id",
            "reporter",
            "reporter_account_id",
            "assignee",
            "assignee_account_id",
            "status",
            "status_category",
            "status_category_change_date",
            "labels",
            "components",
            "due_date",
            "created",
            "updated",
        ]

    @staticmethod
    def _join_simple_list(values: Optional[Iterable]) -> Optional[str]:
        if isinstance(values, (list, tuple, set)):
            filtered = []
            for val in values:
                if val not in (None, ""):
                    filtered.append(str(val))
            if filtered:
                return ", ".join(filtered)
            return None
        if values in (None, ""):
            return None
        return str(values)

    @staticmethod
    def _join_component_names(values: Optional[Iterable]) -> Optional[str]:
        if isinstance(values, list):
            names: List[str] = []
            for component in values:
                if isinstance(component, dict):
                    name = component.get("name")
                    if name:
                        names.append(name)
                elif component not in (None, ""):
                    names.append(str(component))
            if names:
                return ", ".join(names)
            return None
        if values in (None, ""):
            return None
        return str(values)

    @staticmethod
    def _join_comment_bodies(values: Optional[Iterable]) -> Optional[str]:
        if isinstance(values, list):
            comments: List[str] = []
            for comment in values:
                if isinstance(comment, dict):
                    body = comment.get("body")
                    if body:
                        comments.append(body)
            if comments:
                return "\n\n".join(comments)
            return None
        return None

    # META TABLES
    @staticmethod
    def meta_get_tables(table_name: str) -> Dict[str, str]:
        if table_name == "issues":
            return {
                "table_name": "issues",
                "table_schma": "jira",
                "table_type": "BASE TABLE",
                "table_description": "Jira issues across all projects accessible to the configured user.",
            }
        return {}

    @staticmethod
    def meta_get_columns(table_name: str) -> List[Dict]:
        if table_name == "issues":
            return [
                {
                    "table_name": "issues",
                    "column_name": "id",
                    "data_type": "TEXT",
                    "description": "Issue ID",
                    "is_nullable": False
                },
                {
                    "table_name": "issues",
                    "column_name": "key",
                    "data_type": "TEXT",
                    "description": "Issue key",
                    "is_nullable": False
                },
                {
                    "table_name": "issues",
                    "column_name": "project_id",
                    "data_type": "TEXT",
                    "description": "ID of the project the issue belongs to",
                    "is_nullable": False
                },
                {
                    "table_name": "issues",
                    "column_name": "project_key",
                    "data_type": "TEXT",
                    "description": "Key of the project the issue belongs to",
                    "is_nullable": False
                },
                {
                    "table_name": "issues",
                    "column_name": "project_name",
                    "data_type": "TEXT",
                    "description": "Name of the project the issue belongs to",
                    "is_nullable": False
                },
                {
                    "table_name": "issues",
                    "column_name": "project",
                    "data_type": "TEXT",
                    "description": "Project information as a JSON string",
                    "is_nullable": False
                },
                {
                    "table_name": "issues",
                    "column_name": "issue_type",
                    "data_type": "TEXT",
                    "description": "Type of the issue",
                    "is_nullable": False
                },
                {
                    "table_name": "issues",
                    "column_name": "summary",
                    "data_type": "TEXT",
                    "description": "Summary of the issue",
                    "is_nullable": False
                },
                {
                    "table_name": "issues",
                    "column_name": "description",
                    "data_type": "TEXT",
                    "description": "Description of the issue",
                    "is_nullable": False
                },
                {
                    "table_name": "issues",
                    "column_name": "priority",
                    "data_type": "TEXT",
                    "description": "Priority of the issue",
                    "is_nullable": False
                },
                {
                    "table_name": "issues",
                    "column_name": "creator",
                    "data_type": "TEXT",
                    "description": "Display name of the issue creator",
                    "is_nullable": False
                },
                {
                    "table_name": "issues",
                    "column_name": "creator_account_id",
                    "data_type": "TEXT",
                    "description": "Account ID of the issue creator",
                    "is_nullable": False
                },
                {
                    "table_name": "issues",
                    "column_name": "reporter",
                    "data_type": "TEXT",
                    "description": "Display name of the issue reporter",
                    "is_nullable": False
                },
                {
                    "table_name": "issues",
                    "column_name": "reporter_account_id",
                    "data_type": "TEXT",
                    "description": "Account ID of the issue reporter",
                    "is_nullable": False
                },
                {
                    "table_name": "issues",
                    "column_name": "assignee",
                    "data_type": "TEXT",
                    "description": "Display name of the issue assignee",
                    "is_nullable": False
                },
                {
                    "table_name": "issues",
                    "column_name": "assignee_account_id",
                    "data_type": "TEXT",
                    "description": "Account ID of the issue assignee",
                    "is_nullable": False
                },
                {
                    "table_name": "issues",
                    "column_name": "status",
                    "data_type": "TEXT",
                    "description": "Current status of the issue",
                    "is_nullable": False
                },
                {
                    "table_name": "issues",
                    "column_name": "status_category",
                    "data_type": "TEXT",
                    "description": "Category of the current status",
                    "is_nullable": False
                },
                {
                    "table_name": "issues",
                    "column_name": "status_category_change_date",
                    "data_type": "TIMESTAMP",
                    "description": "Date when the status category last changed",
                    "is_nullable": False
                },
                {
                    "table_name": "issues",
                    "column_name": "labels",
                    "data_type": "TEXT",
                    "description": "Comma-separated labels associated with the issue",
                    "is_nullable": False
                },
                {
                    "table_name": "issues",
                    "column_name": "components",
                    "data_type": "TEXT",
                    "description": "Comma-separated component names associated with the issue.,
                    "is_nullable": False
                },
                {
                    "table_name": "issues",
                    "column_name": "due_date",
                    "data_type": "DATE",
                    "description": "Due date of the issue",
                },
                {
                    "table_name": "issues",
                    "column_name": "created",
                    "data_type": "TIMESTAMP",
                    "description": "Creation date of the issue",
                    "is_nullable": False
                },
                {
                    "table_name": "issues",
                    "column_name": "updated",
                    "data_type": "TIMESTAMP",
                    "description": "Last updated date of the issue",
                    "is_nullable": False
                },
            ]
        return []


class JiraAttachmentsTable(JiraIssueFetcherMixin, JiraTableBase):
    """
    Attachments table: derived from issue attachments.
    """

    def list(
        self,
        conditions: Optional[List[FilterCondition]] = None,
        limit: Optional[int] = None,
        sort: Optional[List[SortColumn]] = None,
        targets: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        client: Jira = self.handler.connect()
        issues = self._fetch_issues(client, conditions, None)
        attachment_rows = self._build_attachment_rows(client, issues, limit)
        return self.to_dataframe(attachment_rows)

    def _build_attachment_rows(
        self,
        client: Jira,
        issues: List[Dict],
        limit: Optional[int],
    ) -> List[Dict]:
        attachments: List[Dict] = []

        for issue in issues:
            if not isinstance(issue, dict):
                continue

            issue_attachments = self._get_issue_field(client, issue, "attachment") or []
            if not isinstance(issue_attachments, list):
                continue

            for attachment in issue_attachments:
                if not isinstance(attachment, dict):
                    continue

                row = {
                    "issue_id": issue.get("id"),
                    "issue_key": issue.get("key"),
                    "attachment_id": attachment.get("id"),
                    "filename": attachment.get("filename"),
                    "mime_type": attachment.get("mimeType"),
                    "size": attachment.get("size"),
                    "content_url": attachment.get("content"),
                    "thumbnail_url": attachment.get("thumbnail"),
                    "created": attachment.get("created"),
                    "author": (attachment.get("author") or {}).get("displayName"),
                    "author_account_id": (attachment.get("author") or {}).get(
                        "accountId"
                    ),
                }
                attachments.append(row)

                if limit is not None and len(attachments) >= limit:
                    return attachments

        return attachments

    def get_columns(self) -> List[str]:
        return [
            "issue_id",
            "issue_key",
            "attachment_id",
            "filename",
            "mime_type",
            "size",
            "content_url",
            "thumbnail_url",
            "created",
            "author",
            "author_account_id",
        ]

    # META TABLES
    @staticmethod
    def meta_get_tables(table_name: str) -> Dict[str, str]:
        if table_name == "attachments":
            return {
                "table_name": "attachments",
                "table_schma": "jira",
                "table_type": "BASE TABLE",
                "table_description": "Jira issue attachments across all projects accessible to the configured user.",
            }
        return {}
    
    @staticmethod
    def meta_get_columns(table_name: str) -> List[Dict]:
        if table_name == "attachments":
            return [
                {
                    "table_name": "attachments",
                    "column_name": "issue_id",
                    "data_type": "TEXT",
                    "description": "ID of the issue the attachment belongs to",
                    "is_nullable": False
                },
                {
                    "table_name": "attachments",
                    "column_name": "issue_key",
                    "data_type": "TEXT",
                    "description": "Key of the issue the attachment belongs to",
                    "is_nullable": False
                },
                {
                    "table_name": "attachments",
                    "column_name": "attachment_id",
                    "data_type": "TEXT",
                    "description": "ID of the attachment",
                    "is_nullable": False
                },
                {
                    "table_name": "attachments",
                    "column_name": "filename",
                    "data_type": "TEXT",
                    "description": "Filename of the attachment",
                    "is_nullable": False
                },
                {
                    "table_name": "attachments",
                    "column_name": "mime_type",
                    "data_type": "TEXT",
                    "description": "MIME type of the attachment",
                    "is_nullable": False
                },
                {
                    "table_name": "attachments",
                    "column_name": "size",
                    "data_type": "INTEGER",
                    "description": "Size of the attachment in bytes",
                    "is_nullable": False
                },
                {
                    "table_name": "attachments",
                    "column_name": "content_url",
                    "data_type": "TEXT",
                    "description": "URL to access the content of the attachment",
                    "is_nullable": False
                },
                {
                    "table_name": "attachments",
                    "column_name": "thumbnail_url",
                    "data_type": "TEXT",
                    "description": "URL to access the thumbnail of the attachment",
                    "is_nullable": False
                },
                {
                    "table_name": "attachments",
                    "column_name": "created",
                    "data_type": "TIMESTAMP",
                    "description":  "Creation date of the attachment",
                    "is_nullable": False
                },
                {
                    "table_name": "attachments",
                    "column_name": "author",
                    "data_type": "TEXT",
                    "description": "Display name of the attachment author",
                    "is_nullable": False
                },
                {
                    "table_name": "attachments",
                    "column_name": "author_account_id",
                    "data_type": "TEXT",
                    "description": "Account ID of the attachment author",
                    "is_nullable": False
                }
            ]
        return []

class JiraCommentsTable(JiraIssueFetcherMixin, JiraTableBase):
    """
    Comments table: derived from issue comments.
    """

    def list(
        self,
        conditions: Optional[List[FilterCondition]] = None,
        limit: Optional[int] = None,
        sort: Optional[List[SortColumn]] = None,
        targets: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        client: Jira = self.handler.connect()
        issues = self._fetch_issues(client, conditions, None)
        comment_rows = self._build_comment_rows(client, issues, limit)
        return self.to_dataframe(comment_rows)

    def _build_comment_rows(
        self,
        client: Jira,
        issues: List[Dict],
        limit: Optional[int],
    ) -> List[Dict]:
        comments_rows: List[Dict] = []

        for issue in issues:
            if not isinstance(issue, dict):
                continue

            comments_container = self._get_issue_field(client, issue, "comment") or {}
            if isinstance(comments_container, dict):
                issue_comments = comments_container.get("comments", [])
            else:
                issue_comments = []

            for comment in issue_comments:
                if not isinstance(comment, dict):
                    continue

                row = {
                    "issue_id": issue.get("id"),
                    "issue_key": issue.get("key"),
                    "comment_id": comment.get("id"),
                    "body": comment.get("body"),
                    "created": comment.get("created"),
                    "updated": comment.get("updated"),
                    "author": (comment.get("author") or {}).get("displayName"),
                    "author_account_id": (comment.get("author") or {}).get("accountId"),
                    "visibility_type": (comment.get("visibility") or {}).get("type"),
                    "visibility_value": (comment.get("visibility") or {}).get("value"),
                }
                comments_rows.append(row)

                if limit is not None and len(comments_rows) >= limit:
                    return comments_rows

        return comments_rows

    def get_columns(self) -> List[str]:
        return [
            "issue_id",
            "issue_key",
            "comment_id",
            "body",
            "created",
            "updated",
            "author",
            "author_account_id",
            "visibility_type",
            "visibility_value",
        ]
    
    # META TABLES
    @staticmethod
    def meta_get_tables(table_name: str) -> Dict[str, str]:
        if table_name == "comments":
            return {
                "table_name": "comments",
                "table_schma": "jira",
                "table_type": "BASE TABLE",
                "table_description": "Jira issue comments across all projects accessible to the configured user.",
            }
        return {}
    
    @staticmethod
    def meta_get_columns(table_name: str) -> List[Dict]:
        if table_name == "comments":
            return [
                {
                    "table_name": "comments",
                    "column_name": "issue_id",
                    "data_type": "TEXT",
                    "description": "ID of the issue the comment belongs to",
                    "is_nullable": False
                },
                {
                    "table_name": "comments",
                    "column_name": "issue_key",
                    "data_type": "TEXT",
                    "description": "Key of the issue the comment belongs to",
                    "is_nullable": False
                },
                {
                    "table_name": "comments",
                    "column_name": "comment_id",
                    "data_type": "TEXT",
                    "description": "ID of the comment",
                    "is_nullable": False
                },
                {
                    "table_name": "comments",
                    "column_name": "body",
                    "data_type": "TEXT",
                    "description": "Body of the comment",
                    "is_nullable": False
                },
                {
                    "table_name": "comments",
                    "column_name": "created",
                    "data_type": "TIMESTAMP",
                    "description": "Creation date of the comment",
                    "is_nullable": False
                },
                {
                    "table_name": "comments",
                    "column_name": "updated",
                    "data_type": "TIMESTAMP",
                    "description":  "Last updated date of the comment",
                    "is_nullable": False
                },
                {
                    "table_name": "comments",
                    "column_name": "author",
                    "data_type": "TEXT",
                    "description": "Display name of the comment author",
                    "is_nullable": False
                },
                {
                    "table_name": "comments",
                    "column_name": "author_account_id",
                    "data_type": "TEXT",
                    "description": "Account ID of the comment author",
                    "is_nullable": False
                },
                {
                    "table_name": "comments",
                    "column_name": "visibility_type",
                    "data_type": "TEXT",
                    "description": "Type of visibility for the comment",
                    "is_nullable": False
                },
                {
                    "table_name": "comments",
                    "column_name": "visibility_value",
                    "data_type": "TEXT",
                    "description": "Value of visibility for the comment",
                    "is_nullable": False
                }
            ]
        return []


class JiraGroupsTable(JiraTableBase):
    """
    Groups table: user groups available in Jira.
    """

    def list(
        self,
        conditions: Optional[List[FilterCondition]] = None,
        limit: Optional[int] = None,
        sort: Optional[List[SortColumn]] = None,
        targets: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        client: Jira = self.handler.connect()

        if limit is not None:
            group_response = client.get_groups(limit=limit)
        else:
            group_response = client.get_groups()

        groups = group_response.get("groups", [])

        return self.to_dataframe(groups)

    def get_columns(self) -> List[str]:
        return [
            "groupId",
            "name",
            "html",
        ]
    

    #META TABLES
    @staticmethod
    def meta_get_tables(table_name: str) -> Dict[str, str]:
        if table_name == "groups":
            return {
                "table_name": "groups",
                "table_schma": "jira",
                "table_type": "BASE TABLE",
                "table_description": "Jira user groups available to the configured user.",
            }
        return {}
    
    @staticmethod
    def meta_get_columns(table_name: str) -> List[Dict]:
        if table_name == "groups":
            return [
                {
                    "table_name": "groups",
                    "column_name": "groupId",
                    "data_type": "TEXT",
                    "description": "Group ID",
                    "is_nullable": False
                },
                {
                    "table_name": "groups",
                    "column_name": "name",
                    "data_type": "TEXT",
                    "description": "Group name",
                    "is_nullable": False
                },
                {
                    "table_name": "groups",
                    "column_name": "html",
                    "data_type": "TEXT",
                    "description": "HTML representation of the group",
                    "is_nullable": False
                },
            ]
        return []


class JiraUsersTable(JiraTableBase):
    """
    Users table: users accessible to the current Jira context.
    """

    def list(
        self,
        conditions: Optional[List[FilterCondition]] = None,
        limit: Optional[int] = None,
        sort: Optional[List[SortColumn]] = None,
        targets: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        client: Jira = self.handler.connect()

        is_cloud = getattr(client, "cloud", None)
        if is_cloud is False:
            self._column_mode = "server"
            users = self.get_server_users(client, conditions, limit)
        else:
            self._column_mode = "cloud"
            users = self.get_cloud_users(client, conditions, limit)

        return self.to_dataframe(users)

    def get_cloud_users(
        self,
        client: Jira,
        conditions: Optional[List[FilterCondition]],
        limit: Optional[int],
    ) -> List[Dict]:
        users: List[Dict] = []
        conditions = conditions or []

        for condition in conditions:
            if condition.column == "accountId":
                if condition.op == FilterOperator.EQUAL:
                    user = client.user(account_id=condition.value)
                    if isinstance(user, dict):
                        users.append(user)
                    else:
                        logger.debug(
                            "Skipping non-dict user result for account_id %s: %s",
                            condition.value,
                            type(user).__name__,
                        )
                elif condition.op == FilterOperator.IN:
                    for account_id in condition.value:
                        user = client.user(account_id=account_id)
                        if isinstance(user, dict):
                            users.append(user)
                        else:
                            logger.debug(
                                "Skipping non-dict user result for account_id %s: %s",
                                account_id,
                                type(user).__name__,
                            )
                else:
                    raise ValueError(
                        f"Unsupported operator {condition.op} for column {condition.column}."
                    )
                condition.applied = True

        if not users:
            users = self._fetch_all_users(client, limit)

        return users

    def get_server_users(
        self,
        client: Jira,
        conditions: Optional[List[FilterCondition]],
        limit: Optional[int],
    ) -> List[Dict]:
        users: List[Dict] = []
        conditions = conditions or []

        for condition in conditions:
            if condition.column in ("username", "name", "accountId"):
                if condition.op == FilterOperator.IN:
                    values = (
                        condition.value
                        if isinstance(condition.value, (list, tuple, set))
                        else [condition.value]
                    )
                elif condition.op == FilterOperator.EQUAL:
                    values = [condition.value]
                else:
                    raise ValueError(
                        f"Unsupported operator {condition.op} for column {condition.column}."
                    )
                for value in values:
                    try:
                        user = client.user(username=value)
                    except HTTPError as user_error:
                        logger.debug(
                            "Failed to fetch server user '%s': %s", value, user_error
                        )
                        continue
                    if isinstance(user, dict):
                        users.append(user)
                condition.applied = True

        if not users:
            try:
                user = client.user(username=".")
                if isinstance(user, dict):
                    users.append(user)
            except HTTPError as user_error:
                logger.debug(
                    "Failed to fetch default server user '%s': %s", ".", user_error
                )
        if not users:
            users = self._fetch_all_users(client, limit)

        return users

    def _fetch_all_users(self, client: Jira, limit: Optional[int]) -> List[Dict]:
        """
        Fetch all accessible users with pagination and a fallback for Jira Cloud.
        """
        users: List[Dict] = []
        start = 0
        page_size = limit or 50
        if page_size <= 0:
            page_size = 50

        while True:
            try:
                resp = client.users_get_all(start=start, limit=page_size)
                page_users = self._normalize_users_response(resp)
                if not isinstance(resp, (list, dict)) and not page_users:
                    raise HTTPError(f"Unexpected users response: {resp}")
            except HTTPError as exc:
                logger.warning(
                    "users_get_all failed (start=%s, limit=%s): %s; falling back to user search",
                    start,
                    page_size,
                    exc,
                )
                resp, page_users = self._fallback_user_search(
                    client, start, page_size, exc
                )

            users.extend(page_users)

            if limit is not None and len(users) >= limit:
                return users[:limit]

            if len(page_users) < page_size:
                break

            start += len(page_users)

        return users

    def _fallback_user_search(
        self, client: Jira, start: int, page_size: int, original_exc: HTTPError
    ) -> Tuple[Any, List[Dict]]:
        """
        Jira user search using both cloud and server parameter styles.
        """
        is_cloud = getattr(client, "cloud", None)
        search_variants: List[Dict[str, Any]] = []

        if is_cloud is False:
            search_variants.append(
                {"username": ".", "start": start, "limit": page_size}
            )
            search_variants.append({"query": ".", "start": start, "limit": page_size})
        else:
            search_variants.append({"query": ".", "start": start, "limit": page_size})
            search_variants.append(
                {"username": ".", "start": start, "limit": page_size}
            )

        for params in search_variants:
            try:
                resp = client.user_find_by_user_string(**params)
            except HTTPError as search_exc:
                logger.error(
                    "user search failed (params=%s): %s",
                    params,
                    search_exc,
                )
                continue

            page_users = self._normalize_users_response(resp)
            if isinstance(resp, (list, dict)) or page_users:
                return resp, page_users

            logger.debug(
                "Unexpected users search response (params=%s): %s",
                params,
                resp,
            )

        raise HTTPError(f"Unexpected users response: {original_exc}")

    def _normalize_users_response(self, resp: Any) -> List[Dict]:
        """
        Normalize user API responses to a list of dicts.
        """
        if isinstance(resp, list):
            return resp
        if isinstance(resp, dict):
            users = resp.get("users") or resp.get("values") or []
            if users:
                return users
            if resp:
                return [resp]
            return []
        logger.debug("Unexpected users response type: %s", type(resp).__name__)
        return []

    def get_columns(self) -> List[str]:
        column_mode = getattr(self, "_column_mode", "cloud")
        if column_mode == "server":
            return SERVER_COLUMNS
        return CLOUD_COLUMNS
    
    # META TABLES
    @staticmethod
    def meta_get_tables(table_name: str) -> Dict[str, str]:
        raise NotImplementedError("Meta tables not implemented for JiraUsersTable.")
    
    @staticmethod
    def meta_get_columns(table_name: str) -> List[Dict]:
        raise NotImplementedError("Meta tables not implemented for JiraUsersTable.")
    