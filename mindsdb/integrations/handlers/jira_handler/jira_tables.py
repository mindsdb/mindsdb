from typing import List, Optional

from atlassian import Jira
import pandas as pd

from mindsdb.integrations.libs.api_handler import APIResource
from mindsdb.integrations.utilities.sql_utils import FilterCondition, SortColumn, FilterOperator
from mindsdb.utilities import log


logger = log.getLogger(__name__)


class JiraTableBase(APIResource):
    """Base class for Jira tables"""

    def to_dataframe(self, records: Optional[List[dict]]) -> pd.DataFrame:
        """
        Convert records to DataFrame with fixed columns, handling missing optional fields.

        Args:
            records: List of record dictionaries from Jira API, or None/empty list

        Returns:
            DataFrame with all expected columns, missing fields filled with None
        """
        if records:
            df = pd.DataFrame(records)
            df = df.reindex(columns=self.get_columns(), fill_value=None)
        else:
            df = pd.DataFrame([], columns=self.get_columns())
        return df


class JiraProjectsTable(JiraTableBase):
    def list(
        self,
        conditions: Optional[List[FilterCondition]] = None,
        limit: Optional[int] = None,
        sort: Optional[List[SortColumn]] = None,
        targets: Optional[List[str]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        client: Jira = self.handler.connect()

        projects = []
        conditions = conditions or []
        for condition in conditions:
            if condition.column in ("id", "key"):
                if condition.op == FilterOperator.EQUAL:
                    projects = [client.get_project(condition.value)]
                elif condition.op == FilterOperator.IN:
                    projects = [client.get_project(project_id) for project_id in condition.value]
                else:
                    raise ValueError(f"Unsupported operator {condition.op} for column {condition.column}.")
                condition.applied = True

        if not projects:
            projects = client.get_all_projects()

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


class JiraIssuesTable(JiraTableBase):
    def list(
        self,
        conditions: Optional[List[FilterCondition]] = None,
        limit: Optional[int] = None,
        sort: Optional[List[SortColumn]] = None,
        targets: Optional[List[str]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        client: Jira = self.handler.connect()

        issues = []
        conditions = conditions or []
        for condition in conditions:
            if condition.column in ("id", "key"):
                if condition.op == FilterOperator.EQUAL:
                    issues = [client.get_issue(condition.value)]
                elif condition.op == FilterOperator.IN:
                    issues = [client.get_issue(issue_id) for issue_id in condition.value]
                else:
                    raise ValueError(f"Unsupported operator {condition.op} for column {condition.column}.")
                condition.applied = True

            elif condition.column in ("project_id", "project_key", "project_name"):
                if condition.op == FilterOperator.EQUAL:
                    issues = client.get_all_project_issues(condition.value, limit=limit)
                elif condition.op == FilterOperator.IN:
                    for project_id in condition.value:
                        issues.extend(client.get_all_project_issues(project_id, limit=limit))

                condition.applied = True

        if not issues:
            project_ids = [project["id"] for project in client.get_all_projects()]
            for project_id in project_ids:
                issues.extend(
                    self._get_project_issues_with_limit(client, project_id, limit=limit, current_issues=issues)
                )

        if issues:
            return self.normalize(issues)
        else:
            return self.to_dataframe(issues)

    def _get_project_issues_with_limit(
        self,
        client: Jira,
        project_id: str,
        limit: Optional[int] = None,
        current_issues: Optional[List] = None,
    ):
        """
        Helper to get issues from a project, respecting the limit.
        """
        if current_issues is None:
            current_issues = []
        if limit:
            remaining = limit - len(current_issues)
            if remaining <= 0:
                return []
            return client.get_all_project_issues(project_id, limit=remaining)
        else:
            return client.get_all_project_issues(project_id)

    def normalize(self, issues: dict) -> pd.DataFrame:
        issues_df = pd.json_normalize(issues)
        # Use errors='ignore' to skip columns that don't exist in the data
        issues_df.rename(
            columns={
                "fields.project.id": "project_id",
                "fields.project.key": "project_key",
                "fields.project.name": "project_name",
                "fields.summary": "summary",
                "fields.priority.name": "priority",
                "fields.creator.displayName": "creator",
                "fields.assignee.displayName": "assignee",
                "fields.status.name": "status",
            },
            inplace=True,
            errors="ignore",
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
            "summary",
            "priority",
            "creator",
            "assignee",
            "status",
        ]


class JiraGroupsTable(JiraTableBase):
    def list(
        self,
        conditions: Optional[List[FilterCondition]] = None,
        limit: Optional[int] = None,
        sort: Optional[List[SortColumn]] = None,
        targets: Optional[List[str]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        client: Jira = self.handler.connect()

        if limit:
            groups = client.get_groups(limit=limit)["groups"]
        else:
            groups = client.get_groups()["groups"]

        return self.to_dataframe(groups)

    def get_columns(self) -> List[str]:
        return [
            "groupId",
            "name",
            "html",
        ]


class JiraUsersTable(JiraTableBase):
    def list(
        self,
        conditions: Optional[List[FilterCondition]] = None,
        limit: Optional[int] = None,
        sort: Optional[List[SortColumn]] = None,
        targets: Optional[List[str]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        client: Jira = self.handler.connect()

        users = []
        conditions = conditions or []
        for condition in conditions:
            if condition.column == "accountId":
                if condition.op == FilterOperator.EQUAL:
                    users = [client.user(account_id=condition.value)]
                elif condition.op == FilterOperator.IN:
                    users = [client.user(account_id=accountId) for accountId in condition.value]
                else:
                    raise ValueError(f"Unsupported operator {condition.op} for column {condition.column}.")
                condition.applied = True

        if not users:
            if limit:
                users = client.users_get_all(limit=limit)
            else:
                users = client.users_get_all()

        return self.to_dataframe(users)

    def get_columns(self) -> List[str]:
        return [
            "accountId",
            "accountType",
            "emailAddress",
            "displayName",
            "active",
            "timeZone",
            "locale",
        ]
