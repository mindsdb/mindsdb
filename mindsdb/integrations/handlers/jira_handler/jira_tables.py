from typing import List

from atlassian import Jira
import pandas as pd

from mindsdb.integrations.libs.api_handler import APIResource
from mindsdb.integrations.utilities.sql_utils import FilterCondition, SortColumn, FilterOperator
from mindsdb.utilities import log


logger = log.getLogger(__name__)


class JiraProjectsTable(APIResource):
    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[str] = None,
        **kwargs
    ) -> pd.DataFrame:
        client = self.handler.connect()

        projects = []
        for condition in conditions:
            if condition.column in ('id', 'key'): 
                if condition.op == FilterOperator.EQUAL:
                    projects = [client.get_project(condition.value)]
                elif condition.op == FilterOperator.IN:
                    projects = [client.get_project(project_id) for project_id in condition.value]
                condition.applied = True

        if not projects:
            # NOTE: Does this have a limit? Paging?
            projects = client.get_all_projects()

        if projects:
            projects_df = pd.DataFrame(projects)
            projects_df = projects_df[self.get_columns()]
        else:
            projects_df = pd.DataFrame([], columns=self.get_columns())

        return projects_df
    
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
    

class JiraIssuesTable(APIResource):
    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[str] = None,
        **kwargs
    ) -> pd.DataFrame:
        client = self.handler.connect()

        issues = []
        are_conditions_valid = False
        for condition in conditions:
            if condition.column in ('id', 'key'): 
                if condition.op == FilterOperator.EQUAL:
                    issues = [client.get_issue(condition.value)]
                elif condition.op == FilterOperator.IN:
                    issues = [client.get_issue(issue_id) for issue_id in condition.value]
                condition.applied = True
                are_conditions_valid = True

            elif condition.column in ('project_id', 'project_key', 'project_name'):
                if condition.op == FilterOperator.EQUAL:
                    issues = client.get_all_project_issues(condition.value, limit=limit)
                elif condition.op == FilterOperator.IN:
                    for project_id in condition.value:
                        if limit:
                            # NOTE: Does this have a limit? Paging?
                            issues.extend(client.get_all_project_issues(project_id, limit=limit - len(issues)))
                            if len(issues) >= limit:
                                break
                        else:
                            issues.extend(client.get_all_project_issues(project_id))

                condition.applied = True
                are_conditions_valid = True

        if not are_conditions_valid:
            raise ValueError("Either the issue 'id', 'key' or one of 'project_id', 'project_key', 'project_name' must be provided.")

        if issues:
            issues_df = pd.json_normalize(issues)
            issues_df.rename(columns={
                "fields.project.id": "project_id",
                "fields.project.key": "project_key",
                "fields.project.name": "project_name",
                "fields.summary": "summary",
                "fields.priority.name": "priority",
                "fields.creator.displayName": "creator",
                "fields.assignee.displayName": "assignee",
                "fields.status.name": "status",
            }, inplace=True)
            issues_df = issues_df[self.get_columns()]
        else:
            issues_df = pd.DataFrame([], columns=self.get_columns())

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

