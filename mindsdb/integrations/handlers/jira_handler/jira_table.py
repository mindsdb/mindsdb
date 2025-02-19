from typing import List

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
    ):
        client = self.handler.connect()

        projects = []
        for condition in conditions:
            if (condition.column in ('id', 'key') and condition.op == FilterOperator.EQUAL):
                projects = [client.get_project(condition.value)]
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
    ):
        client = self.handler.connect()

        issues = []
        for condition in conditions:
            if (condition.column in ('id', 'key') and condition.op == FilterOperator.EQUAL):
                issues = [client.get_issue(condition.value)]
                condition.applied = True

            if condition.column in ('project_id', 'project_key', 'project_name') and condition.op == FilterOperator.EQUAL:
                issues = client.get_all_project_issues(condition.value)
                condition.applied = True

        if not issues:
            raise ValueError("Either the issue 'id', 'key' or one of 'project_id', 'project_key', 'project_name' must be provided.")

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

        return issues_df

    def get_columns(self):
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

