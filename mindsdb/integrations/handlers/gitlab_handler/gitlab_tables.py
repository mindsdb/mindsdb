import pandas as pd

from typing import List

from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.utilities import log

from mindsdb_sql_parser import ast

logger = log.getLogger(__name__)


class GitlabIssuesTable(APITable):
    """The GitLab Issue Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the GitLab "List repository issues" API
        Args:
            query: SELECT
        Returns:
            DataFrame
        Raises:
            ValueError
        """

        conditions = extract_comparison_conditions(query.where)

        if query.limit:
            total_results = query.limit.value
        else:
            total_results = 20

        issues_kwargs = {}
        order_by_conditions = {}

        if query.order_by and len(query.order_by) > 0:
            order_by_conditions["columns"] = []
            order_by_conditions["ascending"] = []

            for an_order in query.order_by:
                if an_order.field.parts[1] in self.get_columns():
                    order_by_conditions["columns"].append(an_order.field.parts[1])

                    if an_order.direction == "ASC":
                        order_by_conditions["ascending"].append(True)
                    else:
                        order_by_conditions["ascending"].append(False)
                else:
                    raise ValueError(
                        f"Order by unknown column {an_order.field.parts[1]}"
                    )

        for a_where in conditions:
            if a_where[1] == "state":
                if a_where[0] != "=":
                    raise ValueError("Unsupported where operation for state")
                if a_where[2] not in ["opened", "closed", "all"]:
                    raise ValueError(
                        f"Unsupported where argument for state {a_where[2]}"
                    )

                issues_kwargs["state"] = a_where[2]

                continue
            if a_where[1] == "labels":
                if a_where[0] != "=":
                    raise ValueError("Unsupported where operation for state")

                issues_kwargs["labels"] = a_where[2].split(",")

                continue
            if a_where[1] in ["assignee", "creator"]:
                if a_where[0] != "=":
                    raise ValueError(f"Unsupported where operation for {a_where[1]}")

                issues_kwargs[a_where[1]] = a_where[2]
            else:
                raise ValueError(f"Unsupported where argument {a_where[1]}")

        self.handler.connect()

        gitlab_issues_df = pd.DataFrame(columns=self.get_columns())

        issues_kwargs["per_page"] = total_results
        issues_kwargs["get_all"] = False
        while True:
            try:
                for issue in self.handler.connection.projects.get(
                    self.handler.repository
                ).issues.list(**issues_kwargs):

                    logger.debug(f"Processing issue {issue.iid}")

                    gitlab_issues_df = pd.concat(
                        [
                            gitlab_issues_df,
                            pd.DataFrame(
                                [
                                    {
                                        "number": issue.iid,
                                        "title": issue.title,
                                        "state": issue.state,
                                        "creator": issue.author["name"],
                                        "closed_by": issue.closed_by
                                        if issue.closed_by
                                        else None,
                                        "labels": ",".join(
                                            [label for label in issue.labels]
                                        ),
                                        "assignees": ",".join(
                                            [
                                                assignee["name"]
                                                for assignee in issue.assignees
                                            ]
                                        ),
                                        "body": issue.description,
                                        "created": issue.created_at,
                                        "updated": issue.updated_at,
                                        "closed": issue.closed_at,
                                    }
                                ]
                            ),
                        ]
                    )

                    if gitlab_issues_df.shape[0] >= total_results:
                        break
            except IndexError:
                break

            if gitlab_issues_df.shape[0] >= total_results:
                break
            else:
                break

        selected_columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                selected_columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                selected_columns.append(target.parts[-1])
            else:
                raise ValueError(f"Unknown query target {type(target)}")

        if len(gitlab_issues_df) == 0:
            gitlab_issues_df = pd.DataFrame([], columns=selected_columns)
        else:
            gitlab_issues_df.columns = self.get_columns()
            for col in set(gitlab_issues_df.columns).difference(set(selected_columns)):
                gitlab_issues_df = gitlab_issues_df.drop(col, axis=1)

            if len(order_by_conditions.get("columns", [])) > 0:
                gitlab_issues_df = gitlab_issues_df.sort_values(
                    by=order_by_conditions["columns"],
                    ascending=order_by_conditions["ascending"],
                )

        return gitlab_issues_df

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses
        Returns
        -------
        List[str]: list of columns
        """

        return [
            "number",
            "title",
            "state",
            "creator",
            "closed_by",
            "labels",
            "assignees",
            "body",
            "created",
            "updated",
            "closed",
        ]


class GitlabMergeRequestsTable(APITable):
    """The GitLab Merge Requests Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the GitLab "List repository rerge requests" API
        Args:
            query: SELECT
        Returns:
            DataFrame
        Raises:
            ValueError
        """

        conditions = extract_comparison_conditions(query.where)

        if query.limit:
            total_results = query.limit.value
        else:
            total_results = 20

        merge_requests_kwargs = {}
        order_by_conditions = {}

        if query.order_by and len(query.order_by) > 0:
            order_by_conditions["columns"] = []
            order_by_conditions["ascending"] = []

            for an_order in query.order_by:
                if an_order.field.parts[1] in self.get_columns():
                    order_by_conditions["columns"].append(an_order.field.parts[1])

                    if an_order.direction == "ASC":
                        order_by_conditions["ascending"].append(True)
                    else:
                        order_by_conditions["ascending"].append(False)
                else:
                    raise ValueError(
                        f"Order by unknown column {an_order.field.parts[1]}"
                    )

        for a_where in conditions:
            if a_where[1] == "state":
                if a_where[0] != "=":
                    raise ValueError("Unsupported where operation for state")
                if a_where[2] not in ["opened", "closed", "merged", "all"]:
                    raise ValueError(
                        f"Unsupported where argument for state {a_where[2]}"
                    )

                merge_requests_kwargs["state"] = a_where[2]

                continue
            if a_where[1] == "labels":
                if a_where[0] != "=":
                    raise ValueError("Unsupported where operation for labels")

                merge_requests_kwargs["labels"] = a_where[2].split(",")

                continue
            if a_where[1] in ["target_branch", "source_branch"]:
                if a_where[0] != "=":
                    raise ValueError(f"Unsupported where operation for {a_where[1]}")

                merge_requests_kwargs[a_where[1]] = a_where[2]
            else:
                raise ValueError(f"Unsupported where argument {a_where[1]}")

        self.handler.connect()

        gitlab_merge_requests_df = pd.DataFrame(columns=self.get_columns())

        merge_requests_kwargs["per_page"] = total_results
        merge_requests_kwargs["get_all"] = False
        while True:
            try:
                for merge_request in self.handler.connection.projects.get(
                    self.handler.repository
                ).mergerequests.list(**merge_requests_kwargs):

                    logger.debug(f"Processing merge request {merge_request.iid}")

                    gitlab_merge_requests_df = pd.concat(
                        [
                            gitlab_merge_requests_df,
                            pd.DataFrame(
                                [
                                    {
                                        "number": merge_request.iid,
                                        "title": merge_request.title,
                                        "state": merge_request.state,
                                        "creator": merge_request.author["name"],
                                        "closed_by": merge_request.closed_by
                                        if merge_request.closed_by
                                        else None,
                                        "mergeed_by": merge_request.merge_user["name"]
                                        if merge_request.merge_user
                                        else None,
                                        "labels": ",".join(
                                            [label for label in merge_request.labels]
                                        ),
                                        "assignees": ",".join(
                                            [
                                                assignee["name"]
                                                for assignee in merge_request.assignees
                                            ]
                                        ),
                                        "reviewers": ",".join(
                                            [
                                                reviewer["name"]
                                                for reviewer in merge_request.reviewers
                                            ]
                                        ),
                                        "body": merge_request.description,
                                        "target_branch": merge_request.target_branch,
                                        "source_branch": merge_request.source_branch,
                                        "upvotes": merge_request.upvotes,
                                        "downvotes": merge_request.downvotes,
                                        "draft": merge_request.draft,
                                        "work_in_progress": merge_request.work_in_progress,
                                        "milestone": merge_request.milestone["state"]
                                        if merge_request.milestone
                                        else None,
                                        "merge_status": merge_request.merge_status,
                                        "detailed_merge_status": merge_request.detailed_merge_status,
                                        "user_notes_count": merge_request.user_notes_count,
                                        "has_conflicts": merge_request.has_conflicts,
                                        "blocking_discussions_resolved": merge_request.blocking_discussions_resolved,
                                        "created": merge_request.created_at,
                                        "updated": merge_request.updated_at,
                                        "closed": merge_request.closed_at,
                                        "merged": merge_request.merged_at,
                                    }
                                ]
                            ),
                        ]
                    )

                    if gitlab_merge_requests_df.shape[0] >= total_results:
                        break
            except IndexError:
                break

            if gitlab_merge_requests_df.shape[0] >= total_results:
                break
            else:
                break

        selected_columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                selected_columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                selected_columns.append(target.parts[-1])
            else:
                raise ValueError(f"Unknown query target {type(target)}")

        if len(gitlab_merge_requests_df) == 0:
            gitlab_merge_requests_df = pd.DataFrame([], columns=selected_columns)
        else:
            gitlab_merge_requests_df.columns = self.get_columns()
            for col in set(gitlab_merge_requests_df.columns).difference(set(selected_columns)):
                gitlab_merge_requests_df = gitlab_merge_requests_df.drop(col, axis=1)

            if len(order_by_conditions.get("columns", [])) > 0:
                gitlab_merge_requests_df = gitlab_merge_requests_df.sort_values(
                    by=order_by_conditions["columns"],
                    ascending=order_by_conditions["ascending"],
                )

        return gitlab_merge_requests_df

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses
        Returns
        -------
        List[str]: list of columns
        """

        return [
            "number",
            "title",
            "state",
            "creator",
            "closed_by",
            "mergeed_by",
            "labels",
            "assignees",
            "reviewers",
            "body",
            "target_branch",
            "source_branch",
            "upvotes",
            "downvotes",
            "draft",
            "work_in_progress",
            "milestone",
            "merge_status",
            "detailed_merge_status",
            "user_notes_count",
            "has_conflicts",
            "blocking_discussions_resolved",
            "created",
            "updated",
            "closed",
            "merged",
        ]
