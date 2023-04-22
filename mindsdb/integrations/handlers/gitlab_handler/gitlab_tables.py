import pandas as pd

from typing import List

from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.utilities.log import get_log

from mindsdb_sql.parser import ast

logger = get_log("integrations.gitlab_handler")

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
                if an_order.field.parts[0] != "issues":
                    next

                if an_order.field.parts[1] in ["created", "updated"]:
                    if issues_kwargs != {}:
                        raise ValueError(
                            "Duplicate order conditions found for created/updated"
                        )

                    issues_kwargs["sort"] = an_order.field.parts[1]
                    issues_kwargs["direction"] = an_order.direction
                elif an_order.field.parts[1] in self.get_columns():
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
                if a_where[2] not in ["open", "closed", "all"]:
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

        start = True
        while start:
            try:
                issues = self.handler.connection.projects.get(
                    self.handler.repository
                ).issues.list()
                for issue in issues:

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
                start = False

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
