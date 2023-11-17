import pandas as pd

from typing import List

from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.utilities import log

from mindsdb_sql.parser import ast

logger = log.getLogger(__name__)

class JiraProjectsTable(APITable):
    """Jira Projects Table implementation"""
    _MAX_API_RESULTS = 100
    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Jira "get_all_project_issues" API endpoint
        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query
        Returns
        -------
        pd.DataFrame
            jira "get_all_project_issues" matching the query
        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        conditions = extract_comparison_conditions(query.where)

        if query.limit:
            total_results = query.limit.value

        issues_kwargs = {}
        order_by_conditions = {}

        if query.order_by and len(query.order_by) > 0:
            order_by_conditions["columns"] = []
            order_by_conditions["ascending"] = []

            for an_order in query.order_by:
                if an_order.field.parts[0] != "key":
                    continue    
                if an_order.field.parts[1] in ["reporter","assignee","status"]:
                    if issues_kwargs != {}:
                        raise ValueError(
                            "Duplicate order conditions found for reporter,status and assignee"
                        )
                    issues_kwargs["sort"] = an_order.field.parts[1]
                    issues_kwargs["direction"] = an_order.direction
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
        project = self.handler.connection_data['project']
        jira_project_df = self.call_jira_api(project)
        
        selected_columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                selected_columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                selected_columns.append(target.parts[-1])
            else:
                raise ValueError(f"Unknown query target {type(target)}")


        if len(jira_project_df) == 0:
            jira_project_df = pd.DataFrame([], columns=selected_columns)
            return jira_project_df

        jira_project_df.columns = self.get_columns()
        for col in set(jira_project_df.columns).difference(set(selected_columns)):
            jira_project_df = jira_project_df.drop(col, axis=1)

        if len(order_by_conditions.get("columns", [])) > 0:
            jira_project_df = jira_project_df.sort_values(
                by=order_by_conditions["columns"],
                ascending=order_by_conditions["ascending"],
            )
        
        if query.limit:
            jira_project_df = jira_project_df.head(total_results)

        return jira_project_df

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses
        Returns
        -------
        List[str]
            List of columns
        """
        return [
        'key',
        'summary',
        'status',
        'reporter',
        'assignee',
        'priority',
        ]

    def call_jira_api(self, project):

        jira = self.handler.connect()
        max_records = jira.get_project_issues_count(project)
        max_records = 100
        jql_query = self.handler.construct_jql()
        max_results = self._MAX_API_RESULTS 
        start_index = 0
        total = 1
        fields = [
        'key',
        'fields.summary',
        'fields.status.name',
        'fields.reporter.name',
        'fields.assignee.name',
        'fields.priority.name',
        ]

        all_jira_issues_df = pd.DataFrame(columns=fields)

        while start_index <= total:
            results = self.handler.connect().jql(jql_query,start=start_index, limit=max_results)
            df = pd.json_normalize(results['issues'])
            df = df[fields]
            start_index += max_results
            total = max_records
            all_jira_issues_df = pd.concat([all_jira_issues_df, df], axis=0)


        all_jira_issues_df = all_jira_issues_df.rename(columns={
                                                                'key': 'key', 
                                                                'fields.summary': 'summary',
                                                                'fields.reporter.name':'reporter',
                                                                'fields.assignee.name':'assignee',
                                                                'fields.priority.name':'priority',
                                                                'fields.status.name':'status'})
        
        return all_jira_issues_df

