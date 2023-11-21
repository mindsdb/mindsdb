from typing import List
import pandas as pd
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb_sql.parser import ast
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb_sql.parser.ast.select.constant import Constant
import json


class StatusPages(APITable):

    # table name in the database
    name = 'status_pages'

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Receive query as AST (abstract syntax tree) and act upon it.

        Args:
            query (ASTNode): sql query represented as AST. Usually it should be ast.Select

        Returns:
            pd.DataFrame
        """
        conditions = extract_comparison_conditions(query.where)
        # Get page id from query
        _id = None
        for op, arg1, arg2 in conditions:
            if arg1 == 'id' and op == '=':
                _id = arg2
            else:
                raise NotImplementedError

        # Get column names from query
        selected_columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                selected_columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                selected_columns.append(target.parts[-1])
            else:
                raise ValueError(f"Unknown query target {type(target)}")

        # 'id' needs to selected when searching with 'id'
        temp_selected_columns = selected_columns
        if _id and 'id' not in selected_columns:
            selected_columns = ['id'] + selected_columns

        # Get limit from query
        limit = query.limit.value if query.limit else 20
        total_results = limit

        page_no = 1  # default page no
        result_df = pd.DataFrame(columns=selected_columns)

        # call instatus api and get the response as pd.DataFrame
        while True:
            df = self.handler.call_instatus_api(endpoint='/v2/pages', params={'page': page_no, 'per_page': 100})
            if len(df) == 0 or limit <= 0:
                break
            else:
                result_df = pd.concat([result_df, df[selected_columns]], ignore_index=True)

            page_no += 1
            limit -= len(df)

        # select columns from pandas data frame df
        if result_df.empty:
            result_df = pd.DataFrame(columns=selected_columns)
        elif _id:
            result_df = result_df[result_df['id'] == _id]

        # delete 'id' column if 'id' not present in temp_selected_columns
        if 'id' not in temp_selected_columns and 'id' in selected_columns:
            result_df = result_df.drop('id', axis=1)

        return result_df.head(n=total_results)

    def insert(self, query: ast.Insert) -> None:
        """Receive query as AST (abstract syntax tree) and act upon it somehow.

        Args:
            query (ASTNode): sql query represented as AST. Usually it should be ast.Insert

        Returns:
            None
        """
        data = {}
        for column, value in zip(query.columns, query.values[0]):
            if isinstance(value, Constant):
                data[column.name] = value.value
            else:
                data[column.name] = value
        self.handler.call_instatus_api(endpoint='/v1/pages', method='POST', data=json.dumps(data))

    def update(self, query: ast.Update) -> None:
        """Receive query as AST (abstract syntax tree) and act upon it somehow.

        Args:
            query (ASTNode): sql query represented as AST. Usually it should be ast.Update
        Returns:
            None
        """
        conditions = extract_comparison_conditions(query.where)
        # Get page id from query
        _id = None
        for op, arg1, arg2 in conditions:
            if arg1 == 'id' and op == '=':
                _id = arg2
            else:
                raise NotImplementedError

        data = {}
        for key, value in query.update_columns.items():
            if isinstance(value, Constant):
                if key == 'components':
                    data[key] = json.loads(value.value)  # Convert 'components' value to a Python list
                else:
                    data[key] = value.value
        self.handler.call_instatus_api(endpoint=f'/v2/{_id}', method='PUT', data=json.dumps(data))

    def get_columns(self, ignore: List[str] = []) -> List[str]:
        """columns

        Args:
            ignore (List[str], optional): exclusion items. Defaults to [].

        Returns:
            List[str]: available columns with `ignore` items removed from the list.
        """
        return [
            "id",
            "subdomain",
            "name",
            "workspaceId",
            "logoUrl",
            "faviconUrl",
            "websiteUrl",
            "customDomain",
            "publicEmail",
            "twitter",
            "status",
            "subscribeBySms",
            "sendSmsNotifications",
            "language",
            "useLargeHeader",
            "brandColor",
            "okColor",
            "disruptedColor",
            "degradedColor",
            "downColor",
            "noticeColor",
            "unknownColor",
            "googleAnalytics",
            "smsService",
            "htmlInMeta",
            "htmlAboveHeader",
            "htmlBelowHeader",
            "htmlAboveFooter",
            "htmlBelowFooter",
            "htmlBelowSummary",
            "uptimeDaysDisplay",
            "uptimeOutageDisplay",
            "launchDate",
            "cssGlobal",
            "onboarded",
            "createdAt",
            "updatedAt"
        ]
