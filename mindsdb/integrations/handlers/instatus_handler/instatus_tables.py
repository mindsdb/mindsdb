from typing import List
import pandas as pd
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb_sql.parser import ast
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions


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

        # Get page_no from query
        for op, arg1, arg2 in conditions:

            if arg1 == 'page_no' and op == '=':
                page_no = int(arg2)
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

        # Get limit from query
        if query.limit:
            per_page = query.limit.value
            if per_page > 100:
                raise Exception("The maximum is 100 items per page")
        else:
            per_page = 50

        # call instatus api and get the response as pd.DataFrame
        df = self.handler.call_instatus_api(endpoint='/v2/pages', params={'page': page_no, 'per_page': per_page})

        # select columns from pandas data frame df
        if len(df) == 0:
            df = pd.DataFrame([], columns=selected_columns)

        return df[selected_columns]

    def insert(self, query: ast.Insert) -> None:
        """Receive query as AST (abstract syntax tree) and act upon it somehow.

        Args:
            query (ASTNode): sql query represented as AST. Usually it should be ast.Insert

        Returns:
            None
        """
        # TODO
        raise NotImplementedError()

        return None

    def update(self, query: ast.Update) -> None:
        """Receive query as AST (abstract syntax tree) and act upon it somehow.

        Args:
            query (ASTNode): sql query represented as AST. Usually it should be ast.Update
        Returns:
            None
        """
        # TODO
        raise NotImplementedError()

        return None

    def delete(self, query: ast.Delete) -> None:
        """Receive query as AST (abstract syntax tree) and act upon it somehow.

        Args:
            query (ASTNode): sql query represented as AST. Usually it should be ast.Delete

        Returns:
            None
        """
        # TODO
        raise NotImplementedError()

        return None

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
