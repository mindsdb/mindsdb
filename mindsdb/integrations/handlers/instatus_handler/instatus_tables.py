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
            if isinstance(value, str):
                try:
                    value = json.loads(value)
                except json.JSONDecodeError:
                    if value == 'True':
                        value = True
                    elif value == 'False':
                        value = False
            data[column.name] = value
        self.handler.call_instatus_api(endpoint='/v1/pages', method='POST', json_data=data)

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
                    data[key] = json.loads(value.value)
                else:
                    data[key] = value.value

        if 'components' in data and isinstance(data['components'], str):
            data['components'] = json.loads(data['components'])

        self.handler.call_instatus_api(endpoint=f'/v2/{_id}', method='PUT', json_data=data)

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


class Components(APITable):

    # table name in the database
    name = 'components'

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Receive query as AST (abstract syntax tree) and act upon it.

        Args:
            query (ASTNode): SQL query represented as AST. Usually it should be ast.Select

        Returns:
            pd.DataFrame
        """
        conditions = extract_comparison_conditions(query.where)

        if len(conditions) == 0:
            raise Exception('WHERE clause is required')

        # Get page id and component id from query
        pageId = None
        componentId = None
        for condition in conditions:
            if condition[1] == 'page_id' and condition[0] == '=':
                pageId = condition[2]

            if condition[1] == 'component_id' and condition[0] == '=':
                componentId = condition[2]

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

        limit = query.limit.value if query.limit else None
        if componentId:
            # Call instatus API and get the response as pd.DataFrame
            df = self.handler.call_instatus_api(endpoint=f'/v1/{pageId}/components/{componentId}')
            result_df = df[selected_columns]
        else:
            # Call instatus API and get the response as pd.DataFrame
            page_size = 100
            # Calculate the number of pages required
            page_count = (limit + page_size - 1) // page_size if limit else 1
            result_df = pd.DataFrame(columns=selected_columns)

            # Call instatus API and get the response as pd.DataFrame for each page
            for page in range(1, page_count + 1):
                current_page_size = min(page_size, limit) if limit else page_size

                df = self.handler.call_instatus_api(endpoint=f'/v1/{pageId}/components', params={'page': page, 'per_page': current_page_size})
                # Break if no more data is available or limit is reached
                if len(df) == 0 or (limit and limit <= 0) or limit == 0:
                    break
                result_df = pd.concat([result_df, df[selected_columns]], ignore_index=True)

                if limit:
                    limit -= len(df)

        return result_df

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
                if column.name == 'translations':
                    data[column.name] = json.loads(value.value)
                else:
                    data[column.name] = value.value
        pageId = data['page_id']
        if 'page_id' in data:
            del data['page_id']
        self.handler.call_instatus_api(endpoint=f'/v1/{pageId}/components', method='POST', json_data=data)

    def update(self, query: ast.Update) -> None:
        """Receive query as AST (abstract syntax tree) and act upon it somehow.

        Args:
            query (ASTNode): sql query represented as AST. Usually it should be ast.Update
        Returns:
            None
        """
        conditions = extract_comparison_conditions(query.where)
        # Get page id and component id from query
        pageId = None
        componentId = None
        for condition in conditions:
            if condition[1] == 'page_id' and condition[0] == '=':
                pageId = condition[2]
            elif condition[1] == 'component_id' and condition[0] == '=':
                componentId = condition[2]
            else:
                raise Exception("page_id and component_id both are required")

        data = {}
        for key, value in query.update_columns.items():
            if isinstance(value, Constant):
                if key == 'translations':
                    data[key] = json.loads(value.value)
                else:
                    data[key] = value.value
        self.handler.call_instatus_api(endpoint=f'/v1/{pageId}/components/{componentId}', method='PUT', json_data=data)

    def get_columns(self, ignore: List[str] = []) -> List[str]:
        """columns

        Args:
            ignore (List[str], optional): exclusion items. Defaults to [].

        Returns:
            List[str]: available columns with `ignore` items removed from the list.
        """
        return [
            "id",
            "name",
            "nameTranslationId",
            "description",
            "descriptionTranslationId",
            "status",
            "order",
            "showUptime",
            "createdAt",
            "updatedAt",
            "archivedAt",
            "siteId",
            "uniqueEmail",
            "oldGroup",
            "groupId",
            "isParent",
            "isCollapsed",
            "monitorId",
            "nameHtml",
            "nameHtmlTranslationId",
            "descriptionHtml",
            "descriptionHtmlTranslationId",
            "isThirdParty",
            "thirdPartyStatus",
            "thirdPartyComponentId",
            "thirdPartyComponentServiceId",
            "importedFromStatuspage",
            "startDate",
            "group",
            "translations"
        ]
