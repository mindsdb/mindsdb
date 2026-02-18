from typing import List
import pandas as pd
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb_sql_parser import ast
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb_sql_parser.ast.select.constant import Constant
import json
import re

langCodes = ["ar", "cs", "da", "de", "en", "es", "et", "fi", "fr", "hu", "id", "it", "ja", "ko",
             "nl", "no", "pl", "pt", "pt-BR", "ro", "rs", "ru", "sl", "sq", "sv", "tr", "uk",
             "vi", "zh", "zh-TW"]


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
            for langCode in langCodes:
                try:
                    df[f"translations_name_in_{langCode}"] = df["translations"].apply(lambda x: x.get("name", None)).apply(lambda x: x.get(langCode, None))
                    df[f"translations_desc_in_{langCode}"] = df["translations"].apply(lambda x: x.get("description", None)).apply(lambda x: x.get(langCode, None))
                except AttributeError:
                    df[f"translations_name_in_{langCode}"] = None
                    df[f"translations_desc_in_{langCode}"] = None
            df = df.drop(columns=["translations"])

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
                ''' Add translations_name_in_{langCode} and translations_desc_in_{langCode} columns to the dataframe'''
                for i in range(len(df)):
                    for langCode in langCodes:
                        try:
                            df.at[i, f"translations_name_in_{langCode}"] = df.at[i, "translations"].get("name", {}).get(langCode, None)
                            df.at[i, f"translations_desc_in_{langCode}"] = df.at[i, "translations"].get("description", {}).get(langCode, None)
                        except AttributeError:
                            df.at[i, f"translations_name_in_{langCode}"] = None
                            df.at[i, f"translations_desc_in_{langCode}"] = None

                # Drop the 'translations' column
                df = df.drop(columns=["translations"])
                # Concatenate the dataframes
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
        data = {'translations': {
            "name": {},
            "description": {}
        }}

        for column, value in zip(query.columns, query.values[0]):
            if isinstance(value, Constant):
                data[column.name] = json.loads(value.value) if column.name == 'translations' else value.value
            elif isinstance(value, str):
                try:
                    if re.match(r'^translations_name_in_[a-zA-Z\-]+$', column.name):
                        lang_code = column.name.split('_')[-1]
                        if lang_code not in langCodes:
                            raise Exception(f'Invalid language code {lang_code}')
                        data['translations']['name'][lang_code] = value
                    elif re.match(r'^translations_desc_in_[a-zA-Z\-]+$', column.name):
                        lang_code = column.name.split('_')[-1]
                        if lang_code not in langCodes:
                            raise Exception(f'Invalid language code {lang_code}')
                        data['translations']['description'][lang_code] = value
                    else:
                        data[column.name] = json.loads(value)
                except json.JSONDecodeError:
                    data[column.name] = True if value == 'True' else (False if value == 'False' else value)

        page_id = data.pop('page_id', None)

        if page_id is not None:
            self.handler.call_instatus_api(endpoint=f'/v1/{page_id}/components', method='POST', json_data=data)

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

        data = {'translations': {
            "name": {},
            "description": {}
        }}
        for key, value in query.update_columns.items():
            if isinstance(value, Constant):
                if re.match(r'^translations_name_in_[a-zA-Z\-]+$', key):
                    lang_code = key.split('_')[-1]
                    if lang_code not in langCodes:
                        raise Exception(f'Invalid language code {lang_code}')
                    data['translations']['name'][lang_code] = value.value
                elif re.match(r'^translations_desc_in_[a-zA-Z\-]+$', key):
                    lang_code = key.split('_')[-1]
                    if lang_code not in langCodes:
                        raise Exception(f'Invalid language code {lang_code}')
                    data['translations']['description'][lang_code] = value.value
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
        ] + [f'translations_name_in_{langCode}' for langCode in langCodes] + [f'translations_desc_in_{langCode}' for langCode in langCodes]
