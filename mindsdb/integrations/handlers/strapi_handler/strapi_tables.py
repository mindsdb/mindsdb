from typing import List
import pandas as pd
from mindsdb.integrations.libs.api_handler import APIHandler, APITable
from mindsdb_sql_parser import ast
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb_sql_parser.ast.select.constant import Constant
import json


class StrapiTable(APITable):

    def __init__(self, handler: APIHandler, name: str):
        super().__init__(handler)
        self.name = name
        # get all the fields of a collection as columns
        self.columns = self.handler.call_strapi_api(method='GET', endpoint=f'/api/{name}').columns

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Triggered at the SELECT query

        Args:
            query (ast.Select): User's entered query

        Returns:
            pd.DataFrame: The queried information
        """
        # Initialize _id and selected_columns
        _id = None
        selected_columns = []

        # Get id from where clause, if available
        conditions = extract_comparison_conditions(query.where)
        for op, arg1, arg2 in conditions:
            if arg1 == 'id' and op == '=':
                _id = arg2
            else:
                raise ValueError("Unsupported condition in WHERE clause")

        # Get selected columns from query
        for target in query.targets:
            if isinstance(target, ast.Star):
                selected_columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                selected_columns.append(target.parts[-1])
            else:
                raise ValueError(f"Unknown query target {type(target)}")

        # Initialize the result DataFrame
        result_df = None

        if _id is not None:
            # Fetch data using the provided endpoint for the specific id
            df = self.handler.call_strapi_api(method='GET', endpoint=f'/api/{self.name}/{_id}')

            if len(df) > 0:
                result_df = df[selected_columns]
        else:
            # Fetch data without specifying an id
            page_size = 100  # The page size you want to use for API requests
            limit = query.limit.value if query.limit else None
            result_df = pd.DataFrame(columns=selected_columns)

            if limit:
                # Calculate the number of pages required
                page_count = (limit + page_size - 1) // page_size
            else:
                page_count = 1

            for page in range(1, page_count + 1):
                if limit:
                    # Calculate the page size for this request
                    current_page_size = min(page_size, limit)
                else:
                    current_page_size = page_size

                df = self.handler.call_strapi_api(method='GET', endpoint=f'/api/{self.name}', params={'pagination[page]': page, 'pagination[pageSize]': current_page_size})

                if len(df) == 0:
                    break

                result_df = pd.concat([result_df, df[selected_columns]], ignore_index=True)

                if limit:
                    limit -= current_page_size

        return result_df

    def insert(self, query: ast.Insert) -> None:
        """triggered at the INSERT query
        Args:
            query (ast.Insert): user's entered query
        """
        data = {'data': {}}
        for column, value in zip(query.columns, query.values[0]):
            if isinstance(value, Constant):
                data['data'][column.name] = value.value
            else:
                data['data'][column.name] = value
        self.handler.call_strapi_api(method='POST', endpoint=f'/api/{self.name}', json_data=json.dumps(data))

    def update(self, query: ast.Update) -> None:
        """triggered at the UPDATE query

        Args:
            query (ast.Update): user's entered query
        """
        conditions = extract_comparison_conditions(query.where)
        # Get id from query
        for op, arg1, arg2 in conditions:
            if arg1 == 'id' and op == '=':
                _id = arg2
            else:
                raise NotImplementedError
        data = {'data': {}}
        for key, value in query.update_columns.items():
            if isinstance(value, Constant):
                data['data'][key] = value.value
        self.handler.call_strapi_api(method='PUT', endpoint=f'/api/{self.name}/{_id}', json_data=json.dumps(data))

    def get_columns(self, ignore: List[str] = []) -> List[str]:
        """columns

        Args:
            ignore (List[str], optional): exclusion items. Defaults to [].

        Returns:
            List[str]: available columns with `ignore` items removed from the list.
        """

        return [item for item in self.columns if item not in ignore]
