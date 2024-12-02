from typing import List
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.integrations.libs.api_handler import APIHandler, APITable
from mindsdb_sql_parser import ast
import pandas as pd
from mindsdb_sql_parser.ast.select.constant import Constant
import json


class Articles(APITable):
    name: str = "articles"

    def __init__(self, handler: APIHandler):
        super().__init__(handler)

    def select(self, query: ast.Select) -> pd.DataFrame:
        """triggered at the SELECT query

        Args:
            query (ast.Select): user's entered query

        Returns:
            pd.DataFrame: the queried information
        """
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
            df = self.handler.call_intercom_api(endpoint=f'/articles/{_id}')

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
                if limit == 0:
                    break
                if limit:
                    # Calculate the page size for this request
                    current_page_size = min(page_size, limit)
                else:
                    current_page_size = page_size

                df = pd.DataFrame(self.handler.call_intercom_api(endpoint='/articles', params={'page': page, 'per_page': current_page_size})['data'][0])
                if len(df) == 0:
                    break
                result_df = pd.concat([result_df, df[selected_columns]], ignore_index=True)
                if limit:
                    limit -= current_page_size
        return result_df

    def insert(self, query: ast.Insert) -> None:
        """insert

        Args:
            query (ast.Insert): user's entered query

        Returns:
            None
        """
        data = {}
        for column, value in zip(query.columns, query.values[0]):
            if isinstance(value, Constant):
                data[column.name] = value.value
            else:
                data[column.name] = value
        self.handler.call_intercom_api(endpoint='/articles', method='POST', data=json.dumps(data))

    def update(self, query: ast.Update) -> None:
        """update

        Args:
            query (ast.Update): user's entered query

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
                data[key] = value.value
            else:
                data[key] = value
        self.handler.call_intercom_api(endpoint=f'/articles/{_id}', method='PUT', data=json.dumps(data))

    def get_columns(self, ignore: List[str] = []) -> List[str]:
        """columns

        Args:
            ignore (List[str], optional): exclusion items. Defaults to [].

        Returns:
            List[str]: available columns with `ignore` items removed from the list.
        """
        return [
            "type",
            "id",
            "workspace_id",
            "title",
            "description",
            "body",
            "author_id",
            "state",
            "created_at",
            "updated_at",
            "url",
            "parent_id",
            "parent_ids",
            "parent_type",
            "statistics"
        ]
