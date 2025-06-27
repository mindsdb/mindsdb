import pandas as pd
from typing import List

from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb_sql_parser import ast


class FrappeDocumentsTable(APITable):

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Selects data from the Frappe API and returns it as a pandas DataFrame.

        Returns dataframe representing the Frappe API results.

        Args:
            query (ast.Select): Given SQL SELECT query
        """
        conditions = extract_comparison_conditions(query.where)

        params = {}
        filters = []
        for op, arg1, arg2 in conditions:
            if arg1 == 'doctype':
                if op != '=':
                    raise NotImplementedError
                params['doctype'] = arg2
            elif arg1 == 'name':
                params['name'] = arg2
            else:
                filters.append([arg1, op, arg2])

        if 'doctype' not in params:
            raise ValueError('"doctype" parameter required')

        if query.limit:
            params['limit'] = query.limit.value
        if filters:
            params['filters'] = filters

        if 'name' in params:
            document_data = self.handler.call_frappe_api(
                method_name='get_document',
                params=params
            )
        else:
            document_data = self.handler.call_frappe_api(
                method_name='get_documents',
                params=params
            )

        # Only return the columns we need to.
        columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                columns = document_data.columns
                break
            elif isinstance(target, ast.Identifier):
                columns.append(target.parts[-1])
            else:
                raise NotImplementedError

        if len(document_data) == 0:
            return pd.DataFrame([], columns=columns)

        # Remove columns not part of select.
        for col in set(document_data.columns).difference(set(columns)):
            document_data = document_data.drop(col, axis=1)

        return document_data

    def insert(self, query: ast.Insert) -> pd.DataFrame:
        columns = [col.name for col in query.columns]

        for row in query.values:
            params = dict(zip(columns, row))

            self.handler.call_frappe_api('create_document', params)

    def get_columns(self) -> List:
        """Gets all columns to be returned in pandas DataFrame responses"""
        return ['doctype', 'data']
