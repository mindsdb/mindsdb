from typing import List

import pandas as pd
from mindsdb_sql.parser import ast

from mindsdb.integrations.handlers.utilities.query_utilities import (
    SELECTQueryExecutor,
    SELECTQueryParser,
)
from mindsdb.integrations.libs.api_handler import APIHandler, APITable
from mindsdb.integrations.utilities.sql_utils import conditions_to_filter


class CustomAPITable(APITable):

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.handler.connect()

    def get_columns(self, ignore: List[str] = []) -> List[str]:
        return [item for item in self.columns if item not in ignore]

    def select(self, query: ast.Select) -> pd.DataFrame:
        raise NotImplementedError()

    def parse_select(self, query: ast.Select, table_name: str):
        select_statement_parser = SELECTQueryParser(query, table_name, self.get_columns())
        self.selected_columns, self.where_conditions, self.order_by_conditions, self.result_limit = select_statement_parser.parse_query()

    def get_where_param(self, query: ast.Select, param: str):
        params = conditions_to_filter(query.where)
        if param not in params:
            raise Exception(f"WHERE condition does not have '{param}' selector")
        return params[param]

    def apply_query_params(self, df, query):
        select_statement_parser = SELECTQueryParser(query, self.name, self.get_columns())
        selected_columns, _, order_by_conditions, result_limit = select_statement_parser.parse_query()
        select_statement_executor = SELECTQueryExecutor(df, selected_columns, [], order_by_conditions, result_limit)
        return select_statement_executor.execute_query()


class AddressEmailAddressTable(CustomAPITable):
    """Retrieves all the email address data linked to all business partner address records in the system"""

    name: str = "address_email_address"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "AddressID",
        "Person",
        "OrdinalNumber",
        "IsDefaultEmailAddress",
        "EmailAddress",
        "SearchEmailAddress",
        "AddressCommunicationRemarkText",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_AddressEmailAddress")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)
