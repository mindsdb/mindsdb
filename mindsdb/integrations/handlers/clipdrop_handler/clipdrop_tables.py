import pandas as pd
from typing import List
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.handlers.utilities.query_utilities import SELECTQueryParser, SELECTQueryExecutor
from mindsdb.utilities.log import get_log
from mindsdb_sql.parser import ast

logger = get_log("integrations.clipdrop_handler")


class RemoveTextTable(APITable):
    """The Remove Text Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the https://clipdrop-api.co/remove-text/v1" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'remove_text',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        search_params = {}
        subset_where_conditions = []
        for op, arg1, arg2 in where_conditions:
            if arg1 == 'img_url':
                if op == '=':
                    search_params["img_url"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for img_url column.")
            elif arg1 in self.get_columns():
                subset_where_conditions.append([op, arg1, arg2])

        filter_flag = ("img_url" in search_params)

        if not filter_flag:
            raise NotImplementedError("img_url column has to be present in WHERE clause.")

        df = pd.DataFrame(columns=self.get_columns())

        response = self.handler.client.remove_text(search_params["img_url"])
        
        res = self.check_res(response)

        df = pd.json_normalize(res)

        select_statement_executor = SELECTQueryExecutor(
            df,
            selected_columns,
            subset_where_conditions,
            order_by_conditions,
            result_limit
        )

        df = select_statement_executor.execute_query()

        return df
    
    def check_res(self, response):
        if response["code"] == 200:
            return {"saved_path": response["content"], "error_message": "", "response_code": 200}
        return {"saved_path": "", "error_message": response["content"], "response_code": response["code"]}

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses

        Returns
        -------
        List[str]
            List of columns
        """

        return [
            "error_message",
            "saved_path",
            "response_code"
        ]
