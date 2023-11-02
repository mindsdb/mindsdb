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


class RemoveBackgroundTable(APITable):
    """The Remove background Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the https://clipdrop-api.co/remove-background/v1" API

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
            'remove_background',
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

        response = self.handler.client.remove_background(search_params["img_url"])

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


class SketchToImageTable(APITable):
    """The Sketch to Image implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the https://clipdrop-api.co/sketch-to-image/v1/sketch-to-image" API

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
            'sketch_to_image',
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
            if arg1 == 'text':
                if op == '=':
                    search_params["text"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for text column.")
            elif arg1 in self.get_columns():
                subset_where_conditions.append([op, arg1, arg2])

        filter_flag = ("img_url" in search_params) and ("text" in search_params)

        if not filter_flag:
            raise NotImplementedError("img_url and text columns have to be present in WHERE clause.")

        df = pd.DataFrame(columns=self.get_columns())

        response = self.handler.client.sketch_to_image(search_params["img_url"], search_params["text"])

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


class TextToImageTable(APITable):
    """The Text to Image implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the https://clipdrop-api.co/text-to-image/v1" API

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
            'text_to_image',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        search_params = {}
        subset_where_conditions = []
        for op, arg1, arg2 in where_conditions:
            if arg1 == 'text':
                if op == '=':
                    search_params["text"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for text column.")
            elif arg1 in self.get_columns():
                subset_where_conditions.append([op, arg1, arg2])

        filter_flag = ("text" in search_params)

        if not filter_flag:
            raise NotImplementedError("text column has to be present in WHERE clause.")

        df = pd.DataFrame(columns=self.get_columns())

        response = self.handler.client.text_to_image(search_params["text"])

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


class ReplaceBackgroundTable(APITable):
    """The Replace background implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the https://clipdrop-api.co/replace-background/v1" API

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
            'replace_background',
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
            if arg1 == 'text':
                if op == '=':
                    search_params["text"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for text column.")
            elif arg1 in self.get_columns():
                subset_where_conditions.append([op, arg1, arg2])

        filter_flag = ("img_url" in search_params) and ("text" in search_params)

        if not filter_flag:
            raise NotImplementedError("text column has to be present in WHERE clause.")

        df = pd.DataFrame(columns=self.get_columns())

        response = self.handler.client.replace_background(search_params["img_url"], search_params["text"])

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


class ReimagineTable(APITable):
    """The Reimagine Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the https://clipdrop-api.co/reimagine/v1/reimagine" API

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
            'reimagine',
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

        response = self.handler.client.reimagine(search_params["img_url"])

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
