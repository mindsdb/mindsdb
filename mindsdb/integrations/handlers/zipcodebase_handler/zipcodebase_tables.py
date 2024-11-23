import pandas as pd
from typing import List
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.handlers.query_utilities import SELECTQueryParser, SELECTQueryExecutor
from mindsdb.utilities import log
from mindsdb_sql_parser import ast

logger = log.getLogger(__name__)


class ZipCodeBaseCodeLocationTable(APITable):
    """The ZipCodeBase Location Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the https://app.zipcodebase.com/documentation#search API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Location of the codes matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'code_to_location',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        search_params = {}
        subset_where_conditions = []

        for op, arg1, arg2 in where_conditions:
            if arg1 == "codes":
                if op == '=':
                    search_params["codes"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for codes column.")

            elif arg1 in self.get_columns():
                subset_where_conditions.append([op, arg1, arg2])

        filter_flag = "codes" in search_params

        if not filter_flag:
            raise NotImplementedError("`codes` column has to be present in where clause.")

        code_to_location_df = pd.DataFrame(columns=self.get_columns())

        response = self.handler.client.code_to_location(search_params.get("codes"))

        self.check_res(res=response)

        content = response["content"]

        code_to_location_df = pd.json_normalize(self.clean_resp(content["results"]))

        select_statement_executor = SELECTQueryExecutor(
            code_to_location_df,
            selected_columns,
            subset_where_conditions,
            order_by_conditions,
            result_limit
        )

        code_to_location_df = select_statement_executor.execute_query()

        return code_to_location_df

    def clean_resp(self, data):
        clean_data = []
        for k, v in data.items():
            clean_data.extend(v)
        return clean_data

    def check_res(self, res):
        if res["code"] != 200:
            raise Exception("Error fetching results - " + res["error"])

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses
        Returns
        -------
        List[str]
            List of columns
        """

        return [
            "postal_code",
            "country_code",
            "latitude",
            "longitude",
            "city",
            "state",
            "city_en",
            "state_en",
            "state_code",
            "province",
            "province_code"
        ]


class ZipCodeBaseCodeInRadiusTable(APITable):
    """The ZipCodeBase Codes within Radius Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the https://app.zipcodebase.com/documentation#radius API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            codes within the radius

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'codes_within_radius',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        search_params = {}
        subset_where_conditions = []

        for op, arg1, arg2 in where_conditions:
            if arg1 == "code":
                if op == '=':
                    search_params["code"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for code column.")

            if arg1 == "radius":
                if op == '=':
                    search_params["radius"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for radius column.")

            if arg1 == "country":
                if op == '=':
                    search_params["country"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for country column.")

            if arg1 == "unit":
                if op == '=':
                    search_params["unit"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for unit column.")

            elif arg1 in self.get_columns():
                subset_where_conditions.append([op, arg1, arg2])

        filter_flag = ("code" in search_params) and ("radius" in search_params) and ("country" in search_params)

        if not filter_flag:
            raise NotImplementedError("`codes`, `radius` and `country` columns have to be present in where clause.")

        code_to_location_df = pd.DataFrame(columns=self.get_columns())

        response = self.handler.client.codes_within_radius(search_params.get("code"), search_params.get("radius"), search_params.get("country"), search_params.get("unit", "km"))

        self.check_res(res=response)

        content = response["content"]

        logger.info(f"response size - {len(content['results'])}")
        code_to_location_df = pd.json_normalize(content["results"])

        select_statement_executor = SELECTQueryExecutor(
            code_to_location_df,
            selected_columns,
            subset_where_conditions,
            order_by_conditions,
            result_limit
        )

        code_to_location_df = select_statement_executor.execute_query()

        return code_to_location_df

    def check_res(self, res):
        if res["code"] != 200:
            raise Exception("Error fetching results - " + res["error"])

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses
        Returns
        -------
        List[str]
            List of columns
        """

        return [
            "code",
            "city",
            "state",
            "city_en",
            "state_en",
            "distance"
        ]


class ZipCodeBaseCodeByCityTable(APITable):
    """The ZipCodeBase Codes within a City Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the https://app.zipcodebase.com/documentation#city API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            codes within the city

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'codes_by_city',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        search_params = {}
        subset_where_conditions = []

        for op, arg1, arg2 in where_conditions:
            if arg1 == "city":
                if op == '=':
                    search_params["city"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for city column.")

            if arg1 == "country":
                if op == '=':
                    search_params["country"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for country column.")

            elif arg1 in self.get_columns():
                subset_where_conditions.append([op, arg1, arg2])

        filter_flag = ("city" in search_params) and ("country" in search_params)

        if not filter_flag:
            raise NotImplementedError("`city` and `country` columns have to be present in where clause.")

        codes_by_city_df = pd.DataFrame(columns=self.get_columns())

        response = self.handler.client.codes_by_city(search_params.get("city"), search_params.get("country"))

        self.check_res(res=response)

        content = response["content"]

        logger.info(f"response size - {len(content['results'])}")
        codes_by_city_df = pd.json_normalize({"codes": content["results"]})

        select_statement_executor = SELECTQueryExecutor(
            codes_by_city_df,
            selected_columns,
            subset_where_conditions,
            order_by_conditions,
            result_limit
        )

        codes_by_city_df = select_statement_executor.execute_query()

        return codes_by_city_df

    def check_res(self, res):
        if res["code"] != 200:
            raise Exception("Error fetching results - " + res["error"])

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses
        Returns
        -------
        List[str]
            List of columns
        """

        return [
            "codes"
        ]


class ZipCodeBaseCodeByStateTable(APITable):
    """The ZipCodeBase Codes within a State Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the https://app.zipcodebase.com/documentation#state API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            codes within the state

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'codes_by_state',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        search_params = {}
        subset_where_conditions = []

        for op, arg1, arg2 in where_conditions:
            if arg1 == "state":
                if op == '=':
                    search_params["state"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for state column.")

            if arg1 == "country":
                if op == '=':
                    search_params["country"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for country column.")

            elif arg1 in self.get_columns():
                subset_where_conditions.append([op, arg1, arg2])

        filter_flag = ("state" in search_params) and ("country" in search_params)

        if not filter_flag:
            raise NotImplementedError("`state` and `country` columns have to be present in where clause.")

        codes_by_state_df = pd.DataFrame(columns=self.get_columns())

        response = self.handler.client.codes_by_state(search_params.get("state"), search_params.get("country"))

        self.check_res(res=response)

        content = response["content"]

        logger.info(f"response size - {len(content['results'])}")
        codes_by_state_df = pd.json_normalize({"codes": content["results"]})

        select_statement_executor = SELECTQueryExecutor(
            codes_by_state_df,
            selected_columns,
            subset_where_conditions,
            order_by_conditions,
            result_limit
        )

        codes_by_state_df = select_statement_executor.execute_query()

        return codes_by_state_df

    def check_res(self, res):
        if res["code"] != 200:
            raise Exception("Error fetching results - " + res["error"])

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses
        Returns
        -------
        List[str]
            List of columns
        """

        return [
            "codes"
        ]


class ZipCodeBaseStatesByCountryTable(APITable):
    """The ZipCodeBase Provinces/states within a country Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the https://app.zipcodebase.com/documentation#provinces API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            states within a country

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'states_by_country',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        search_params = {}
        subset_where_conditions = []

        for op, arg1, arg2 in where_conditions:

            if arg1 == "country":
                if op == '=':
                    search_params["country"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for country column.")

            elif arg1 in self.get_columns():
                subset_where_conditions.append([op, arg1, arg2])

        filter_flag = ("country" in search_params)

        if not filter_flag:
            raise NotImplementedError("`country` column has to be present in where clause.")

        states_by_country_df = pd.DataFrame(columns=self.get_columns())

        response = self.handler.client.states_by_country(search_params.get("country"))

        self.check_res(res=response)

        content = response["content"]

        logger.info(f"response size - {len(content['results'])}")
        states_by_country_df = pd.json_normalize({"states": content["results"]})

        select_statement_executor = SELECTQueryExecutor(
            states_by_country_df,
            selected_columns,
            subset_where_conditions,
            order_by_conditions,
            result_limit
        )

        states_by_country_df = select_statement_executor.execute_query()

        return states_by_country_df

    def check_res(self, res):
        if res["code"] != 200:
            raise Exception("Error fetching results - " + res["error"])

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses
        Returns
        -------
        List[str]
            List of columns
        """

        return [
            "states"
        ]
