import pandas as pd
from typing import List
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.handlers.query_utilities import SELECTQueryParser, SELECTQueryExecutor
from mindsdb.utilities import log
from mindsdb_sql.parser import ast

logger = log.getLogger(__name__)


class AQByUserLocationTable(APITable):
    """The Air Quality By User Location Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the https://aqicn.org/json-api/doc/#api-Geolocalized_Feed-GetHereFeed" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Air Quality Data

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'air_quality_user_location',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        subset_where_conditions = []
        for op, arg1, arg2 in where_conditions:
            if arg1 in self.get_columns():
                subset_where_conditions.append([op, arg1, arg2])

        df = pd.DataFrame(columns=self.get_columns())

        response = self.handler.aqicn_client.air_quality_user_location()

        self.check_res(res=response)

        df = pd.json_normalize(response["content"])

        select_statement_executor = SELECTQueryExecutor(
            df,
            selected_columns,
            subset_where_conditions,
            order_by_conditions,
            result_limit
        )

        df = select_statement_executor.execute_query()

        return df

    def check_res(self, res):
        if res["code"] != 200:
            raise Exception("Error fetching results - " + res["content"])

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses

        Returns
        -------
        List[str]
            List of columns
        """

        return [
            "status",
            "data.aqi",
            "data.idx",
            "data.attributions",
            "data.city.geo",
            "data.city.name",
            "data.city.url",
            "data.city.location",
            "data.dominentpol",
            "data.iaqi.co.v",
            "data.iaqi.dew.v",
            "data.iaqi.h.v",
            "data.iaqi.no2.v",
            "data.iaqi.p.v",
            "data.iaqi.pm10.v",
            "data.iaqi.so2.v",
            "data.iaqi.t.v",
            "data.iaqi.w.v",
            "data.time.s",
            "data.time.tz",
            "data.time.v",
            "data.time.iso",
            "data.forecast.daily.o3",
            "data.forecast.daily.pm10",
            "data.forecast.daily.pm25",
            "data.debug.sync"
        ]


class AQByCityTable(APITable):
    """The Air Quality By City Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the https://aqicn.org/json-api/doc/#api-City_Feed-GetCityFeed" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Air Quality Data

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'air_quality_city',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        search_params = {}
        subset_where_conditions = []
        for op, arg1, arg2 in where_conditions:
            if arg1 == 'city':
                if op == '=':
                    search_params["city"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for city column.")
            elif arg1 in self.get_columns():
                subset_where_conditions.append([op, arg1, arg2])

        filter_flag = ("city" in search_params)

        if not filter_flag:
            raise NotImplementedError("city column has to be present in where clause.")

        df = pd.DataFrame(columns=self.get_columns())

        response = self.handler.aqicn_client.air_quality_city(search_params["city"])

        self.check_res(res=response)

        df = pd.json_normalize(response["content"])

        select_statement_executor = SELECTQueryExecutor(
            df,
            selected_columns,
            subset_where_conditions,
            order_by_conditions,
            result_limit
        )

        df = select_statement_executor.execute_query()

        return df

    def check_res(self, res):
        if res["code"] != 200:
            raise Exception("Error fetching results - " + res["content"])

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses

        Returns
        -------
        List[str]
            List of columns
        """

        return [
            "status",
            "data.aqi",
            "data.idx",
            "data.attributions",
            "data.city.geo",
            "data.city.name",
            "data.city.url",
            "data.city.location",
            "data.dominentpol",
            "data.iaqi.co.v",
            "data.iaqi.dew.v",
            "data.iaqi.h.v",
            "data.iaqi.no2.v",
            "data.iaqi.o3.v",
            "data.iaqi.p.v",
            "data.iaqi.pm10.v",
            "data.iaqi.pm25.v",
            "data.iaqi.so2.v",
            "data.iaqi.t.v",
            "data.iaqi.w.v",
            "data.time.s",
            "data.time.tz",
            "data.time.v",
            "data.time.iso",
            "data.forecast.daily.o3",
            "data.forecast.daily.pm10",
            "data.forecast.daily.pm25",
            "data.debug.sync"
        ]


class AQByLatLngTable(APITable):
    """The Air Quality By Lat Lng Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the https://aqicn.org/json-api/doc/#api-Geolocalized_Feed-GetGeolocFeed" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Air Quality Data

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'air_quality_lat_lng',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        search_params = {}
        subset_where_conditions = []
        for op, arg1, arg2 in where_conditions:
            if arg1 == 'lat':
                if op == '=':
                    search_params["lat"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for lat column.")
            if arg1 == 'lng':
                if op == '=':
                    search_params["lng"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for lng column.")
            elif arg1 in self.get_columns():
                subset_where_conditions.append([op, arg1, arg2])

        filter_flag = ("lat" in search_params) and ("lng" in search_params)

        if not filter_flag:
            raise NotImplementedError("lat and lng columns have to be present in where clause.")

        df = pd.DataFrame(columns=self.get_columns())

        response = self.handler.aqicn_client.air_quality_lat_lng(search_params["lat"], search_params["lng"])

        self.check_res(res=response)

        df = pd.json_normalize(response["content"])

        select_statement_executor = SELECTQueryExecutor(
            df,
            selected_columns,
            subset_where_conditions,
            order_by_conditions,
            result_limit
        )

        df = select_statement_executor.execute_query()

        return df

    def check_res(self, res):
        if res["code"] != 200:
            raise Exception("Error fetching results - " + res["content"])

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses

        Returns
        -------
        List[str]
            List of columns
        """

        return [
            "status",
            "data.aqi",
            "data.idx",
            "data.attributions",
            "data.city.geo",
            "data.city.name",
            "data.city.url",
            "data.city.location",
            "data.dominentpol",
            "data.iaqi.co.v",
            "data.iaqi.dew.v",
            "data.iaqi.h.v",
            "data.iaqi.no2.v",
            "data.iaqi.o3.v",
            "data.iaqi.p.v",
            "data.iaqi.pm10.v",
            "data.iaqi.pm25.v",
            "data.iaqi.so2.v",
            "data.iaqi.t.v",
            "data.iaqi.w.v",
            "data.time.s",
            "data.time.tz",
            "data.time.v",
            "data.time.iso",
            "data.forecast.daily.o3",
            "data.forecast.daily.pm10",
            "data.forecast.daily.pm25",
            "data.debug.sync"
        ]


class AQByNetworkStationTable(APITable):
    """The Air Quality By Network Station Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the https://aqicn.org/json-api/doc/#api-Search-SearchByName" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Air Quality Data

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'air_quality_station_by_name',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        search_params = {}
        subset_where_conditions = []
        for op, arg1, arg2 in where_conditions:
            if arg1 == 'name':
                if op == '=':
                    search_params["name"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for name column.")
            elif arg1 in self.get_columns():
                subset_where_conditions.append([op, arg1, arg2])

        filter_flag = ("name" in search_params)

        if not filter_flag:
            raise NotImplementedError("name column have to be present in where clause.")

        df = pd.DataFrame(columns=self.get_columns())

        response = self.handler.aqicn_client.air_quality_station_by_name(search_params["name"])

        self.check_res(res=response)

        df = pd.json_normalize(response["content"]["data"])

        select_statement_executor = SELECTQueryExecutor(
            df,
            selected_columns,
            subset_where_conditions,
            order_by_conditions,
            result_limit
        )

        df = select_statement_executor.execute_query()

        return df

    def check_res(self, res):
        if res["code"] != 200:
            raise Exception("Error fetching results - " + res["content"])

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses

        Returns
        -------
        List[str]
            List of columns
        """

        return [
            'uid',
            'aqi',
            'time.tz',
            'time.stime',
            'time.vtime',
            'station.name',
            'station.geo',
            'station.url',
            'station.country'
        ]
