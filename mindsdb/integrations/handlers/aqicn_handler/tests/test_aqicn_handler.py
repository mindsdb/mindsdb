import unittest
from mindsdb.integrations.handlers.aqicn_handler.aqicn_handler import AQICNHandler
from mindsdb.integrations.handlers.aqicn_handler.aqicn_tables import *
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


class AQICNHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "api_key": "c91dc67dc3bc839c898f5a8705ebb782c274ac27"
        }
        cls.handler = AQICNHandler('test_aqicn_handler', connection_data= cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_native_query_select(self):
        query = "SELECT * FROM mindsdb_aqicn.air_quality_user_location"
        result = self.handler.native_query(query)
        city_table=AQByCityTable(result)
        print(city_table.get_columns())
        assert result.type is RESPONSE_TYPE.TABLE

    def test_2_native_query_select_by_city(self):
        query = "SELECT * FROM mindsdb_aqicn.air_quality_city where city=\"Bangalore\""
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_3_native_query_select_by_coordinates(self):
        query = "SELECT * FROM mindsdb_aqicn.air_quality_lat_lng where lat=\"12.938539\" AND lng=\"77.5901\""
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE
    
    def test_4_native_query_select_by_station_name(self):
        query = "SELECT * FROM mindsdb_aqicn.air_quality_station_by_name where name=\"bangalore\""
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE


if __name__ == '__main__':
    unittest.main()