import unittest

from mindsdb.integrations.handlers.mendeley_handler.mendeley_handler import MendeleyHandler
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


class MendeleyHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_data": {"client_id": 15253, "client_secret": "BxmSvbrRW5iYEIQR"}
        }
        cls.handler = MendeleyHandler("test_mendeley_handler", **cls.kwargs)

    def test_0_connect(self):
        self.handler.connect()

    def test_1_check_connection(self):
        self.handler.check_connection()

    def test_2_select(self):
        query = "SELECT * FROM catalog_search_data WHERE doi='10.1111/joim.13091'"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_3_select(self):
        query = "SELECT * FROM catalog_search_data WHERE id='af1a0408-7409-3a8b-ad91-8accd4f8849a'"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_4_select(self):
        query = "SELECT * FROM catalog_search_data WHERE title='The American Mineralogist crystal structure database'"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_5_select(self):
        query = "SELECT * FROM catalog_search_data WHERE id='8e86b541-84fd-30ef-9eed-e8c9af847ca3' AND doi='10.1093/ajcn/77.1.71'"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_6_select(self):
        query = "SELECT * FROM catalog_search_data WHERE issn='15570878'"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_7_select(self):
        query = "SELECT * FROM catalog_search_data WHERE source='American Journal of Clinical Nutrition'"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_8_select(self):
        query = "SELECT * FROM catalog_search_data WHERE source='American Journal of Clinical Nutrition'AND id='4eeda257-8db4-3dad-80c8-6912356d3887'"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_9_select(self):
        query = "SELECT * FROM catalog_search_data WHERE source='American Journal of Clinical Nutrition'AND max_year='2020'"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_10_call_mendeley_api_invalid_method_name(self):
        with self.assertRaises(NotImplementedError):
            self.handler.call_mendeley_api("method1", None)

    def test_11_select_invalid_condition_name(self):
        with self.assertRaises(NotImplementedError):
            query = "SELECT * FROM catalog_search_data WHERE name='American Journal of Clinical Nutrition'"
            self.handler.native_query(query)

    def test_12_select_invalid_operator(self):
        with self.assertRaises(NotImplementedError):
            query = "SELECT * FROM catalog_search_data WHERE source>'American Journal of Clinical Nutrition'AND max_year='2020' "
            self.handler.native_query(query)

    def test_13_select_invalid_column_name(self):
        with self.assertRaises(KeyError):
            query = "SELECT name FROM catalog_search_data WHERE source='American Journal of Clinical Nutrition'AND max_year='2020' "
            self.handler.native_query(query)

    def test_14_get_columns(self):
        columns = self.handler.catalog_search_data.get_columns()

        expected_columns = [

            'title',
            'type',
            'source',
            'year',
            'pmid',
            'sgr',
            'issn',
            'scopus',
            'doi',
            'pui',
            'authors',
            'keywords',
            'link',
            'id'
        ]

        self.assertListEqual(columns, expected_columns)

    def test_15_select_invalid_condition_name(self):
        with self.assertRaises(ValueError):
            query = "SELECT * FROM catalog_search_data"
            self.handler.native_query(query)


if __name__ == "__main__":
    unittest.main()
