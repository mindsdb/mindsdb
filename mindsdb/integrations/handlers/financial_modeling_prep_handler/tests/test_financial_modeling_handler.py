import unittest
from mindsdb.integrations.handlers.financial_modeling_prep_handler.financial_modeling_handler import FinancialModelingHandler
from mindsdb.integrations.libs.response import RESPONSE_TYPE
from mindsdb_sql import parse_sql


class FinancialModelingHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        connection_data = {
            "api_key": "GJvlw9YgVm5J4KIxdP1VPkvWzt747Q6j"
        }
        cls.handler = FinancialModelingHandler('test_financial_handler', connection_data)
    
    def test_0_select_query(self):
        query = parse_sql("SELECT * FROM my_table.daily_chart_table WHERE symbol = 'AAPL'")
        result = self.handler.query(query)
        # assert result.type is RESPONSE_TYPE.TABLE
        assert result.type is RESPONSE_TYPE.OK
        # self.assertEqual(result, "call api")
        
    # def test_1_select_limit_query(self):
    #     query = parse_sql("SELECT * FROM my_table.daily_chart_table WHERE symbol = 'AAPL' LIMIT 5")
    #     result = self.handler.query(query)
    #     assert result.shape[0] == 5 


if __name__ == '__main__':
    unittest.main()