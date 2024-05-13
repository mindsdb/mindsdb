import unittest
from mindsdb.integrations.handlers.financial_modeling_prep_handler.financial_modeling_handler import FinancialModelingHandler
from mindsdb.integrations.libs.response import RESPONSE_TYPE

class FinancialModelingHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.handler = FinancialModelingHandler('test_financial_handler', connection_data)
    
    def test_0_select_query(self):
        query = "SELECT * FROM my_table.daily_chart_table WHERE symbol = 'AAPL'"
        result = self.handler.query(query)
        assert result.type is RESPONSE_TYPE.TABLE
