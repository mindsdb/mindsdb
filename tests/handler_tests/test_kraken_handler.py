from mindsdb.integrations.handlers.kraken_handler.kraken_tables import KrakenTradesTable
from mindsdb.integrations.handlers.kraken_handler.kraken_handler import KrakenHandler

from unittest.mock import Mock
from unittest import mock

import unittest
import pandas as pd


class KrakenAggregatedTradesTableTest(unittest.TestCase):
    def test_get_columns_returns_all_columns(self):
        api_handler = Mock(KrakenHandler)
        trades_table = KrakenTradesTable(api_handler)
        expected_columns = [
            "ordertxid",
            "postxid",
            "pair",
            "time",
            "type",
            "ordertype",
            "price",
            "cost",
            "fee",
            "vol",
            "margin",
            "misc"
        ]
        self.assertListEqual(trades_table.get_columns(), expected_columns)

    def test_select_returns_all_columns(self):
        api_handler = Mock(KrakenHandler)
        api_handler.call_kraken_api.return_value = pd.DataFrame([
            [
                "TCWJEG-FL4SZ-3FKGH6",
                "OQCLML-BW3P3-BUCMWZ",
                "TKH2SE-M7IF5-CFI7LT",
                "XXBTZUSD",
                1688667769.6396,
                "buy",
                "limit",
                "30010.00000",
                "300.10000",
                "0.00000",
                "0.01000000",
                "0.00000",
                ""
            ]
        ])
        trades_table = KrakenTradesTable(api_handler)
        queries = []
        all_trade_data = trades_table.select_trade_history(queries)
        print(all_trade_data.iloc[0])
        first_trade_data = all_trade_data.iloc[0]

        self.assertEqual(all_trade_data.shape[1], 13)
        self.assertEqual(first_trade_data[0], "TCWJEG-FL4SZ-3FKGH6")
        self.assertEqual(first_trade_data[1], "OQCLML-BW3P3-BUCMWZ")
        self.assertEqual(first_trade_data[2], "TKH2SE-M7IF5-CFI7LT")
        self.assertEqual(first_trade_data[3], "XXBTZUSD")
        self.assertEqual(first_trade_data[4], 1688667769.6396)
        self.assertEqual(first_trade_data[5], 'buy')
        self.assertEqual(first_trade_data[6], 'limit')
        self.assertEqual(first_trade_data[7], "30010.00000")
        self.assertEqual(first_trade_data[8], '300.10000')
        self.assertEqual(first_trade_data[9], "0.00000")
        self.assertEqual(first_trade_data[10], '0.01000000')
        self.assertEqual(first_trade_data[11], '0.00000')
        self.assertEqual(first_trade_data[12], '')
