from mindsdb.integrations.handlers.coinbase_handler.coinbase_tables import CoinBaseAggregatedTradesTable
from mindsdb.integrations.handlers.coinbase_handler.coinbase_handler import CoinBaseHandler
from mindsdb_sql.parser import ast
from mindsdb_sql.parser.ast.select.star import Star
from mindsdb_sql.parser.ast.select.identifier import Identifier

from unittest.mock import Mock

import pandas as pd
import unittest


class CoinBaseAggregatedTradesTableTest(unittest.TestCase):
    def test_get_columns_returns_all_columns(self):
        api_handler = Mock(CoinBaseHandler)
        trades_table = CoinBaseAggregatedTradesTable(api_handler)
        # Order matters.
        expected_columns = [
            'symbol',
            'low',
            'high',
            'open',
            'close',
            'volume',
            'timestamp',
            'timestamp_iso'
        ]
        self.assertListEqual(trades_table.get_columns(), expected_columns)

    def test_select_returns_all_columns(self):
        api_handler = Mock(CoinBaseHandler)
        api_handler.call_coinbase_api.return_value = pd.DataFrame([
            [
                'BTC-USD',                   # symbol
                34330.01,                    # low
                34623.21,                    # high
                34493.51,                    # open
                34349.16,                    # close
                719.064133,                  # volume
                1698710400,                  # timestamp
                "2023-10-30T20:00:00-04:00"  # timestamp_iso
            ]
        ])
        trades_table = CoinBaseAggregatedTradesTable(api_handler)

        select_all = ast.Select(
            targets=[Star()],
            from_table='coinbase_candle_data',
            where='coinbase_candle_data.symbol = "BTC-USD"'
        )

        all_trade_data = trades_table.select(select_all)
        first_trade_data = all_trade_data.iloc[0]

        self.assertEqual(all_trade_data.shape[1], 8)
        self.assertEqual(first_trade_data['symbol'], 'BTC-USD')
        self.assertEqual(first_trade_data['low'], 34330.01)
        self.assertEqual(first_trade_data['high'], 34623.21)
        self.assertEqual(first_trade_data['open'], 34493.51)
        self.assertEqual(first_trade_data['close'], 34349.16)
        self.assertEqual(first_trade_data['volume'], 719.064133)
        self.assertEqual(first_trade_data['timestamp'], 1698710400)
        self.assertEqual(first_trade_data['timestamp_iso'], '2023-10-30T20:00:00-04:00')

    def test_select_returns_only_selected_columns(self):
        api_handler = Mock(CoinBaseHandler)
        api_handler.call_coinbase_api.return_value = pd.DataFrame([
            [
                'BTC-USD',                   # symbol
                34330.01,                    # low
                34623.21,                    # high
                34493.51,                    # open
                34349.16,                    # close
                719.064133,                  # volume
                1698710400,                  # timestamp
                "2023-10-30T20:00:00-04:00"  # timestamp_iso
            ]
        ])
        trades_table = CoinBaseAggregatedTradesTable(api_handler)

        open_time_identifier = Identifier(path_str='open')
        close_time_identifier = Identifier(path_str='close')
        select_times = ast.Select(
            targets=[open_time_identifier, close_time_identifier],
            from_table='coinbase_candle_data',
            where='coinbase_candle_data.symbol = "BTC-USD"'
        )

        all_trade_data = trades_table.select(select_times)
        first_trade_data = all_trade_data.iloc[0]

        self.assertEqual(all_trade_data.shape[1], 2)
        self.assertEqual(first_trade_data['open'], 34493.51)
        self.assertEqual(first_trade_data['close'], 34349.16)
