from mindsdb.integrations.handlers.binance_handler.binance_tables import BinanceAggregatedTradesTable
from mindsdb.integrations.handlers.binance_handler.binance_handler import BinanceHandler
from mindsdb_sql.parser import ast
from mindsdb_sql.parser.ast.select.star import Star
from mindsdb_sql.parser.ast.select.identifier import Identifier

from unittest.mock import Mock

import pandas as pd
import unittest


class BinanceAggregatedTradesTableTest(unittest.TestCase):
    def test_get_columns_returns_all_columns(self):
        api_handler = Mock(BinanceHandler)
        trades_table = BinanceAggregatedTradesTable(api_handler)
        # Order matters.
        expected_columns = [
            'symbol',
            'open_time',
            'open_price',
            'high_price',
            'low_price',
            'close_price',
            'volume',
            'close_time',
            'quote_asset_volume',
            'number_of_trades',
            'taker_buy_base_asset_volume',
            'taker_buy_quote_asset_volume'
        ]
        self.assertListEqual(trades_table.get_columns(), expected_columns)

    def test_select_returns_all_columns(self):
        api_handler = Mock(BinanceHandler)
        api_handler.call_binance_api.return_value = pd.DataFrame([
            [
                'symbol',  # Symbol
                1499040000000,  # Kline open time
                '0.01634790',  # Open price
                '0.80000000',  # High price
                '0.01575800',  # Low price
                '0.01577100',  # Close price
                '148976.11427815',  # Volume
                1499644799999,  # Kline Close time
                '2434.19055334',  # Quote asset volume
                308,  # Number of trades
                '1756.87402397',  # Taker buy base asset volume
                '28.46694368',  # Taker buy quote asset volume
            ]
        ])
        trades_table = BinanceAggregatedTradesTable(api_handler)

        select_all = ast.Select(
            targets=[Star()],
            from_table='aggregated_trade_data',
            where='aggregated_trade_data.symbol = "symbol"'
        )

        all_trade_data = trades_table.select(select_all)
        first_trade_data = all_trade_data.iloc[0]

        self.assertEqual(all_trade_data.shape[1], 12)
        self.assertEqual(first_trade_data['symbol'], 'symbol')
        self.assertEqual(first_trade_data['open_time'], 1499040000000)
        self.assertEqual(first_trade_data['open_price'], '0.01634790')
        self.assertEqual(first_trade_data['high_price'], '0.80000000')
        self.assertEqual(first_trade_data['low_price'], '0.01575800')
        self.assertEqual(first_trade_data['close_price'], '0.01577100')
        self.assertEqual(first_trade_data['volume'], '148976.11427815')
        self.assertEqual(first_trade_data['close_time'], 1499644799999)
        self.assertEqual(first_trade_data['quote_asset_volume'], '2434.19055334')
        self.assertEqual(first_trade_data['number_of_trades'], 308)
        self.assertEqual(first_trade_data['taker_buy_base_asset_volume'], '1756.87402397')
        self.assertEqual(first_trade_data['taker_buy_quote_asset_volume'], '28.46694368')

    def test_select_returns_only_selected_columns(self):
        api_handler = Mock(BinanceHandler)
        api_handler.call_binance_api.return_value = pd.DataFrame([
            [
                'symbol',  # Symbol
                1499040000000,  # Kline open time
                '0.01634790',  # Open price
                '0.80000000',  # High price
                '0.01575800',  # Low price
                '0.01577100',  # Close price
                '148976.11427815',  # Volume
                1499644799999,  # Kline Close time
                '2434.19055334',  # Quote asset volume
                308,  # Number of trades
                '1756.87402397',  # Taker buy base asset volume
                '28.46694368',  # Taker buy quote asset volume
            ]
        ])
        trades_table = BinanceAggregatedTradesTable(api_handler)

        open_time_identifier = Identifier(path_str='open_time')
        close_time_identifier = Identifier(path_str='close_time')
        select_times = ast.Select(
            targets=[open_time_identifier, close_time_identifier],
            from_table='aggregated_trade_data',
            where='aggregated_trade_data.symbol = "symbol"'
        )

        all_trade_data = trades_table.select(select_times)
        first_trade_data = all_trade_data.iloc[0]

        self.assertEqual(all_trade_data.shape[1], 2)
        self.assertEqual(first_trade_data['open_time'], 1499040000000)
        self.assertEqual(first_trade_data['close_time'], 1499644799999)
