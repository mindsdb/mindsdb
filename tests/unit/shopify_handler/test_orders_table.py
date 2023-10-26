import logging

import pytest
import pandas as pd

from mindsdb.integrations.handlers.shopify_handler import shopify_tables
from unittest import mock
from mindsdb_sql import parse_sql


def test_orders_select(shopify_handler, shopify_session, orders_table, sample_orders):
    id_to_query = 5522106319070
    orders_table.handler.connect = mock.Mock(return_value=shopify_session)
    shopify_tables.shopify.ShopifyResource.activate_session = mock.Mock()
    order_mock_1 = mock.MagicMock()
    shopify_tables.shopify.Order.find = mock.Mock(return_value=[order_mock_1])
    order_mock_1.to_dict = mock.Mock(return_value=sample_orders[0])
    query = f"SELECT * from shopify_handler.orders where id = {id_to_query};"
    parsed_query = parse_sql(query, dialect='mindsdb')
    response = orders_table.select(parsed_query)
    first_item = response.iloc[0]
    assert first_item['id'] == id_to_query

