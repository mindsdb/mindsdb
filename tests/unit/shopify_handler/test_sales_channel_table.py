from mindsdb.integrations.handlers.shopify_handler import shopify_tables
from unittest import mock
from mindsdb_sql import parse_sql


def test_sales_channel_select(shopify_handler, shopify_session, sales_channel_table, sample_sales_channel):
    id_to_query = 107948212446
    sales_channel_table.handler.connect = mock.Mock(return_value=shopify_session)
    shopify_tables.shopify.ShopifyResource.activate_session = mock.Mock()
    sales_channel_mock = mock.MagicMock()
    shopify_tables.shopify.Publication.find = mock.Mock(return_value=[sales_channel_mock])
    sales_channel_mock.to_dict = mock.Mock(return_value=sample_sales_channel[1])
    query = f"SELECT * from shopify_handler.sales_channel where id = {id_to_query};"
    parsed_query = parse_sql(query, dialect='mindsdb')
    response = sales_channel_table.select(parsed_query)
    first_item = response.iloc[0]
    assert first_item['id'] == id_to_query

