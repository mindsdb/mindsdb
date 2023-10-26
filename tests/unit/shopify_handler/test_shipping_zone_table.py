from mindsdb.integrations.handlers.shopify_handler import shopify_tables
from unittest import mock
from mindsdb_sql import parse_sql


def test_shipping_zone_channel_select(shopify_handler, shopify_session, shipping_zone_table, sample_shipping_zones):
    id_to_query = 376959271134
    shipping_zone_table.handler.connect = mock.Mock(return_value=shopify_session)
    shopify_tables.shopify.ShopifyResource.activate_session = mock.Mock()
    shipping_zone_mock = mock.MagicMock()
    shopify_tables.shopify.ShippingZone.find = mock.Mock(return_value=[shipping_zone_mock])
    shipping_zone_mock.to_dict = mock.Mock(return_value=sample_shipping_zones[0])
    query = f"SELECT * from shopify_handler.shipping_zone where id = {id_to_query};"
    parsed_query = parse_sql(query, dialect='mindsdb')
    response = shipping_zone_table.select(parsed_query)
    first_item = response.iloc[0]
    assert first_item['id'] == id_to_query

