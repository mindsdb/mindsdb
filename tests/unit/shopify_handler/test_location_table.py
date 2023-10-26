from mindsdb.integrations.handlers.shopify_handler import shopify_tables
from unittest import mock
from mindsdb_sql import parse_sql


def test_locations_select(shopify_handler, shopify_session, locations_table, sample_locations):
    id_to_query = 72001192158
    locations_table.handler.connect = mock.Mock(return_value=shopify_session)
    shopify_tables.shopify.ShopifyResource.activate_session = mock.Mock()
    location_mock_1 = mock.MagicMock()
    shopify_tables.shopify.Location.find = mock.Mock(return_value=[location_mock_1])
    location_mock_1.to_dict = mock.Mock(return_value=sample_locations[0])
    query = f"SELECT * from shopify_handler.locations where id = {id_to_query};"
    parsed_query = parse_sql(query, dialect='mindsdb')
    response = locations_table.select(parsed_query)
    first_item = response.iloc[0]
    assert first_item['id'] == id_to_query
