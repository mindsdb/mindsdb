from mindsdb.integrations.handlers.shopify_handler import shopify_tables
from unittest import mock
from mindsdb_sql import parse_sql


def test_carrier_service_select(shopify_handler, shopify_session, carrier_service_table, sample_carriers):
    id_to_query = 14079244
    carrier_service_table.handler.connect = mock.Mock(return_value=shopify_session)
    shopify_tables.shopify.ShopifyResource.activate_session = mock.Mock()
    carrier_mock_1 = mock.MagicMock()
    shopify_tables.shopify.CarrierService.find = mock.Mock(return_value=[carrier_mock_1])
    carrier_mock_1.to_dict = mock.Mock(return_value=sample_carriers[0])
    query = f"SELECT * from shopify_handler.carriers where id = {id_to_query};"
    parsed_query = parse_sql(query, dialect='mindsdb')
    response = carrier_service_table.select(parsed_query)
    first_item = response.iloc[0]
    assert first_item['id'] == id_to_query
