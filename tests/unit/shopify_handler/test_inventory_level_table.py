import pytest
from mindsdb.integrations.handlers.shopify_handler import shopify_tables
from unittest import mock
from mindsdb_sql import parse_sql


def test_inventory_level_select(shopify_handler, shopify_session, inventory_level_table, sample_inventory_levels):
    inventory_item_id = 49148385
    location_id = 655441491
    inventory_level_table.handler.connect = mock.Mock(return_value=shopify_session)
    shopify_tables.shopify.ShopifyResource.activate_session = mock.Mock()
    inventory_level_mock_1 = mock.MagicMock()
    inventory_level_mock_2 = mock.MagicMock()
    shopify_tables.shopify.InventoryLevel.find = mock.Mock(return_value=[inventory_level_mock_1, inventory_level_mock_2])
    inventory_level_mock_1.to_dict = mock.Mock(return_value=sample_inventory_levels[0])
    inventory_level_mock_2.to_dict = mock.Mock(return_value=sample_inventory_levels[1])
    query = (f"SELECT * from shopify_handler.inventory_level "
             f"where inventory_item_ids = {inventory_item_id} "
             f"and location_ids = {location_id};")
    parsed_query = parse_sql(query, dialect='mindsdb')
    response = inventory_level_table.select(parsed_query)
    first_item = response.iloc[0]
    assert first_item['inventory_item_id'] == inventory_item_id


def test_inventory_level_select_other_fields_unsupported(shopify_handler,
                                                         shopify_session,
                                                         inventory_level_table,
                                                         sample_inventory_levels):
    inventory_level_table.handler.connect = mock.Mock(return_value=shopify_session)
    shopify_tables.shopify.ShopifyResource.activate_session = mock.Mock()
    inventory_level_mock = mock.MagicMock()
    shopify_tables.shopify.InventoryLevel.find = mock.Mock(return_value=[inventory_level_mock])
    inventory_level_mock.to_dict = mock.Mock(return_value=sample_inventory_levels[0])
    query = f"SELECT * from shopify_handler.inventory_level where available=4;"
    parsed_query = parse_sql(query, dialect='mindsdb')
    with pytest.raises(NotImplementedError) as e:
        inventory_level_table.select(parsed_query)
    assert e.value.args[0] == "inventory_item_ids or location_ids column has to be present in where clause."


def test_inventory_level_select_unsupported_operators(shopify_handler,
                                                      shopify_session,
                                                      inventory_level_table,
                                                      sample_inventory_levels):
    inventory_level_table.handler.connect = mock.Mock(return_value=shopify_session)
    shopify_tables.shopify.ShopifyResource.activate_session = mock.Mock()
    inventory_level_mock = mock.MagicMock()
    shopify_tables.shopify.InventoryLevel.find = mock.Mock(return_value=[inventory_level_mock])
    inventory_level_mock.to_dict = mock.Mock(return_value=sample_inventory_levels[0])
    query = f"SELECT * from shopify_handler.inventory_level where inventory_item_ids > 10000;"
    parsed_query = parse_sql(query, dialect='mindsdb')
    with pytest.raises(NotImplementedError) as e:
        inventory_level_table.select(parsed_query)
    assert e.value.args[0] == "Only '=' operator is supported for inventory_item_ids column."

    query = f"SELECT * from shopify_handler.inventory_level where location_ids > 10000;"
    parsed_query = parse_sql(query, dialect='mindsdb')
    with pytest.raises(NotImplementedError) as e:
        inventory_level_table.select(parsed_query)
    assert e.value.args[0] == "Only '=' operator is supported for location_ids column."
