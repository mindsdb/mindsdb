import pytest
import shopify
import logging
import pandas as pd
import responses

from mindsdb.integrations.handlers.shopify_handler import shopify_handler as handler
from mindsdb.integrations.libs.api_handler_exceptions import ConnectionFailed, MissingConnectionParams, InvalidNativeQuery
from unittest import mock
from mindsdb.integrations.handlers.shopify_handler import shopify_tables
from mindsdb_sql import parse_sql


# Tests for Shopify Handler

def test_init_without_credentials():
    with pytest.raises(MissingConnectionParams) as e:
        shopify_handler = handler.ShopifyHandler('shopify_handler')
    assert e.value.args[0] == 'Incomplete parameters passed to Shopify Handler'


def test_connect(shopify_handler, shopify_session):
    handler.shopify.Session = mock.Mock(return_value=shopify_session)
    connection = shopify_handler.connect()
    assert connection == shopify_session
    assert connection == shopify_handler.connection
    assert shopify_handler.yotpo_app_key == 'some_yotpo_app_key'
    assert shopify_handler.yotpo_access_token == 'some_yotpo_access_token'
    assert shopify_handler.is_connected is True


def test_connect_with_no_credentials(shopify_handler):
    shopify_handler.kwargs['connection_data'] = None
    with pytest.raises(MissingConnectionParams) as e:
        shopify_handler.connect()
    assert e.value.args[0] == "Incomplete parameters passed to Shopify Handler"


def test_connect_with_connection_instantiated(shopify_handler, shopify_session):
    shopify_handler.is_connected = True
    shopify_handler.connection = shopify_session
    connection = shopify_handler.connect()
    assert connection is shopify_session


def test_check_connection(shopify_handler, shopify_session, response_ok):
    handler.connect = mock.Mock(return_value=shopify_session)
    handler.shopify.ShopifyResource.activate_session = mock.Mock()
    handler.shopify.Shop.current = mock.Mock(return_value=shopify.Shop())
    handler.requests.get = mock.Mock(return_value=response_ok)
    response = shopify_handler.check_connection()
    assert response.success is True


def test_check_connection_yotpo_failure(shopify_handler, shopify_session, response_400):
    handler.connect = mock.Mock(return_value=shopify_session)
    handler.shopify.ShopifyResource.activate_session = mock.Mock()
    handler.shopify.Shop.current = mock.Mock(return_value=shopify.Shop())
    handler.requests.get = mock.Mock(return_value=response_400)
    response = shopify_handler.check_connection()
    assert response.success is False


def test_check_connection_with_exception(shopify_handler, shopify_session, response_400):
    handler.connect = mock.Mock(return_value=shopify_session)
    handler.shopify.ShopifyResource.activate_session = mock.Mock()
    handler.shopify.Shop.current = mock.Mock(
        return_value=shopify.Shop(),
        side_effect=Exception("Some Exception occurred")
    )
    handler.requests.get = mock.Mock(return_value=response_400)
    with pytest.raises(ConnectionFailed) as e:
        response = shopify_handler.check_connection()
    assert e.value.args[0] == 'Conenction to Shopify failed.'
    # assert response.success is False


def test_native_query(shopify_handler, shopify_session, sample_orders):
    shopify_handler.query = mock.Mock(return_value=sample_orders)
    res = shopify_handler.native_query("SELECT * from shopify_datasource.orders where id = 1;")
    assert res == sample_orders


def test_invalid_native_query(shopify_handler, shopify_session, sample_orders):
    shopify_handler.query = mock.Mock(return_value=sample_orders)
    query = "SELECT * from shopify_datasource.orders where SOME_INVALID_PART id = 1;"
    with pytest.raises(InvalidNativeQuery) as e:
        res = shopify_handler.native_query(query)
    assert e.value.args[0] == f"The query {query} is invalid."


# Tests for Customers Table

def test_customer_select(shopify_handler, shopify_session, customers_table, sample_customers):
    id_to_query = 7092329906398
    customers_table.handler.connect = mock.Mock(return_value=shopify_session)
    shopify_tables.shopify.ShopifyResource.activate_session = mock.Mock()
    customer_mock_1 = mock.MagicMock()
    customer_mock_2 = mock.MagicMock()
    shopify_tables.shopify.Customer.find = mock.Mock(return_value=[customer_mock_1, customer_mock_2])
    customer_mock_1.to_dict = mock.Mock(return_value=sample_customers[0])
    customer_mock_2.to_dict = mock.Mock(return_value=sample_customers[1])
    query = f"SELECT * from shopify_handler.products where id = {id_to_query};"
    parsed_query = parse_sql(query, dialect='mindsdb')
    response = customers_table.select(parsed_query)
    first_item = response.iloc[0]
    assert first_item['id'] == id_to_query


def test_customers_insert(customers_table, shopify_session, caplog):
    caplog.set_level(logging.DEBUG)
    query = """INSERT INTO shopify_handler.customers ('first_name', 'last_name', 'email', 'phone', 'tags') 
                VALUES ('Some FName', 'Some LName', 'somebody@email.com', 'Some phone number', 'Some tags'); 
            """
    parsed_query = parse_sql(query)
    customers_table.handler.connect = mock.Mock(return_value=shopify_session)
    shopify_tables.shopify.ShopifyResource.activate_session = mock.Mock()
    customer_data = {
        'first_name': ['Some FName'],
        'last_name': ['Some LName'],
        'email': ['somebody@email.com'],
        'phone': ['Some phone number'],
        'tags': ['Some tags'],
        'id': [12345]
    }
    customer_data = pd.DataFrame.from_dict(customer_data)
    shopify_tables.shopify.Customer.create = mock.Mock(return_value=customer_data)
    customers_table.insert(parsed_query)
    assert "Customer {0: 12345} created" in caplog.text


def test_customers_insert_fail(customers_table, shopify_session, caplog):
    caplog.set_level(logging.DEBUG)
    query = """INSERT INTO shopify_handler.customers ('first_name', 'last_name', 'email', 'phone', 'tags') 
                VALUES ('Some FName', 'Some LName', 'somebody@email.com', 'Some phone number', 'Some tags'); 
            """
    parsed_query = parse_sql(query)
    customers_table.handler.connect = mock.Mock(return_value=shopify_session)
    shopify_tables.shopify.ShopifyResource.activate_session = mock.Mock()
    customer_data = {
        'first_name': ['Some FName'],
        'last_name': ['Some LName'],
        'email': ['somebody@email.com'],
        'phone': ['Some phone number'],
        'tags': ['Some tags'],
        'id': [12345]
    }
    customer_data = pd.DataFrame.from_dict(customer_data)
    shopify_tables.shopify.Customer.create = mock.Mock(return_value=customer_data)
    customers_table.insert(parsed_query)
    assert 'Customer {0: 12345} created' in caplog.text


def test_customers_insert_exception(customers_table, shopify_session, caplog):
    caplog.set_level(logging.DEBUG)
    query = """INSERT INTO shopify_handler.customers ('first_name', 'last_name', 'email', 'phone', 'tags') 
                VALUES ('Some FName', 'Some LName', 'somebody@email.com', 'Some phone number', 'Some tags');
            """
    parsed_query = parse_sql(query)
    customers_table.handler.connect = mock.Mock(return_value=shopify_session)
    shopify_tables.shopify.ShopifyResource.activate_session = mock.Mock()
    customer_data = {
        'error': ['Error in creating customer']
    }
    customer_data = pd.DataFrame.from_dict(customer_data)
    shopify_tables.shopify.Customer.create = mock.Mock(return_value=customer_data)
    with pytest.raises(Exception) as e:
        customers_table.insert(parsed_query)
    assert 'Customer creation failed' == e.value.args[0]


def test_customers_delete(customers_table, sample_customers, shopify_session, caplog):
    caplog.set_level(logging.DEBUG)
    id_to_delete = 7092329906398
    query = f"""
        DELETE from shopify_handler.customers WHERE id = {id_to_delete};
    """
    parsed_query = parse_sql(query)
    shopify_tables.CustomersTable.get_customers = mock.Mock(return_value=sample_customers)
    customers_table.handler.connect = mock.Mock(return_value=shopify_session)
    shopify_tables.shopify.ShopifyResource.activate_session = mock.Mock()
    shopify_tables.shopify.Customer.find = mock.Mock()
    shopify_tables.shopify.ShopifyResource.destroy = mock.Mock()
    customers_table.delete(parsed_query)
    assert f"Customer {id_to_delete} deleted" in caplog.text


def test_customers_update(customers_table, shopify_session, sample_customers, caplog):
    caplog.set_level(logging.DEBUG)
    id_to_update = 7092329906398
    query = f"""
        UPDATE shopify_handler.customers
        SET first_name = 'New FName'
        WHERE id = {id_to_update};
    """
    parsed_query = parse_sql(query)
    shopify_tables.CustomersTable.get_customers = mock.Mock(return_value=sample_customers)
    customers_table.handler.connect = mock.Mock(return_value=shopify_session)
    shopify_tables.shopify.ShopifyResource.activate_session = mock.Mock()
    shopify_tables.shopify.Customer.find = mock.Mock()
    shopify_tables.shopify.Customer.__setattr__ = mock.Mock()
    shopify_tables.shopify.Customer.save = mock.Mock()
    customers_table.update(parsed_query)
    assert f"Customer {id_to_update} updated" in caplog.text


# Tests for Products Table

def test_products_select(products_table, sample_products, shopify_session):
    id_to_query = 8153100648670
    products_table.handler.connect = mock.Mock(return_value=shopify_session)
    shopify_tables.shopify.ShopifyResource.activate_session = mock.Mock()
    product_mock_1 = mock.MagicMock()
    product_mock_2 = mock.MagicMock()
    shopify_tables.shopify.Product.find = mock.Mock(return_value=[product_mock_1, product_mock_2])
    product_mock_1.to_dict = mock.Mock(return_value=sample_products[0])
    product_mock_2.to_dict = mock.Mock(return_value=sample_products[1])
    query = f"SELECT * from shopify_handler.products where id = {id_to_query};"
    parsed_query = parse_sql(query, dialect='mindsdb')
    response = products_table.select(parsed_query)
    first_item = response.iloc[0]
    assert first_item['id'] == id_to_query


def test_products_insert(products_table, shopify_session, caplog):
    caplog.set_level(logging.DEBUG)
    query = """INSERT INTO shopify_handler.products ('title', 'body_html', 'vendor', 'product_type', 'tags', 'status') 
                VALUES ('Some Title', 'Some body html', 'Some vendor', 'Some product type', 'Some tags', 'Some status'); 
            """
    parsed_query = parse_sql(query)
    products_table.handler.connect = mock.Mock(return_value=shopify_session)
    shopify_tables.shopify.ShopifyResource.activate_session = mock.Mock()
    product_data = {
        'title': ['Some Title'],
        'body_html': ['Some body html'],
        'vendor': ['Some vendor'],
        'product_type': ['Some product type'],
        'tags': ['Some tags'],
        'status': ['Some status'],
        'id': [12345]
    }
    product_data = pd.DataFrame.from_dict(product_data)
    shopify_tables.shopify.Product.create = mock.Mock(return_value=product_data)
    products_table.insert(parsed_query)
    assert "Product {0: 12345} created" in caplog.text


def test_products_insert_fail(products_table, shopify_session, caplog):
    caplog.set_level(logging.DEBUG)
    query = """INSERT INTO shopify_handler.products ('title', 'body_html', 'vendor', 'product_type', 'tags', 'status') 
                    VALUES ('Some Title', 'Some body html', 'Some vendor', 'Some product type', 'Some tags', 'Some status'); 
                """
    parsed_query = parse_sql(query)
    products_table.handler.connect = mock.Mock(return_value=shopify_session)
    shopify_tables.shopify.ShopifyResource.activate_session = mock.Mock()
    product_data = {
        'title': ['Some Title'],
        'body_html': ['Some body html'],
        'vendor': ['Some vendor'],
        'product_type': ['Some product type'],
        'tags': ['Some tags'],
        'status': ['Some status'],
        'id': [12345]
    }
    product_data = pd.DataFrame.from_dict(product_data)
    shopify_tables.shopify.Product.create = mock.Mock(return_value=product_data)
    products_table.insert(parsed_query)
    assert 'Product {0: 12345} created' in caplog.text


def test_products_insert_exception(products_table, shopify_session, caplog):
    caplog.set_level(logging.DEBUG)
    query = """INSERT INTO shopify_handler.products ('title', 'body_html', 'vendor', 'product_type', 'tags', 'status')
                    VALUES ('Some Title', 'Some body html', 'Some vendor', 'Some product type', 'Some tags', 'Some status');
                """
    parsed_query = parse_sql(query)
    products_table.handler.connect = mock.Mock(return_value=shopify_session)
    shopify_tables.shopify.ShopifyResource.activate_session = mock.Mock()
    product_data = {
        'error': ['Error in creating product']
    }
    product_data = pd.DataFrame.from_dict(product_data)
    shopify_tables.shopify.Product.create = mock.Mock(return_value=product_data)
    with pytest.raises(Exception) as e:
        products_table.insert(parsed_query)
    assert 'Product creation failed' == e.value.args[0]


def test_products_delete(products_table, sample_products, shopify_session, caplog):
    caplog.set_level(logging.DEBUG)
    query = """
        DELETE from shopify_handler.products WHERE id = 8153100648670;
    """
    parsed_query = parse_sql(query)
    shopify_tables.ProductsTable.get_products = mock.Mock(return_value=sample_products)
    products_table.handler.connect = mock.Mock(return_value=shopify_session)
    shopify_tables.shopify.ShopifyResource.activate_session = mock.Mock()
    shopify_tables.shopify.Product.find = mock.Mock()
    shopify_tables.shopify.ShopifyResource.destroy = mock.Mock()
    products_table.delete(parsed_query)
    assert "Product 8153100648670 deleted" in caplog.text


def test_products_update(products_table, shopify_session, sample_products, caplog):
    caplog.set_level(logging.DEBUG)
    query = """
        UPDATE shopify_handler.products
        SET vendor = 'New Vendor'
        WHERE id = 8153100648670;
    """
    parsed_query = parse_sql(query)
    shopify_tables.ProductsTable.get_products = mock.Mock(return_value=sample_products)
    products_table.handler.connect = mock.Mock(return_value=shopify_session)
    shopify_tables.shopify.ShopifyResource.activate_session = mock.Mock()
    shopify_tables.shopify.Product.find = mock.Mock()
    shopify_tables.shopify.Product.__setattr__ = mock.Mock()
    shopify_tables.shopify.Product.save = mock.Mock()
    products_table.update(parsed_query)
    assert "Product 8153100648670 updated" in caplog.text


# Tests for Carrier Service Table

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


# Tests for Inventory Level Table

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


# Tests for Customer Reviews Table

def test_customer_review_select(shopify_handler,
                                shopify_session,
                                customer_reviews_table,
                                sample_customer_reviews,
                                yotpo_review_response):
    id_to_query = 34789515
    shopify_handler.connect()
    customer_review_mock_1 = mock.MagicMock()
    shopify_tables.shopify.InventoryLevel.find = mock.Mock(return_value=[customer_review_mock_1])
    customer_review_mock_1.to_dict = mock.Mock(return_value=sample_customer_reviews[0])
    responses.add(responses.GET,
                  f"https://api.yotpo.com/v1/apps/{shopify_handler.yotpo_app_key}/reviews?count=0&utoken={shopify_handler.yotpo_access_token}",
                  json=yotpo_review_response,
                  status=200)
    shopify_tables.requests.Response.json = mock.Mock(return_value=yotpo_review_response)
    query = f"SELECT * from shopify_handler.customer_reviews where id = {id_to_query};"
    parsed_query = parse_sql(query, dialect='mindsdb')
    response = customer_reviews_table.select(parsed_query)
    first_item = response.iloc[0]
    assert first_item['id'] == id_to_query


def test_customer_review_select_without_yotpo_credentials(shopify_handler,
                                                          shopify_session,
                                                          customer_reviews_table):
    shopify_handler.yotpo_app_key = None
    shopify_handler.yotpo_access_token = None
    id_to_query = 34789515
    shopify_tables.shopify.Session = mock.Mock(return_value=shopify_session)
    shopify_tables.shopify.ShopifyResource.activate_session = mock.Mock()
    query = f"SELECT * from shopify_handler.customer_reviews where id = {id_to_query};"
    parsed_query = parse_sql(query, dialect='mindsdb')
    with pytest.raises(Exception) as e:
        customer_reviews_table.select(parsed_query)
    assert "You need to provide 'yotpo_app_key' and 'yotpo_access_token' to retrieve customer reviews." in e.value.args[0]


# Tests for Locations Table

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


# Tests for Orders Table

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


# Tests for Sales Channel Table

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


# Tests for Shipping Zone Table

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
