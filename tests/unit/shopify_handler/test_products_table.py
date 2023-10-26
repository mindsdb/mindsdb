import logging

import pytest
import pandas as pd

from mindsdb.integrations.handlers.shopify_handler import shopify_tables
from unittest import mock
from mindsdb_sql import parse_sql


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
