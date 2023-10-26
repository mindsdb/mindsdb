import logging

import pytest
import pandas as pd

from mindsdb.integrations.handlers.shopify_handler import shopify_tables
from unittest import mock
from mindsdb_sql import parse_sql


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
