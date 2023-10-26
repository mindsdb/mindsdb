import pytest
import responses
from mindsdb.integrations.handlers.shopify_handler import shopify_tables
from unittest import mock
from mindsdb_sql import parse_sql


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
