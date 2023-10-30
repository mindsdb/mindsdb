import pytest
import shopify

from mindsdb.integrations.handlers.shopify_handler import shopify_handler as handler
from mindsdb.integrations.libs.api_handler_exceptions import ConnectionFailed, MissingConnectionParams, InvalidNativeQuery
from unittest import mock


def test_init_without_credentials():
    with pytest.raises(MissingConnectionParams) as e:
        shopif_handler = handler.ShopifyHandler('shopify_handler')
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

