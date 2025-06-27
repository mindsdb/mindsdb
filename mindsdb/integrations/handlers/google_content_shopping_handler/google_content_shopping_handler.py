import json

import pandas as pd
from pandas import DataFrame
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from .google_content_shopping_tables import AccountsTable, OrdersTable, ProductsTable
from mindsdb.integrations.libs.api_handler import APIHandler, FuncParser
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
)
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class GoogleContentShoppingHandler(APIHandler):
    """
        A class for handling connections and interactions with the Google Content API for Shopping.
    """
    name = 'google_content_shopping'

    def __init__(self, name: str, **kwargs):
        """
        Initialize the Google Content API for Shopping handler.
        Args:
            name (str): name of the handler
            kwargs (dict): additional arguments
        """
        super().__init__(name)
        self.token = None
        self.service = None
        self.connection_data = kwargs.get('connection_data', {})
        self.fs_storage = kwargs['file_storage']
        self.credentials_file = self.connection_data.get('credentials', None)
        self.merchant_id = self.connection_data.get('merchant_id', None)
        self.credentials = None
        self.scopes = ['https://www.googleapis.com/auth/content']
        self.is_connected = False
        accounts = AccountsTable(self)
        self.accounts = accounts
        self._register_table('Accounts', accounts)
        orders = OrdersTable(self)
        self.orders = orders
        self._register_table('Orders', orders)
        products = ProductsTable(self)
        self.products = products
        self._register_table('Products', products)

    def connect(self):
        """
        Set up any connections required by the handler
        Should return output of check_connection() method after attempting
        connection. Should switch self.is_connected.
        Returns:
            HandlerStatusResponse
        """
        if self.is_connected is True:
            return self.service
        if self.credentials_file:
            try:
                json_str_bytes = self.fs_storage.file_get('token_content.json')
                json_str = json_str_bytes.decode()
                self.credentials = Credentials.from_authorized_user_info(info=json.loads(json_str), scopes=self.scopes)
            except Exception:
                self.credentials = None
            if not self.credentials or not self.credentials.valid:
                if self.credentials and self.credentials.expired and self.credentials.refresh_token:
                    self.credentials.refresh(Request())
                else:
                    self.credentials = service_account.Credentials.from_service_account_file(
                        self.credentials_file, scopes=self.scopes)
            # Save the credentials for the next run
            json_str = self.credentials.to_json()
            self.fs_storage.file_set('token_content.json', json_str.encode())
            self.service = build('content', 'v2.1', credentials=self.credentials)
        return self.service

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler
        Returns:
            HandlerStatusResponse
        """
        response = StatusResponse(False)

        try:
            self.connect()
            response.success = True
        except Exception as e:
            logger.error(f'Error connecting to Google Content API for Shopping: {e}!')
            response.error_message = e

        self.is_connected = response.success
        return response

    def native_query(self, query: str = None) -> Response:
        """
        Receive raw query and act upon it somehow.
        Args:
            query (Any): query in native format (str for sql databases,
                dict for mongo, api's json etc)
        Returns:
            HandlerResponse
        """
        method_name, params = FuncParser().from_string(query)

        df = self.call_application_api(method_name, params)

        return Response(
            RESPONSE_TYPE.TABLE,
            data_frame=df
        )

    def get_accounts(self, params: dict = None) -> DataFrame:
        """
        Get accounts
        Args:
            params (dict): query parameters
        Returns:
            DataFrame
        """
        service = self.connect()
        page_token = None
        accounts = pd.DataFrame(columns=self.accounts.get_columns())
        if params['account_id']:
            result = service.accounts().get(merchantId=self.merchant_id, accountId=params['account_id']).execute()
            accounts = pd.DataFrame(result, columns=self.accounts.get_columns())
            return accounts
        while True:
            result = service.accounts().list(merchantId=self.merchant_id, page_token=page_token, **params).execute()
            accounts = pd.concat(
                [accounts, pd.DataFrame(result['resources'], columns=self.accounts.get_columns())],
                ignore_index=True
            )
            page_token = result.get('nextPageToken')
            if not page_token:
                break

        if params['startId'] and params['endId']:
            start_id = int(params['startId'])
            end_id = int(params['endId'])
        elif params['startId']:
            start_id = int(params['startId'])
            end_id = start_id + 10
        elif params['endId']:
            end_id = int(params['endId'])
            start_id = end_id - 10
        else:
            raise Exception('startId or endId must be specified')

        accounts = accounts.drop(accounts[(accounts['id'] < start_id) | (accounts['id'] > end_id)].index)

        return accounts

    def delete_accounts(self, params: dict = None) -> DataFrame:
        """
        Delete accounts
        Args:
            params (dict): query parameters
        Returns:
            DataFrame
        """
        service = self.connect()
        args = {}
        if params['force']:
            args = {'force': params['force']}
        if params['accountId']:
            result = service.accounts().delete(merchantId=self.merchant_id, accountId=params['accountId'],
                                               **args).execute()
            return result
        else:
            df = pd.DataFrame(columns=['accountId', 'status'])
            if not params['startId']:
                start_id = int(params['endId']) - 10
            elif not params['endId']:
                end_id = int(params['startId']) + 10
            else:
                start_id = int(params['startId'])
                end_id = int(params['endId'])

            for i in range(start_id, end_id):
                service.accounts().delete(merchantId=self.merchant_id, accountId=i, **args).execute()
                df = pd.concat([df, pd.DataFrame([{'accountId': str(i), 'status': 'deleted'}])], ignore_index=True)
            return df

    def get_orders(self, params: dict = None) -> DataFrame:
        """
        Get orders
        Args:
            params (dict): query parameters
        Returns:
            DataFrame
        """
        service = self.connect()
        page_token = None
        orders = pd.DataFrame(columns=self.orders.get_columns())
        args = {
            key: value for key, value in params.items() if key in ['maxResults',
                                                                   'statuses',
                                                                   'acknowledged',
                                                                   'placedDateStart',
                                                                   'placedDateEnd',
                                                                   'orderBy']
            and value is not None
        }
        if params['order_id']:
            result = service.orders().get(merchantId=self.merchant_id, orderId=params['order_id'], **args).execute()
            orders = pd.DataFrame(result, columns=self.orders.get_columns())
            return orders
        while True:
            result = service.orders().list(merchantId=self.merchant_id, page_token=page_token, **args).execute()
            orders = pd.concat(
                [orders, pd.DataFrame(result['resources'], columns=self.orders.get_columns())],
                ignore_index=True
            )
            page_token = result.get('nextPageToken')
            if not page_token:
                break

        if params['startId'] and params['endId']:
            start_id = int(params['startId'])
            end_id = int(params['endId'])
        elif params['startId']:
            start_id = int(params['startId'])
            end_id = start_id + 10
        elif params['endId']:
            end_id = int(params['endId'])
            start_id = end_id - 10
        else:
            raise Exception('startId or endId must be specified')

        orders = orders.drop(orders[(orders['id'] < start_id) | (orders['id'] > end_id)].index)

        return orders

    def delete_orders(self, params: dict = None) -> DataFrame:
        """
        Delete orders
        Args:
            params (dict): query parameters
        Returns:
            DataFrame
        """
        service = self.connect()
        if params['order_id']:
            result = service.orders().delete(merchantId=self.merchant_id, orderId=params['order_id']).execute()
            return result
        else:
            df = pd.DataFrame(columns=['orderId', 'status'])
            if not params['startId']:
                start_id = int(params['endId']) - 10
            elif not params['endId']:
                end_id = int(params['startId']) + 10
            else:
                start_id = int(params['startId'])
                end_id = int(params['endId'])

            for i in range(start_id, end_id):
                service.orders().delete(merchantId=self.merchant_id, orderId=i).execute()
                df = pd.concat([df, pd.DataFrame([{'orderId': str(i), 'status': 'deleted'}])], ignore_index=True)
            return df

    def get_products(self, params: dict = None) -> DataFrame:
        """
        Get products
        Args:
            params (dict): query parameters
        Returns:
            DataFrame
        """
        service = self.connect()
        page_token = None
        products = pd.DataFrame(columns=self.products.get_columns())
        if params['product_id']:
            result = service.products().get(merchantId=self.merchant_id, productId=params['product_id']).execute()
            products = pd.DataFrame(result, columns=self.products.get_columns())
            return products
        while True:
            result = service.products().list(merchantId=self.merchant_id, page_token=page_token).execute()
            products = pd.concat(
                [products, pd.DataFrame(result['resources'], columns=self.products.get_columns())],
                ignore_index=True
            )
            page_token = result.get('nextPageToken')
            if not page_token:
                break

        if params['startId'] and params['endId']:
            start_id = int(params['startId'])
            end_id = int(params['endId'])
        elif params['startId']:
            start_id = int(params['startId'])
            end_id = start_id + 10
        elif params['endId']:
            end_id = int(params['endId'])
            start_id = end_id - 10
        else:
            raise Exception('startId or endId must be specified')

        products = products.drop(products[(products['id'] < start_id) | (products['id'] > end_id)].index)

        return products

    def update_products(self, params: dict = None) -> DataFrame:
        """
        Update products
        Args:
            params (dict): query parameters
        Returns:
            DataFrame
        """
        body = {
            key: value for key, value in params.items() if key in self.products.get_columns()
        }
        service = self.connect()
        if params['product_id']:
            result = service.products().update(merchantId=self.merchant_id, productId=params['product_id'],
                                               updateMask=params['updateMask'], body=body).execute()

            return result
        else:
            df = pd.DataFrame(columns=['productId', 'status'])
            if not params['startId']:
                start_id = int(params['endId']) - 10
            elif not params['endId']:
                end_id = int(params['startId']) + 10
            else:
                start_id = int(params['startId'])
                end_id = int(params['endId'])

            for i in range(start_id, end_id):
                service.products().update(merchantId=self.merchant_id, productId=i,
                                          updateMask=params['updateMask'], body=body).execute()
                df = pd.concat([df, pd.DataFrame([{'productId': str(i), 'status': 'updated'}])], ignore_index=True)
            return df

    def delete_products(self, params: dict = None) -> DataFrame:
        """
        Delete products
        Args:
            params (dict): query parameters
        Returns:
            DataFrame
        """
        service = self.connect()
        args = {
            key: value for key, value in params.items() if key in ['feedId'] and value is not None
        }
        if params['product_id']:
            result = service.products().delete(merchantId=self.merchant_id, productId=params['product_id'],
                                               **args).execute()
            return result
        else:
            df = pd.DataFrame(columns=['productId', 'status'])
            if not params['startId']:
                start_id = int(params['endId']) - 10
            elif not params['endId']:
                end_id = int(params['startId']) + 10
            else:
                start_id = int(params['startId'])
                end_id = int(params['endId'])

            for i in range(start_id, end_id):
                service.products().delete(merchantId=self.merchant_id, productId=i, **args).execute()
                df = pd.concat([df, pd.DataFrame([{'productId': str(i), 'status': 'deleted'}])], ignore_index=True)
            return df

    def call_application_api(self, method_name: str = None, params: dict = None) -> DataFrame:
        """
        Call Google Search API and map the data to pandas DataFrame
        Args:
            method_name (str): method name
            params (dict): query parameters
        Returns:
            DataFrame
        """
        if method_name == 'get_accounts':
            return self.get_accounts(params)
        elif method_name == 'delete_accounts':
            return self.delete_accounts(params)
        elif method_name == 'get_orders':
            return self.get_orders(params)
        elif method_name == 'delete_orders':
            return self.delete_orders(params)
        elif method_name == 'get_products':
            return self.get_products(params)
        elif method_name == 'update_products':
            return self.update_products(params)
        elif method_name == 'delete_products':
            return self.delete_products(params)
        else:
            raise NotImplementedError(f'Unknown method {method_name}')
