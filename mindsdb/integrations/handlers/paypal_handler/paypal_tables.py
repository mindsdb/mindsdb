import paypalrestsdk
import pandas as pd
from typing import Text, List, Dict
import requests
from mindsdb.utilities import log

from mindsdb_sql.parser import ast
from mindsdb.integrations.libs.api_handler import APITable

from mindsdb.integrations.handlers.utilities.query_utilities import SELECTQueryParser, SELECTQueryExecutor


class PaymentsTable(APITable):

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Pulls PayPal Payments data.
        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query
        Returns
        -------
        pd.DataFrame
            PayPal Payments matching the query
        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'payments',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()
        
        payments_df = pd.json_normalize(self.get_payments(count=result_limit))
        select_statement_executor = SELECTQueryExecutor(
            payments_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        payments_df = select_statement_executor.execute_query()

        return payments_df

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_payments(count=1)).columns.tolist()

    def get_payments(self, **kwargs) -> List[Dict]:
        connection = self.handler.connect()
        payments = paypalrestsdk.Payment.all(kwargs, api=connection)
        return [payment.to_dict() for payment in payments['payments']]

class InvoicesTable(APITable):

    def select(self, query: ast.Select) -> pd.DataFrame:
        select_statement_parser = SELECTQueryParser(
            query,
            'invoices',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        invoices_df = pd.json_normalize(self.get_invoices(count=result_limit))
        select_statement_executor = SELECTQueryExecutor(
            invoices_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        invoices_df = select_statement_executor.execute_query()

        return invoices_df

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_invoices(count=1)).columns.tolist()

    def get_invoices(self, **kwargs) -> List[Dict]:
        connection = self.handler.connect()
        invoices = paypalrestsdk.Invoice.all(kwargs, api=connection)
        return [invoice.to_dict() for invoice in invoices['invoices']]

class OrdersTable(APITable):

    def select(self, query: ast.Select) -> pd.DataFrame:
        select_statement_parser = SELECTQueryParser(
            query,
            'orders',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        orders_df = pd.json_normalize(self.get_orders())

        select_statement_executor = SELECTQueryExecutor(
            orders_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        orders_df = select_statement_executor.execute_query()

        return orders_df

    def get_columns(self,order_id) -> List[Text]:
        return pd.json_normalize(self.get_orders()).columns.tolist()

    def get_columns(self) -> list:
         return pd.json_normalize(self.get_orders(count=1)).columns.tolist()


    def get_orders(self, order_id: str = None, **kwargs) -> List[Dict]:
        connection = self.handler.connect()

        headers =  { "Content-Type": "application/json", "Authorization":"Bearer A21AAL75cp_sDYJltjrVVB_CwiEJV7hMmtkFRL6RAGaNxTpWZ-1uaRpP4aUaMcmoP42Po4PvDXg1u4Xf8uGl9RKfy1XofOQmA"}
 
        response = requests.get('https://api-m.sandbox.paypal.com/v2/checkout/orders/49993801DS344915N', headers=headers)
        return response.json()

    def create_orders(self):

       headers = {
    'Authorization': 'Bearer A21AAL75cp_sDYJltjrVVB_CwiEJV7hMmtkFRL6RAGaNxTpWZ',
}
       data = '{ "intent": "CAPTURE", "purchase_units": [ { "reference_id": "d9f80740-38f0-11e8-b467-0ed5f89f718b", "amount": { "currency_code": "USD", "value": "100.00" } } ], "payment_source": { "paypal": { "experience_context": { "payment_method_preference": "IMMEDIATE_PAYMENT_REQUIRED", "brand_name": "EXAMPLE INC", "locale": "en-US", "landing_page": "LOGIN", "shipping_preference": "SET_PROVIDED_ADDRESS", "user_action": "PAY_NOW", "return_url": "https://example.com/returnUrl", "cancel_url": "https://example.com/cancelUrl" } } } }'
 
       response = requests.post('https://api-m.sandbox.paypal.com/v2/checkout/orders', headers=headers , data=data)
       return response.json()


class SubscriptionsTable(APITable):
    def select(self, query: ast.Select) -> pd.DataFrame:
        select_statement_parser = SELECTQueryParser(
            query,
            'subscriptions',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        subscriptions_df = pd.json_normalize(self.get_subscriptions(count=result_limit))
        select_statement_executor = SELECTQueryExecutor(
            subscriptions_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        subscriptions_df = select_statement_executor.execute_query()
        return subscriptions_df

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_subscriptions(count = 1)).columns.tolist()

    def get_subscriptions(self, **kwargs) -> List[Dict]:
        connection = self.handler.connect()
        subscriptions = paypalrestsdk.BillingPlan.all(kwargs, api=connection)
        return [subscription.to_dict() for subscription in subscriptions['plans']]