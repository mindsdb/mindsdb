import paypalrestsdk
import pandas as pd
from typing import Text, List, Dict
import requests

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
            self.get_columns(None)
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        # order_id = '7SP60397AN682533Y'
        order_id = None

        for condition in where_conditions:
            if condition.column.name == 'order_id':
               order_id = condition.value
            break  

        if order_id is None:
            raise ValueError("order_id must be provided in the WHERE clause of the query")
        
        selected_columns = self.get_columns(order_id)
        
        orders_df = pd.json_normalize(self.get_orders(count=result_limit,order_id=order_id))
        select_statement_executor = SELECTQueryExecutor(
            orders_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        orders_df = select_statement_executor.execute_query()

        return orders_df
    
    def get_columns(self,order_id) -> List[Text]:
        return pd.json_normalize(self.get_orders(order_id)).columns.tolist()

    def get_orders(self, order_id, **kwargs) -> List[Dict]:
       headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer A21AAIUR5R-kPcdVNm6D6zNxxFlSFaE2smdAEZhGFAAsq92VGjrHYBjvZyMh4dzDklXjpDj3rL7vvDRmf5S4fHXcyK0PDwaVg',
    }

       response = requests.get(f'https://api-m.sandbox.paypal.com/v2/checkout/orders/{order_id}', headers=headers)

       if response.status_code == 200:
        data = response.json()
        print(data)
        return data
       else:
        print(f"Request failed with status code: {response.status_code}")
        print(f"Response content: {response.text}")
        return []



