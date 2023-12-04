import paypalrestsdk
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions

import pandas as pd
from typing import Text, List, Dict

from mindsdb_sql.parser import ast
from mindsdb.integrations.libs.api_handler import APITable

from mindsdb.integrations.handlers.utilities.query_utilities import SELECTQueryParser, SELECTQueryExecutor
import requests
import json

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


class OrdersTable(APITable):
    """The PayPal Orders Table implementation"""
    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Pulls PayPal Orders data.
        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query
        Returns
        -------
        pd.DataFrame
            PayPal Orders matching the query
        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        select_statement_parser = SELECTQueryParser(
            query,
            'orders',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        search_params = {"ids": []}
        subset_where_conditions = []
        for op, arg1, arg2 in where_conditions:
            if arg1 == 'id':
                if op == '=':
                    search_params["ids"].append(arg2)
                else:
                    raise NotImplementedError("Only '=' operator is supported for 'ids' column")
            elif arg1 in ['state', 'amount', 'create_time', 'update_time', 'links', 'pending_reason', 'parent_payment']:
                subset_where_conditions.append([op, arg1, arg2])

        if search_params['ids'] == []:
            raise NotImplementedError("id column is required for this table")

        orders_df = pd.json_normalize(self.get_orders(search_params))
        self.clean_selected_columns(selected_columns)
        select_statement_executor = SELECTQueryExecutor(
            orders_df,
            selected_columns,
            subset_where_conditions,
            order_by_conditions
        )
        orders_df = select_statement_executor.execute_query()
        return orders_df

    @staticmethod
    def clean_selected_columns(selected_cols) -> None:
        if "ids" in selected_cols:
            selected_cols.remove("ids")
            selected_cols.append("id")

    def get_columns(self) -> List[Text]:
         return ["id",
                 "status",
                 "intent",
                 "purchase_units",
                 "links",
                 "create_time"]

    # restore this or similar header list for API 2.0 refactor
    #restore this list when restore paypalsdk api, and retired the request call
        # return ["id",
        #         "status",
        #         "intent",
        #         "gross_total_amount.value",
        #         "gross_total_amount.currency",
        #         "purchase_units",
        #         "metadata.supplementary_data",
        #         "redirect_urls.return_url",
        #         "redirect_urls.cancel_url",
        #         "links",
        #         "create_time"]

    def getDataFromApiCall(self,connection,value):
        """
        Replacing the Paypal SDK API call with this request
        The current API v1/payments/orders/<ORDER_ID>
        is broken and deprecated.  They will not fix.
        Replacing the call with v2/checkout/orders/ API call.
        New 2.0 API due out soon and this entire module syould be
        refactored  to use the new API module
        Old calls below can be reverted when 2.0 api refactor happens
        and this direct call using the requests module can be removed.
        """
        client_id = connection.client_id
        client_secret = connection.client_secret
        token_url = 'https://api-m.sandbox.paypal.com/v1/oauth2/token'
        data = {
            'grant_type': 'client_credentials'
        }
        auth = (client_id, client_secret)
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        response = requests.post(token_url, data=data, auth=auth, headers=headers)
        access_token = None
        if response.status_code == 200:
            response_data = response.json()
            access_token = response_data["access_token"]
        else:
            raise ValueError("Could not get auth-token, check client-id and secret-key")
        bearer = "Bearer " + access_token
        headers = {
            'Authorization': bearer,
        }
        url = "https://api-m.sandbox.paypal.com/v2/checkout/orders/" + value
        response = requests.get(url, headers=headers)
        data_dict = json.loads(response.text)
        return data_dict

    def get_orders(self, kwargs) -> List[Dict]:
        connection = self.handler.connect()

        orders = []
        for value in kwargs["ids"]:
            try:
                # restore 'order = ...' call below for API 2.0 refactor
                data_dict=self.getDataFromApiCall(connection,value)
                # order = paypalrestsdk.Order.find(value, api=connection)
            except paypalrestsdk.exceptions.ResourceNotFound:
                continue
            # restore 'order.append' call below for API 2.0 refactor
            orders.append(data_dict)
            # orders.append(order.to_dict())
        return orders


class PayoutsTable(APITable):

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Pulls PayPal payouts data.
        Parameters
        ----------
        query : ast.Select
            Given SQL SELECT query
        Returns
        -------
        pd.DataFrame
            PayPal payouts matching the query
        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'payouts',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        conditions = extract_comparison_conditions(query.where)

        payout_batch_id = ""

        for a_where in conditions:
            if a_where[1] == "payout_batch_id":
                if a_where[0] != "=":
                    raise ValueError("Unsupported where operation for state")

                payout_batch_id = a_where[2]

        payouts_data = self.get_payout(payout_batch_id)  # Get the data
        payouts_df = pd.DataFrame(payouts_data)  # Create a DataFrame

        select_statement_executor = SELECTQueryExecutor(
            payouts_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )

        payouts_df = select_statement_executor.execute_query()

        return payouts_df

    def get_columns(self) -> List[Text]:
        return [
            "payout_batch_id",
            "batch_status",
            "time_created",
            "time_completed",
            "sender_batch_id",
            "email_subject",
            "email_message",
            "funding_source",
            "amount_currency",
            "amount_value",
            "fees_currency",
            "fees_value",
        ]

    def get_payout(self, payout_batch_id:str) -> List[Dict]:
        connection = self.handler.connect()
        endpoint = f"v1/payments/payouts/{payout_batch_id}"
        payout = connection.get(endpoint)

        payout_data = {
            "payout_batch_id": payout['batch_header']['payout_batch_id'],
            "batch_status": payout['batch_header']['batch_status'],
            "time_created": payout['batch_header']['time_created'],
            "time_completed": payout['batch_header']['time_completed'],
            "sender_batch_id": payout['batch_header']['sender_batch_header']['sender_batch_id'],
            "email_subject": payout['batch_header']['sender_batch_header']['email_subject'],
            "email_message": payout['batch_header']['sender_batch_header']['email_message'],
            "funding_source": payout['batch_header']['funding_source'],
            "amount_currency": payout['batch_header']['amount']['currency'],
            "amount_value": payout['batch_header']['amount']['value'],
            "fees_currency": payout['batch_header']['fees']['currency'],
            "fees_value": payout['batch_header']['fees']['value'],
        }

        return [payout_data]

