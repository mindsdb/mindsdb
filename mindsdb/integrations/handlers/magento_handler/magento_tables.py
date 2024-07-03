from typing import Any, Dict, List, Text

import pandas as pd
from mindsdb_sql.parser import ast

from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.handlers.query_utilities import (
    DELETEQueryExecutor,
    DELETEQueryParser,
    INSERTQueryParser,
    SELECTQueryExecutor,
    SELECTQueryParser,
    UPDATEQueryExecutor,
    UPDATEQueryParser,
)
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class BaseTable(APITable):
    """The magento base Table implementation"""

    def __init__(self, handler):
        super().__init__(handler)
        self.endpoint = None
        self.columns: list = []
        self.madantory: list = []

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the magento "GET" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Magento endpoint matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query, self.endpoint, self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = (
            select_statement_parser.parse_query()
        )

        endpoint_df = pd.json_normalize(self.get_endpoint(where=where_conditions))
        select_statement_executor = SELECTQueryExecutor(
            endpoint_df, selected_columns, where_conditions, order_by_conditions
        )
        logger.info(f"where: {where_conditions}")
        endpoint_df = select_statement_executor.execute_query()

        return endpoint_df

    def insert(self, query: ast.Insert) -> None:
        """Inserts data into the Shopify "POST /products" API endpoint.

        Parameters
        ----------
        query : ast.Insert
           Given SQL INSERT query

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        insert_statement_parser = INSERTQueryParser(
            query,
            supported_columns=self.columns,
            mandatory_columns=self.madantory,
            all_mandatory=False,
        )
        endpoint_data = insert_statement_parser.parse_query()
        self.create_endpoint(endpoint_data)

    def update(self, query: ast.Update) -> None:
        """Updates data from the Shopify "PUT" API endpoint.

        Parameters
        ----------
        query : ast.Update
           Given SQL UPDATE query

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        update_statement_parser = UPDATEQueryParser(query)
        values_to_update, where_conditions = update_statement_parser.parse_query()

        endpoint_df = pd.json_normalize(self.get_endpoint())
        update_query_executor = UPDATEQueryExecutor(endpoint_df, where_conditions)

        endpoint_df = update_query_executor.execute_query()
        endpoint_ids = endpoint_df["id"].tolist()
        self.update_endpoint(endpoint_ids, values_to_update)

    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from the Magento "DELETE" API endpoint.

        Parameters
        ----------
        query : ast.Delete
           Given SQL DELETE query

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        delete_statement_parser = DELETEQueryParser(query)
        where_conditions = delete_statement_parser.parse_query()

        endpoint_df = pd.json_normalize(self.get_endpoint())

        delete_query_executor = DELETEQueryExecutor(endpoint_df, where_conditions)

        endpoint_df = delete_query_executor.execute_query()

        endpoint_ids = endpoint_df["id"].tolist()
        self.delete_endpoint(endpoint_ids)

    def get_columns(self) -> List[Text]:
        return self.columns

    def get_endpoint(self, where=None, **kwargs) -> List[Dict]:
        condition_symbols = {
            "=": "eq",
            ">": "gt",
            ">=": "gteq",
            "<": "lt",
            "<=": "lteq",
            "<>": "neq",
            "like": "like",
        }
        api_session = self.handler.connect()

        if not where:
            try:
                response = api_session.search(self.endpoint).get_all()
                logger.info(f"this: {response}")
                if type(response) is list:
                    return [data.data for data in response]
                return response.data
            except Exception as e:
                raise e

        endpoint = api_session.search(self.endpoint)
        for condition in where:
            if condition[0] in condition_symbols:
                endpoint = endpoint.add_criteria(
                    condition[1], condition[2], condition_symbols[condition[0]]
                )
        if endpoint.execute() is None:
            logger.info("Data not found")
            raise Exception("Data not found!")
        if type(endpoint.execute()) is list:
            logger.info(endpoint.execute())
            return [data.data for data in endpoint.execute()]
        return [endpoint.execute().data]

    def delete_endpoint(self, endpoint_ids: List[int]) -> None:
        api_session = self.handler.connect()

        for endpoint_id in endpoint_ids:
            url = api_session.url_for(f"{self.endpoint}/{endpoint_id}")
            api_session.delete(url)
            logger.info(f"{self.endpoint} {endpoint_id} deleted")

    def create_endpoint(self, endpoint_data: Dict[Text, Any]) -> None:
        api_session = self.handler.connect()
        url = api_session.url_for(f"{self.endpoint}")
        api_session.put(url, endpoint_data)
        logger.info(f"{self.endpoint} created")

    def update_endpoint(
        self, endpoint_ids: List[int], values_to_update: Dict[Text, Any]
    ) -> None:
        api_session = self.handler.connect()
        for endpoint_id in endpoint_ids:
            url = api_session.url_for(f"{self.endpoint}/{endpoint_id}")
            api_session.post(url, values_to_update)
            logger.info(f"{self.endpoint} {endpoint_id} updated")


class ProductsTable(BaseTable):
    def __init__(self, handler):
        super().__init__(handler)
        self.endpoint = "products"
        self.columns = [
            "id",
            "sku",
            "name",
            "attribute_set_id",
            "price",
            "status",
            "visibility",
            "type_id",
            "created_at",
            "updated_at",
            "product_links",
            "options",
            "media_gallery_entries",
            "tier_prices",
            "custom_attributes",
            "extension_attributes.website_ids",
            "extension_attributes.category_links",
        ]
        self.madantory = []


class CustomersTable(BaseTable):
    def __init__(self, handler):
        super().__init__(handler)
        self.endpoint = "customers"
        self.columns = [
            "id",
            "group_id",
            "default_billing",
            "default_shipping",
            "created_at",
            "updated_at",
            "created_in",
            "dob",
            "email",
            "firstname",
            "lastname",
            "middlename",
            "prefix",
            "suffix",
            "gender",
            "store_id",
            "taxvat",
            "website_id",
            "addresses",
            "disable_auto_group_change",
            "extension_attributes.is_subscribed",
        ]
        self.madantory = []


class OrdersTable(BaseTable):
    def __init__(self, handler):
        super().__init__(handler)
        self.endpoint = "orders"
        self.columns = [
            "adjustment_negative",
            "adjustment_positive",
            "base_adjustment_negative",
            "base_adjustment_positive",
            "base_currency_code",
            "base_discount_amount",
            "base_discount_invoiced",
            "base_discount_refunded",
            "base_grand_total",
            "base_discount_tax_compensation_amount",
            "base_discount_tax_compensation_invoiced",
            "base_discount_tax_compensation_refunded",
            "base_shipping_amount",
            "base_shipping_discount_amount",
            "base_shipping_discount_tax_compensation_amnt",
            "base_shipping_incl_tax",
            "base_shipping_invoiced",
            "base_shipping_refunded",
            "base_shipping_tax_amount",
            "base_shipping_tax_refunded",
            "base_subtotal",
            "base_subtotal_incl_tax",
            "base_subtotal_invoiced",
            "base_subtotal_refunded",
            "base_tax_amount",
            "base_tax_invoiced",
            "base_tax_refunded",
            "base_total_due",
            "base_total_invoiced",
            "base_total_invoiced_cost",
            "base_total_offline_refunded",
            "base_total_paid",
            "base_total_refunded",
            "base_to_global_rate",
            "base_to_order_rate",
            "billing_address_id",
            "created_at",
            "customer_email",
            "customer_firstname",
            "customer_is_guest",
            "customer_lastname",
            "customer_note_notify",
            "discount_amount",
            "discount_invoiced",
            "discount_refunded",
            "email_sent",
            "entity_id",
            "global_currency_code",
            "grand_total",
            "discount_tax_compensation_amount",
            "discount_tax_compensation_invoiced",
            "discount_tax_compensation_refunded",
            "increment_id",
            "is_virtual",
            "order_currency_code",
            "protect_code",
            "quote_id",
            "remote_ip",
            "shipping_amount",
            "shipping_description",
            "shipping_discount_amount",
            "shipping_discount_tax_compensation_amount",
            "shipping_incl_tax",
            "shipping_invoiced",
            "shipping_refunded",
            "shipping_tax_amount",
            "shipping_tax_refunded",
            "state",
            "status",
            "store_currency_code",
            "store_id",
            "store_name",
            "store_to_base_rate",
            "store_to_order_rate",
            "subtotal",
            "subtotal_incl_tax",
            "subtotal_invoiced",
            "subtotal_refunded",
            "tax_amount",
            "tax_invoiced",
            "tax_refunded",
            "total_due",
            "total_invoiced",
            "total_item_count",
            "total_offline_refunded",
            "total_paid",
            "total_qty_ordered",
            "total_refunded",
            "updated_at",
            "weight",
            "items",
            "status_histories",
            "billing_address.address_type",
            "billing_address.city",
            "billing_address.company",
            "billing_address.country_id",
            "billing_address.email",
            "billing_address.entity_id",
            "billing_address.firstname",
            "billing_address.lastname",
            "billing_address.parent_id",
            "billing_address.postcode",
            "billing_address.region",
            "billing_address.region_code",
            "billing_address.region_id",
            "billing_address.street",
            "billing_address.telephone",
            "payment.account_status",
            "payment.additional_information",
            "payment.amount_ordered",
            "payment.amount_paid",
            "payment.amount_refunded",
            "payment.base_amount_ordered",
            "payment.base_amount_paid",
            "payment.base_amount_refunded",
            "payment.base_shipping_amount",
            "payment.base_shipping_captured",
            "payment.base_shipping_refunded",
            "payment.cc_exp_year",
            "payment.cc_last4",
            "payment.cc_ss_start_month",
            "payment.cc_ss_start_year",
            "payment.entity_id",
            "payment.method",
            "payment.parent_id",
            "payment.shipping_amount",
            "payment.shipping_captured",
            "payment.shipping_refunded",
            "extension_attributes.shipping_assignments",
            "extension_attributes.payment_additional_info",
            "extension_attributes.applied_taxes",
            "extension_attributes.item_applied_taxes",
        ]
        self.madantory = []


class InvoicesTable(BaseTable):
    def __init__(self, handler):
        super().__init__(handler)
        self.endpoint = "invoices"
        self.columns = [
            "base_currency_code",
            "base_discount_amount",
            "base_grand_total",
            "base_discount_tax_compensation_amount",
            "base_shipping_amount",
            "base_shipping_discount_tax_compensation_amnt",
            "base_shipping_incl_tax",
            "base_shipping_tax_amount",
            "base_subtotal",
            "base_subtotal_incl_tax",
            "base_tax_amount",
            "base_to_global_rate",
            "base_to_order_rate",
            "billing_address_id",
            "can_void_flag",
            "created_at",
            "discount_amount",
            "entity_id",
            "global_currency_code",
            "grand_total",
            "discount_tax_compensation_amount",
            "increment_id",
            "order_currency_code",
            "order_id",
            "shipping_address_id",
            "shipping_amount",
            "shipping_discount_tax_compensation_amount",
            "shipping_incl_tax",
            "shipping_tax_amount",
            "state",
            "store_currency_code",
            "store_id",
            "store_to_base_rate",
            "store_to_order_rate",
            "subtotal",
            "subtotal_incl_tax",
            "tax_amount",
            "total_qty",
            "updated_at",
            "items",
            "comments",
        ]
        self.madantory = []


class CategoriesTable(BaseTable):
    def __init__(self, handler):
        super().__init__(handler)
        self.endpoint = "categories"
        self.columns = [
            "id",
            "parent_id",
            "name",
            "position",
            "level",
            "children",
            "created_at",
            "updated_at",
            "path",
            "include_in_menu",
            "custom_attributes",
        ]
        self.madantory = []
