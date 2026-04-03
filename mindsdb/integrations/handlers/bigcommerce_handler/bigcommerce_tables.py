from typing import List
from decimal import Decimal

import pandas as pd

from mindsdb.integrations.handlers.bigcommerce_handler.bigcommerce_api_client import BigCommerceAPIClient
from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator, SortColumn
from mindsdb.integrations.libs.api_handler import MetaAPIResource
from mindsdb.utilities import log


logger = log.getLogger(__name__)


def _make_filter(conditions: list[FilterCondition] | None, op_map: dict) -> dict:
    """Creates a filter dictionary, that can be used in the BigCommerce API.

    Args:
        conditions (list[FilterCondition]): The list of parsed filter conditions.
        op_map (dict): The mapping of filter operators to API parameters.

    Returns:
        dict: The filter dictionary.
    """
    filter = {}
    if conditions is None:
        return filter
    for condition in conditions:
        simple_op = op_map.get((condition.column, condition.op))
        if simple_op:
            value = condition.value
            if isinstance(value, list):
                value = ",".join(map(str, value))
            filter[simple_op] = value
            condition.applied = True
    return filter


def _make_df(result: list[dict], table: MetaAPIResource):
    """Converts a list of dictionaries to a pandas DataFrame.
    If the list is empty, an empty DataFrame is returned with the columns from the table.

    Args:
        result (list[dict]): The list of dictionaries to convert.
        table (MetaAPIResource): The table class.

    Returns:
        pd.DataFrame: The resulting DataFrame.
    """
    if len(result) == 0:
        result = pd.DataFrame([], columns=table.get_columns())
    else:
        result = pd.DataFrame(result)
    return result


def _make_sort_condition_v3(sort: list[SortColumn], sortable_columns: list[str]):
    """Creates a sort condition for the BigCommerce API v3.

    Args:
        sort (list[SortColumn]): The list of parsed sort columns.
        sortable_columns (list[str]): The list of sortable columns.

    Returns:
        dict: The sort condition, that can be used in the BigCommerce API v3.
    """
    sort_condition = None
    if isinstance(sort, list) and len(sort) > 1 and sort[0].column in sortable_columns:
        sort_column = sort[0]
        sort_condition = {
            "sort": sort_column.column,
            "direction": "asc" if sort_column.ascending else "desc",
        }
    return sort_condition


def _make_sort_condition_v2(sort: list[SortColumn], sortable_columns: list[str]):
    """Creates a sort condition for the BigCommerce API v2.

    Args:
        sort (list[SortColumn]): The list of parsed sort columns.
        sortable_columns (list[str]): The list of sortable columns.

    Returns:
        dict: The sort condition, that can be used in the BigCommerce API v2.
    """
    sort_condition = None
    if isinstance(sort, list) and len(sort) == 1 and sort[0].column in sortable_columns:
        sort_column = sort[0]
        sort_column.applied = True
        sort_condition = f"{sort_column.column}:{'asc' if sort_column.ascending else 'desc'}"
    return sort_condition


class BigCommerceOrdersTable(MetaAPIResource):
    """
    The table abstraction for the 'orders' resource of the BigCommerce API.
    """

    name = "orders"

    def list(
        self,
        conditions: list[FilterCondition] = None,
        limit: int = None,
        sort: list[SortColumn] = None,
        **kwargs,
    ):
        """Executes a parsed SELECT SQL query on the 'orders' resource of the BigCommerce API.

        Args:
            conditions (list[FilterCondition]): The list of parsed filter conditions.
            limit (int): The maximum number of records to return.
            sort (list[SortColumn]): The list of parsed sort columns.

        Returns:
            pd.DataFrame: The resulting DataFrame.
        """
        client: BigCommerceAPIClient = self.handler.connect()

        simple_op_map = {
            ("id", FilterOperator.GREATER_THAN): "min_id",
            ("id", FilterOperator.LESS_THAN): "max_id",
            ("total_inc_tax", FilterOperator.GREATER_THAN): "min_total",
            ("total_inc_tax", FilterOperator.LESS_THAN): "max_total",
            ("customer_id", FilterOperator.EQUAL): "customer_id",
            ("email", FilterOperator.EQUAL): "email",
            ("status_id", FilterOperator.EQUAL): "status_id",
            ("cart_id", FilterOperator.EQUAL): "cart_id",
            ("payment_method", FilterOperator.EQUAL): "payment_method",
            ("date_created", FilterOperator.GREATER_THAN): "min_date_created",
            ("date_created", FilterOperator.LESS_THAN): "max_date_created",
            ("date_modified", FilterOperator.GREATER_THAN): "min_date_modified",
            ("date_modified", FilterOperator.LESS_THAN): "max_date_modified",
            ("channel_id", FilterOperator.EQUAL): "channel_id",
            ("external_order_id", FilterOperator.EQUAL): "external_order_id",
        }

        filter = _make_filter(conditions, simple_op_map)

        for condition in conditions:
            if condition.applied:
                continue
            # region special case for filter "id = x"
            if condition.op == FilterOperator.EQUAL and condition.column == "id":
                filter["min_id"] = condition.value
                filter["max_id"] = condition.value
            # endregion

        sortable_columns = [
            "id",
            "customer_id",
            "date_created",
            "date_modified",
            "status_id",
            "channel_id",
            "external_id",
        ]
        sort_condition = _make_sort_condition_v3(sort, sortable_columns)

        result = client.get_orders(filter=filter, sort_condition=sort_condition, limit=limit)
        result = _make_df(result, self)

        decimal_columns = [meta["COLUMN_NAME"] for meta in self.meta_get_columns() if meta["DATA_TYPE"] == "DECIMAL"]
        for column_name in decimal_columns:
            if column_name in result:
                result[column_name] = result[column_name].apply(Decimal)

        return result

    def get_columns(self) -> List[str]:
        """
        Retrieves the attributes (columns) of the 'orders' resource.

        Returns:
            list[str]: A list of attributes (columns) of the 'orders' resource.
        """
        columns = self.meta_get_columns()
        return [column["COLUMN_NAME"] for column in columns]

    def meta_get_tables(self, table_name: str) -> dict:
        client: BigCommerceAPIClient = self.handler.connect()
        orders_count = client.get_orders_count()
        return {
            "table_name": self.name,
            "table_type": "BASE TABLE",
            "table_description": "",
            "row_count": orders_count,
        }

    def meta_get_columns(self, *args, **kwargs) -> List[str]:
        return [
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "id",
                "DATA_TYPE": "INT",
                "COLUMN_DESCRIPTION": "The ID of the order.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "date_modified",
                "DATA_TYPE": "DATETIME",
                "COLUMN_DESCRIPTION": "Value representing the last modification of the order. RFC-2822. This date time is always in UTC in the api response.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "date_shipped",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Value representing the date when the order is fully shipped. RFC-2822",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "cart_id",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "The cart ID from which this order originated, if applicable.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "status",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "The status will include one of the values defined under Order Statuses.",
            },
            {"TABLE_NAME": "orders", "COLUMN_NAME": "subtotal_tax", "DATA_TYPE": "DECIMAL", "COLUMN_DESCRIPTION": ""},
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "shipping_cost_tax",
                "DATA_TYPE": "DECIMAL",
                "COLUMN_DESCRIPTION": "",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "shipping_cost_tax_class_id",
                "DATA_TYPE": "INT",
                "COLUMN_DESCRIPTION": "",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "handling_cost_tax",
                "DATA_TYPE": "DECIMAL",
                "COLUMN_DESCRIPTION": "",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "handling_cost_tax_class_id",
                "DATA_TYPE": "INT",
                "COLUMN_DESCRIPTION": "",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "wrapping_cost_tax",
                "DATA_TYPE": "DECIMAL",
                "COLUMN_DESCRIPTION": "",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "wrapping_cost_tax_class_id",
                "DATA_TYPE": "INT",
                "COLUMN_DESCRIPTION": "",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "payment_status",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Payment status of the order. Allowed: authorized | captured | capture pending | declined | held for review | paid | partially refunded | pending | refunded | void | void pending",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "store_credit_amount",
                "DATA_TYPE": "DECIMAL",
                "COLUMN_DESCRIPTION": "Represents the store credit that the shopper has redeemed on this individual order.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "gift_certificate_amount",
                "DATA_TYPE": "DECIMAL",
                "COLUMN_DESCRIPTION": "",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "currency_id",
                "DATA_TYPE": "INT",
                "COLUMN_DESCRIPTION": "The display currency ID. Depending on the currency selected, the value can be different from the transactional currency.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "currency_code",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "The currency code of the display currency used to present prices to the shopper on the storefront. Depending on the currency selected, the value can be different from the transactional currency.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "currency_exchange_rate",
                "DATA_TYPE": "DECIMAL",
                "COLUMN_DESCRIPTION": "The exchange rate between the store's default currency and the display currency.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "default_currency_id",
                "DATA_TYPE": "INT",
                "COLUMN_DESCRIPTION": "The transactional currency ID.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "default_currency_code",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "The currency code of the transactional currency the shopper pays in.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "store_default_currency_code",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "The currency code of the store's default currency.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "store_default_to_transactional_exchange_rate",
                "DATA_TYPE": "DECIMAL",
                "COLUMN_DESCRIPTION": "The exchange rate between the store's default currency and the transactional currency used in the order.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "coupon_discount",
                "DATA_TYPE": "DECIMAL",
                "COLUMN_DESCRIPTION": "",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "shipping_address_count",
                "DATA_TYPE": "INT",
                "COLUMN_DESCRIPTION": "The number of shipping addresses associated with this transaction.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "is_deleted",
                "DATA_TYPE": "BOOL",
                "COLUMN_DESCRIPTION": "Indicates whether the order is deleted/archived.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "total_tax",
                "DATA_TYPE": "DECIMAL",
                "COLUMN_DESCRIPTION": "Total tax amount for the order.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "is_tax_inclusive_pricing",
                "DATA_TYPE": "BOOL",
                "COLUMN_DESCRIPTION": "Indicate whether the order's base prices include tax. If true, the base prices are inclusive of tax. If false, the base prices are exclusive of tax.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "is_email_opt_in",
                "DATA_TYPE": "BOOL",
                "COLUMN_DESCRIPTION": "Indicates whether the shopper has selected an opt-in check box (on the checkout page) to receive emails.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "order_source",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Reflects the origin of the order. It can affect the order's icon and source as defined in the control panel listing.",
            },
            {"TABLE_NAME": "orders", "COLUMN_NAME": "consignments", "DATA_TYPE": "JSON", "COLUMN_DESCRIPTION": ""},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "products", "DATA_TYPE": "JSON", "COLUMN_DESCRIPTION": ""},
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "shipping_addresses",
                "DATA_TYPE": "JSON",
                "COLUMN_DESCRIPTION": "",
            },
            {"TABLE_NAME": "orders", "COLUMN_NAME": "coupons", "DATA_TYPE": "JSON", "COLUMN_DESCRIPTION": ""},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "billing_address", "DATA_TYPE": "JSON", "COLUMN_DESCRIPTION": ""},
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "base_handling_cost",
                "DATA_TYPE": "DECIMAL",
                "COLUMN_DESCRIPTION": "The value of the base handling cost. The value can't be negative.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "base_shipping_cost",
                "DATA_TYPE": "DECIMAL",
                "COLUMN_DESCRIPTION": "The value of the base shipping cost. The value can't be negative.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "base_wrapping_cost",
                "DATA_TYPE": "DECIMAL",
                "COLUMN_DESCRIPTION": "The value of the base wrapping cost expressed as a floating point number to four decimal places in string format. The value can't be negative.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "channel_id",
                "DATA_TYPE": "INT",
                "COLUMN_DESCRIPTION": "Shows where the order originated. The channel_id defaults to 1. The value must match the ID of a valid and enabled channel.",
            },
            {"TABLE_NAME": "orders", "COLUMN_NAME": "customer_id", "DATA_TYPE": "INT", "COLUMN_DESCRIPTION": ""},
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "customer_message",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Message that the customer entered to the Order Comments box during checkout.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "date_created",
                "DATA_TYPE": "DATETIME",
                "COLUMN_DESCRIPTION": "The date the order was created, formatted in the RFC-2822 standard. You set this attribute on Order creation (POST) to support the migration of historical orders. If you do not provide a value, then it will default to the current date/time. This date time is always in UTC in the api response.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "discount_amount",
                "DATA_TYPE": "DECIMAL",
                "COLUMN_DESCRIPTION": "Amount of discount for this transaction. The value can't be negative.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "ebay_order_id",
                "DATA_TYPE": "TEXT",
                "COLUMN_DESCRIPTION": "If the order was placed through eBay, the eBay order number will be included. Otherwise, the value will be 0.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "external_id",
                "DATA_TYPE": "INT",
                "COLUMN_DESCRIPTION": "The order ID in another system, such as the Amazon order ID if this is an Amazon order.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "external_merchant_id",
                "DATA_TYPE": "INT",
                "COLUMN_DESCRIPTION": "The merchant ID represents an upstream order from an external system. It is the source of truth for orders. After setting it, you cannot write to or update the external_merchant_id.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "external_source",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "This value identifies an external system that generated the order and submitted it to BigCommerce with the Orders API.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "geoip_country",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "The full name of the country where the customer made the purchase, based on the IP.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "geoip_country_iso2",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "The country where the customer made the purchase, in ISO2 format, based on the IP.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "handling_cost_ex_tax",
                "DATA_TYPE": "DECIMAL",
                "COLUMN_DESCRIPTION": "The value of the handling cost, excluding tax. The value can't be negative.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "handling_cost_inc_tax",
                "DATA_TYPE": "DECIMAL",
                "COLUMN_DESCRIPTION": "The value of the handling cost, including tax. The value can't be negative.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "ip_address",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "IPv4 Address of the customer, if known.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "ip_address_v6",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "IPv6 Address of the customer, if known.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "items_shipped",
                "DATA_TYPE": "INT",
                "COLUMN_DESCRIPTION": "The number of items that have been shipped.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "items_total",
                "DATA_TYPE": "INT",
                "COLUMN_DESCRIPTION": "The total number of items in the order.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "order_is_digital",
                "DATA_TYPE": "BOOL",
                "COLUMN_DESCRIPTION": "Whether this is an order for digital products.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "payment_method",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "The payment method for this order.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "payment_provider_id",
                "DATA_TYPE": "INT",
                "COLUMN_DESCRIPTION": "The external Transaction ID/Payment ID within this order's payment provider (if a payment provider was used).",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "refunded_amount",
                "DATA_TYPE": "DECIMAL",
                "COLUMN_DESCRIPTION": "The amount refunded from this transaction; always returns 0. The value can't be negative.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "shipping_cost_ex_tax",
                "DATA_TYPE": "DECIMAL",
                "COLUMN_DESCRIPTION": "The value of shipping cost, excluding tax. The value can't be negative.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "shipping_cost_inc_tax",
                "DATA_TYPE": "DECIMAL",
                "COLUMN_DESCRIPTION": "The value of shipping cost, including tax. The value can't be negative.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "staff_notes",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Any additional notes for staff.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "subtotal_ex_tax",
                "DATA_TYPE": "DECIMAL",
                "COLUMN_DESCRIPTION": "Override value for subtotal excluding tax. The value can't be negative. If specified, the field subtotal_inc_tax is also required.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "subtotal_inc_tax",
                "DATA_TYPE": "DECIMAL",
                "COLUMN_DESCRIPTION": "Override value for subtotal including tax. The value can't be negative. If specified, the field subtotal_ex_tax is also required.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "tax_provider_id",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "BasicTaxProvider - Tax is set to manual and order is created in the store. AvaTaxProvider - Tax is set to automatic and order is created in the store. Empty string - The order is created with the API, or the tax provider is unknown.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "customer_locale",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "The customer's locale.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "external_order_id",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "The order ID in another system, such as the Amazon Order ID if this is an Amazon order. After setting it, you can update this field using a POST or PUT request.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "total_ex_tax",
                "DATA_TYPE": "DECIMAL",
                "COLUMN_DESCRIPTION": "Override value for the total, excluding tax. If specified, the field total_inc_tax is also required. The value can't be negative.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "total_inc_tax",
                "DATA_TYPE": "DECIMAL",
                "COLUMN_DESCRIPTION": "Override value for the total, including tax. If specified, the field total_ex_tax is also required. The value can't be negative.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "wrapping_cost_ex_tax",
                "DATA_TYPE": "DECIMAL",
                "COLUMN_DESCRIPTION": "The value of the wrapping cost, excluding tax. The value can't be negative.",
            },
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "wrapping_cost_inc_tax",
                "DATA_TYPE": "DECIMAL",
                "COLUMN_DESCRIPTION": "The value of the wrapping cost, including tax. The value can't be negative.",
            },
            # These fields are not mentioned in the API documentation, but they are present in the actual response.
            {"TABLE_NAME": "orders", "COLUMN_NAME": "status_id", "DATA_TYPE": "INT", "COLUMN_DESCRIPTION": ""},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "fees", "DATA_TYPE": "JSON", "COLUMN_DESCRIPTION": ""},
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "credit_card_type",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "",
            },
            {"TABLE_NAME": "orders", "COLUMN_NAME": "custom_status", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": ""},
        ]


class BigCommerceProductsTable(MetaAPIResource):
    """
    The table abstraction for the 'products' resource of the BigCommerce API.
    """

    name = "products"

    def list(
        self,
        conditions: list[FilterCondition] = None,
        limit: int = None,
        sort: list[SortColumn] = None,
        targets: list[str] = None,
        **kwargs,
    ):
        """
        Executes a parsed SELECT SQL query on the 'products' resource of the BigCommerce API.

        Args:
            conditions (list[FilterCondition]): The list of parsed filter conditions.
            limit (int): The maximum number of records to return.
            sort (list[SortColumn]): The list of parsed sort columns.
            targets (list[str]): The list of target columns to return.

        Returns:
            pd.DataFrame: The resulting DataFrame.
        """
        client: BigCommerceAPIClient = self.handler.connect()

        simple_op_map = {
            ("id", FilterOperator.EQUAL): "id",
            ("id", FilterOperator.IN): "id:in",
            ("id", FilterOperator.NOT_IN): "id:not_in",
            ("id", FilterOperator.GREATER_THAN): "id:greater",
            ("id", FilterOperator.LESS_THAN): "id:less",
            ("id", FilterOperator.GREATER_THAN_OR_EQUAL): "id:min",
            ("id", FilterOperator.LESS_THAN_OR_EQUAL): "id:max",
            ("channel_id", FilterOperator.IN): "channel_id:in",
            ("categories", FilterOperator.EQUAL): "categories",
            ("categories", FilterOperator.IN): "categories:in",
            ("name", FilterOperator.EQUAL): "name",
            ("mpn", FilterOperator.EQUAL): "mpn",
            ("upc", FilterOperator.EQUAL): "upc",
            ("price", FilterOperator.EQUAL): "price",
            ("weight", FilterOperator.EQUAL): "weight",
            ("condition", FilterOperator.EQUAL): "condition",
            ("brand_id", FilterOperator.EQUAL): "brand_id",
            ("date_modified", FilterOperator.EQUAL): "date_modified",
            ("date_modified", FilterOperator.LESS_THAN_OR_EQUAL): "date_modified:max",
            ("date_modified", FilterOperator.GREATER_THAN_OR_EQUAL): "date_modified:min",
            ("date_last_imported", FilterOperator.EQUAL): "date_last_imported",
            ("date_last_imported", FilterOperator.NOT_EQUAL): "date_last_imported:not",
            ("date_last_imported", FilterOperator.LESS_THAN_OR_EQUAL): "date_last_imported:max",
            ("date_last_imported", FilterOperator.GREATER_THAN_OR_EQUAL): "date_last_imported:min",
            ("is_visible", FilterOperator.EQUAL): "is_visible",
            ("is_featured", FilterOperator.EQUAL): "is_featured",
            ("is_free_shipping", FilterOperator.EQUAL): "is_free_shipping",
            ("inventory_level", FilterOperator.EQUAL): "inventory_level",
            ("inventory_level", FilterOperator.IN): "inventory_level:in",
            ("inventory_level", FilterOperator.NOT_IN): "inventory_level:not_in",
            ("inventory_level", FilterOperator.GREATER_THAN_OR_EQUAL): "inventory_level:min",
            ("inventory_level", FilterOperator.LESS_THAN_OR_EQUAL): "inventory_level:max",
            ("inventory_level", FilterOperator.GREATER_THAN): "inventory_level:greater",
            ("inventory_level", FilterOperator.LESS_THAN): "inventory_level:less",
            ("inventory_low", FilterOperator.EQUAL): "inventory_low",
            ("out_of_stock", FilterOperator.EQUAL): "out_of_stock",
            ("total_sold", FilterOperator.EQUAL): "total_sold",
            ("type", FilterOperator.EQUAL): "type",
            ("keyword", FilterOperator.EQUAL): "keyword",
            ("keyword_context", FilterOperator.EQUAL): "keyword_context",
            ("availability", FilterOperator.EQUAL): "availability",
            ("sku", FilterOperator.EQUAL): "sku",
            ("sku", FilterOperator.IN): "sku:in",
        }

        filter = {}
        for condition in conditions:
            simple_op = simple_op_map.get((condition.column, condition.op))
            if simple_op:
                value = condition.value
                if isinstance(value, list):
                    value = ",".join(map(str, value))
                filter[simple_op] = value
                condition.applied = True

        filter = _make_filter(conditions, simple_op_map)

        if targets:
            available_columns = self.get_columns()
            for column_name in targets:
                if column_name not in available_columns:
                    raise ValueError(f"Field '{column_name}' does not exists")
            filter["include_fields"] = ",".join(targets)

        sortable_columns = [
            "id",
            "name",
            "sku",
            "price",
            "date_modified",
            "date_last_imported",
            "inventory_level",
            "is_visible",
            "total_sold",
            "calculated_price",
        ]
        sort_condition = _make_sort_condition_v3(sort, sortable_columns)

        result = client.get_products(
            filter=filter,
            sort_condition=sort_condition,
            limit=limit,
        )
        result = _make_df(result, self)

        return result

    def get_columns(self) -> List[str]:
        """Retrieves the columns names of the 'products' resource.

        Returns:
            list[str]: A list of columns names of the 'products' resource.
        """
        columns = self.meta_get_columns()
        return [column["COLUMN_NAME"] for column in columns]

    def meta_get_tables(self, *args, **kwargs) -> dict:
        client: BigCommerceAPIClient = self.handler.connect()
        products_count = client.get_products_count()
        return {
            "table_name": self.name,
            "table_type": "BASE TABLE",
            "table_description": "",
            "row_count": products_count,
        }

    def meta_get_columns(self, *args, **kwargs) -> List[str]:
        return [
            {"TABLE_NAME": "products", "COLUMN_NAME": "id", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "name", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "type", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "sku", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "description", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "weight", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "width", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "depth", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "height", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "price", "DATA_TYPE": "DECIMAL"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "cost_price", "DATA_TYPE": "DECIMAL"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "retail_price", "DATA_TYPE": "DECIMAL"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "sale_price", "DATA_TYPE": "DECIMAL"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "map_price", "DATA_TYPE": "DECIMAL"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "tax_class_id", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "product_tax_code", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "calculated_price", "DATA_TYPE": "DECIMAL"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "categories", "DATA_TYPE": "JSON"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "brand_id", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "option_set_id", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "option_set_display", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "inventory_level", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "inventory_warning_level", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "inventory_tracking", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "reviews_rating_sum", "DATA_TYPE": "DECIMAL"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "reviews_count", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "total_sold", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "fixed_cost_shipping_price", "DATA_TYPE": "DECIMAL"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "is_free_shipping", "DATA_TYPE": "BOOL"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "is_visible", "DATA_TYPE": "BOOL"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "is_featured", "DATA_TYPE": "BOOL"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "related_products", "DATA_TYPE": "JSON"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "warranty", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "bin_picking_number", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "layout_file", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "upc", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "mpn", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "gtin", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "date_last_imported", "DATA_TYPE": "DATETIME"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "search_keywords", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "availability", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "availability_description", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "gift_wrapping_options_type", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "gift_wrapping_options_list", "DATA_TYPE": "JSON"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "sort_order", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "condition", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "is_condition_shown", "DATA_TYPE": "BOOL"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "order_quantity_minimum", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "order_quantity_maximum", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "page_title", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "meta_keywords", "DATA_TYPE": "JSON"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "meta_description", "DATA_TYPE": "VARTEXTCHAR"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "date_created", "DATA_TYPE": "DATETIME"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "date_modified", "DATA_TYPE": "DATETIME"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "view_count", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "preorder_release_date", "DATA_TYPE": "DATETIME"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "preorder_message", "DATA_TYPE": "DECIMAL"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "is_preorder_only", "DATA_TYPE": "BOOL"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "is_price_hidden", "DATA_TYPE": "BOOL"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "price_hidden_label", "DATA_TYPE": "DECIMAL"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "custom_url", "DATA_TYPE": "JSON"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "base_variant_id", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "open_graph_type", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "open_graph_title", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "open_graph_description", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "open_graph_use_meta_description", "DATA_TYPE": "BOOL"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "open_graph_use_product_name", "DATA_TYPE": "BOOL"},
            {"TABLE_NAME": "products", "COLUMN_NAME": "open_graph_use_image", "DATA_TYPE": "BOOL"},
        ]


class BigCommerceCustomersTable(MetaAPIResource):
    """
    The table abstraction for the 'customers' resource of the BigCommerce API.
    """

    name = "customers"

    def list(
        self,
        conditions: list[FilterCondition] = None,
        limit: int = None,
        sort: list[SortColumn] = None,
        **kwargs,
    ):
        """
        Executes a parsed SELECT SQL query on the 'customers' resource of the BigCommerce API.

        Args:
            conditions (list[FilterCondition]): The list of parsed filter conditions.
            limit (int): The maximum number of records to return.
            sort (list[SortColumn]): The list of parsed sort columns.

        Returns:
            pd.DataFrame: The resulting DataFrame.
        """
        # doc: https://developer.bigcommerce.com/docs/rest-management/customers
        client: BigCommerceAPIClient = self.handler.connect()

        simple_op_map = {
            ("id", FilterOperator.EQUAL): "id:in",  # custom filter
            ("id", FilterOperator.IN): "id:in",
            ("company", FilterOperator.EQUAL): "company:in",  # custom filter
            ("company", FilterOperator.IN): "company:in",
            ("customer_group_id", FilterOperator.EQUAL): "customer_group_id:in",  # custom filter
            ("customer_group_id", FilterOperator.IN): "customer_group_id:in",
            ("date_created", FilterOperator.EQUAL): "date_created",
            ("date_created", FilterOperator.LESS_THAN): "date_created:max",
            ("date_created", FilterOperator.GREATER_THAN): "date_created:min",
            ("date_modified", FilterOperator.EQUAL): "date_modified",
            ("date_modified", FilterOperator.LESS_THAN): "date_modified:max",
            ("date_modified", FilterOperator.GREATER_THAN): "date_modified:min",
            ("email", FilterOperator.EQUAL): "email:in",  # custom filter
            ("email", FilterOperator.IN): "email:in",
            ("name", FilterOperator.IN): "name:in",
            ("name", FilterOperator.LIKE): "name:like",
            ("phone", FilterOperator.EQUAL): "phone:in",  # custom filter
            ("phone", FilterOperator.IN): "phone:in",
            ("registration_ip_address", FilterOperator.EQUAL): "registration_ip_address:in",  # custom filter
            ("registration_ip_address", FilterOperator.IN): "registration_ip_address:in",
        }

        filter = _make_filter(conditions, simple_op_map)

        sortable_columns = ["date_created", "last_name", "date_modified"]
        sort_condition = _make_sort_condition_v2(sort, sortable_columns)

        result = client.get_customers(
            filter=filter,
            sort_condition=sort_condition,
            limit=limit,
        )
        result = _make_df(result, self)

        # 'name' is added to use server-side filtering
        result["name"] = result["first_name"] + " " + result["last_name"]

        return result

    def get_columns(self) -> List[str]:
        """Retrieves the columns names of the 'customers' resource.

        Returns:
            list[str]: A list of columns names of the 'customers' resource.
        """
        columns = self.meta_get_columns()
        return [column["COLUMN_NAME"] for column in columns]

    def meta_get_tables(self, table_name: str) -> dict:
        client: BigCommerceAPIClient = self.handler.connect()
        customers_count = client.get_customers_count()
        return {
            "table_name": self.name,
            "table_type": "BASE TABLE",
            "table_description": "",
            "row_count": customers_count,
        }

    def meta_get_columns(self, *args, **kwargs) -> List[str]:
        return [
            {"TABLE_NAME": "customers", "COLUMN_NAME": "id", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "customers", "COLUMN_NAME": "authentication", "DATA_TYPE": "JSON"},
            {"TABLE_NAME": "customers", "COLUMN_NAME": "company", "DATA_TYPE": "VARCHAR"},
            {"TABLE_NAME": "customers", "COLUMN_NAME": "customer_group_id", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "customers", "COLUMN_NAME": "email", "DATA_TYPE": "VARCHAR"},
            {"TABLE_NAME": "customers", "COLUMN_NAME": "first_name", "DATA_TYPE": "VARCHAR"},
            {"TABLE_NAME": "customers", "COLUMN_NAME": "last_name", "DATA_TYPE": "VARCHAR"},
            # 'name' is added to use server-side filtering: first_name + last_name
            {"TABLE_NAME": "customers", "COLUMN_NAME": "name", "DATA_TYPE": "VARCHAR"},
            {"TABLE_NAME": "customers", "COLUMN_NAME": "notes", "DATA_TYPE": "VARCHAR"},
            {"TABLE_NAME": "customers", "COLUMN_NAME": "phone", "DATA_TYPE": "VARCHAR"},
            {"TABLE_NAME": "customers", "COLUMN_NAME": "registration_ip_address", "DATA_TYPE": "VARCHAR"},
            {"TABLE_NAME": "customers", "COLUMN_NAME": "tax_exempt_category", "DATA_TYPE": "VARCHAR"},
            {"TABLE_NAME": "customers", "COLUMN_NAME": "date_created", "DATA_TYPE": "DATETIME"},
            {"TABLE_NAME": "customers", "COLUMN_NAME": "date_modified", "DATA_TYPE": "DATETIME"},
            {
                "TABLE_NAME": "customers",
                "COLUMN_NAME": "accepts_product_review_abandoned_cart_emails",
                "DATA_TYPE": "BOOL",
            },
            {"TABLE_NAME": "customers", "COLUMN_NAME": "origin_channel_id", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "customers", "COLUMN_NAME": "channel_ids", "DATA_TYPE": "JSON"},
        ]


class BigCommerceCategoriesTable(MetaAPIResource):
    """
    The table abstraction for the 'categories' resource of the BigCommerce API.
    """

    name = "categories"

    def list(
        self,
        conditions: list[FilterCondition] = None,
        limit: int = None,
        targets: list[str] = None,
        **kwargs,
    ):
        """Executes a parsed SELECT SQL query on the 'categories' resource of the BigCommerce API.

        Args:
            conditions (list[FilterCondition]): The list of parsed filter conditions.
            limit (int): The maximum number of records to return.
            targets (list[str]): The list of target columns to return.

        Returns:
            pd.DataFrame: The resulting DataFrame.
        """
        # doc: https://developer.bigcommerce.com/docs/rest-catalog/category-trees/categories#get-all-categories
        client: BigCommerceAPIClient = self.handler.connect()

        simple_op_map = {
            ("category_id", FilterOperator.EQUAL): "category_id:in",  # custom filter
            ("category_id", FilterOperator.IN): "category_id:in",
            ("category_id", FilterOperator.NOT_IN): "category_id:not_in",
            ("tree_id", FilterOperator.EQUAL): "tree_id:in",  # custom filter
            ("tree_id", FilterOperator.IN): "tree_id:in",
            ("tree_id", FilterOperator.NOT_IN): "tree_id:not_in",
            ("parent_id", FilterOperator.EQUAL): "parent_id:in",  # custom filter
            ("parent_id", FilterOperator.IN): "parent_id:in",
            ("parent_id", FilterOperator.NOT_IN): "parent_id:not_in",
            ("page_title", FilterOperator.EQUAL): "page_title",
            ("page_title", FilterOperator.LIKE): "page_title:like",
            ("name", FilterOperator.EQUAL): "name",
            ("name", FilterOperator.LIKE): "name:like",
            ("keyword", FilterOperator.EQUAL): "keyword",
            ("is_visible", FilterOperator.EQUAL): "is_visible",
        }

        filter = _make_filter(conditions, simple_op_map)

        if targets:
            available_columns = self.get_columns()
            for column_name in targets:
                if column_name not in available_columns:
                    raise ValueError(f"Field '{column_name}' does not exists")
            filter["include_fields"] = ",".join(targets)

        result = client.get_categories(
            filter=filter,
            limit=limit,
        )
        result = _make_df(result, self)

        return result

    def get_columns(self) -> List[str]:
        """Retrieves the columns names of the 'categories' resource.

        Returns:
            list[str]: A list of columns names of the 'categories' resource.
        """
        columns = self.meta_get_columns()
        return [column["COLUMN_NAME"] for column in columns]

    def meta_get_tables(self, *args, **kwargs) -> dict:
        client: BigCommerceAPIClient = self.handler.connect()
        categories_count = client.get_categories_count()
        return {
            "table_name": self.name,
            "table_type": "BASE TABLE",
            "table_description": "",
            "row_count": categories_count,
        }

    def meta_get_columns(self, *args, **kwargs) -> List[str]:
        return [
            {"TABLE_NAME": "categories", "COLUMN_NAME": "category_id", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "categories", "COLUMN_NAME": "parent_id", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "categories", "COLUMN_NAME": "tree_id", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "categories", "COLUMN_NAME": "name", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "categories", "COLUMN_NAME": "description", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "categories", "COLUMN_NAME": "views", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "categories", "COLUMN_NAME": "sort_order", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "categories", "COLUMN_NAME": "page_title", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "categories", "COLUMN_NAME": "search_keywords", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "categories", "COLUMN_NAME": "meta_keywords", "DATA_TYPE": "JSON"},
            {"TABLE_NAME": "categories", "COLUMN_NAME": "meta_description", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "categories", "COLUMN_NAME": "layout_file", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "categories", "COLUMN_NAME": "is_visible", "DATA_TYPE": "BOOL"},
            {"TABLE_NAME": "categories", "COLUMN_NAME": "default_product_sort", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "categories", "COLUMN_NAME": "url", "DATA_TYPE": "JSON"},
            {"TABLE_NAME": "categories", "COLUMN_NAME": "image_url", "DATA_TYPE": "VARCHAR"},
        ]


class BigCommercePickupsTable(MetaAPIResource):
    """
    The table abstraction for the 'pickups' resource of the BigCommerce API.
    """

    name = "pickups"

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        **kwargs,
    ):
        """Executes a parsed SELECT SQL query on the 'pickups' resource of the BigCommerce API.

        Args:
            conditions (List[FilterCondition]): The list of parsed filter conditions.
            limit (int): The maximum number of records to return.

        Returns:
            pd.DataFrame: The resulting DataFrame.
        """
        client: BigCommerceAPIClient = self.handler.connect()

        simple_op_map = {
            ("order_id", FilterOperator.EQUAL): "order_id:in",  # custom filter
            ("order_id", FilterOperator.IN): "order_id:in",
            ("pickup_id", FilterOperator.EQUAL): "pickup_id:in",  # custom filter
            ("pickup_id", FilterOperator.IN): "pickup_id:in",
        }

        filter = _make_filter(conditions, simple_op_map)

        result = client.get_pickups(
            filter=filter,
            limit=limit,
        )
        result = _make_df(result, self)

        return result

    def get_columns(self) -> List[str]:
        """Retrieves the columns names of the 'pickups' resource.

        Returns:
            list[str]: A list of columns names of the 'pickups' resource.
        """
        columns = self.meta_get_columns()
        return [column["COLUMN_NAME"] for column in columns]

    def meta_get_tables(self, *args, **kwargs) -> dict:
        client: BigCommerceAPIClient = self.handler.connect()
        pickups_count = client.get_pickups_count()
        return {
            "table_name": self.name,
            "table_type": "BASE TABLE",
            "table_description": "",
            "row_count": pickups_count,
        }

    def meta_get_columns(self, *args, **kwargs):
        return [
            {"TABLE_NAME": "pickups", "COLUMN_NAME": "id", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "pickups", "COLUMN_NAME": "pickup_method_id", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "pickups", "COLUMN_NAME": "order_id", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "pickups", "COLUMN_NAME": "ready_at", "DATA_TYPE": "DATETIME"},
            {"TABLE_NAME": "pickups", "COLUMN_NAME": "created_at", "DATA_TYPE": "DATETIME"},
            {"TABLE_NAME": "pickups", "COLUMN_NAME": "updated_at", "DATA_TYPE": "DATETIME"},
            {"TABLE_NAME": "pickups", "COLUMN_NAME": "pickup_items", "DATA_TYPE": "JSON"},
        ]


class BigCommercePromotionsTable(MetaAPIResource):
    """
    The table abstraction for the 'promotions' resource of the BigCommerce API.
    """

    name = "promotions"

    def list(
        self,
        conditions: list[FilterCondition] = None,
        limit: int = None,
        sort: list[SortColumn] = None,
        targets: list[str] = None,
        **kwargs,
    ):
        """Executes a parsed SELECT SQL query on the 'promotions' resource of the BigCommerce API.

        Args:
            conditions (list[FilterCondition]): The list of parsed filter conditions.
            limit (int): The maximum number of records to return.
            sort (list[SortColumn]): The list of parsed sort columns.
            targets (list[str]): The list of target columns to return.

        Returns:
            pd.DataFrame: The resulting DataFrame.
        """
        client: BigCommerceAPIClient = self.handler.connect()

        simple_op_map = {
            ("id", FilterOperator.EQUAL): "id",
            ("name", FilterOperator.EQUAL): "name",
            ("currency_code", FilterOperator.EQUAL): "currency_code",
            ("redemption_type", FilterOperator.EQUAL): "redemption_type",
            ("status", FilterOperator.EQUAL): "status",
            ("channels", FilterOperator.EQUAL): "channels",  # custom filter
            ("channels", FilterOperator.IN): "channels",
        }

        filter = _make_filter(conditions, simple_op_map)

        sortable_columns = ["id", "name", "start_date", "priority"]
        sort_condition = _make_sort_condition_v3(sort, sortable_columns)

        result = client.get_promotions(
            filter=filter,
            sort_condition=sort_condition,
            limit=limit,
        )
        result = _make_df(result, self)

        return result

    def get_columns(self) -> List[str]:
        """Retrieves the columns names of the 'promotions' resource.

        Returns:
            list[str]: A list of columns names of the 'promotions' resource.
        """
        columns = self.meta_get_columns()
        return [column["COLUMN_NAME"] for column in columns]

    def meta_get_tables(self, *args, **kwargs) -> dict:
        client: BigCommerceAPIClient = self.handler.connect()
        promotions_count = client.get_promotions_count()
        return {
            "table_name": self.name,
            "table_type": "BASE TABLE",
            "table_description": "",
            "row_count": promotions_count,
        }

    def meta_get_columns(self, *args, **kwargs) -> List[str]:
        return [
            {"TABLE_NAME": "promotions", "COLUMN_NAME": "id", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "promotions", "COLUMN_NAME": "redemption_type", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "promotions", "COLUMN_NAME": "name", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "promotions", "COLUMN_NAME": "display_name", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "promotions", "COLUMN_NAME": "channels", "DATA_TYPE": "JSON"},
            {"TABLE_NAME": "promotions", "COLUMN_NAME": "customer", "DATA_TYPE": "JSON"},
            {"TABLE_NAME": "promotions", "COLUMN_NAME": "rules", "DATA_TYPE": "JSON"},
            {"TABLE_NAME": "promotions", "COLUMN_NAME": "current_uses", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "promotions", "COLUMN_NAME": "max_uses", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "promotions", "COLUMN_NAME": "status", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "promotions", "COLUMN_NAME": "start_date", "DATA_TYPE": "DATETIME"},
            {"TABLE_NAME": "promotions", "COLUMN_NAME": "end_date", "DATA_TYPE": "DATETIME"},
            {"TABLE_NAME": "promotions", "COLUMN_NAME": "stop", "DATA_TYPE": "BOOL"},
            {"TABLE_NAME": "promotions", "COLUMN_NAME": "can_be_used_with_other_promotions", "DATA_TYPE": "BOOL"},
            {"TABLE_NAME": "promotions", "COLUMN_NAME": "currency_code", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "promotions", "COLUMN_NAME": "notifications", "DATA_TYPE": "JSON"},
            {"TABLE_NAME": "promotions", "COLUMN_NAME": "shipping_address", "DATA_TYPE": "JSON"},
            {"TABLE_NAME": "promotions", "COLUMN_NAME": "schedule", "DATA_TYPE": "JSON"},
            {"TABLE_NAME": "promotions", "COLUMN_NAME": "created_from", "DATA_TYPE": "TEXT"},
        ]


class BigCommerceWishlistsTable(MetaAPIResource):
    """
    The table abstraction for the 'wishlists' resource of the BigCommerce API.
    """

    name = "wishlists"

    def list(
        self,
        conditions: list[FilterCondition] = None,
        limit: int = None,
        **kwargs,
    ):
        """Executes a parsed SELECT SQL query on the 'wishlists' resource of the BigCommerce API.

        Args:
            conditions (list[FilterCondition]): The list of parsed filter conditions.
            limit (int): The maximum number of records to return.

        Returns:
            pd.DataFrame: The resulting DataFrame.
        """
        client: BigCommerceAPIClient = self.handler.connect()

        simple_op_map = {
            ("customer_id", FilterOperator.IN): "customer_id:in",
        }

        filter = _make_filter(conditions, simple_op_map)

        result = client.get_wishlists(
            filter=filter,
            limit=limit,
        )
        result = _make_df(result, self)

        return result

    def get_columns(self) -> List[str]:
        """Retrieves the columns names of the 'wishlists' resource.

        Returns:
            list[str]: A list of columns names of the 'wishlists' resource.
        """
        columns = self.meta_get_columns()
        return [column["COLUMN_NAME"] for column in columns]

    def meta_get_tables(self, *args, **kwargs) -> dict:
        client: BigCommerceAPIClient = self.handler.connect()
        wishlists_count = client.get_wishlists_count()
        return {
            "table_name": self.name,
            "table_type": "BASE TABLE",
            "table_description": "",
            "row_count": wishlists_count,
        }

    def meta_get_columns(self, *args, **kwargs) -> List[str]:
        return [
            {"TABLE_NAME": "wishlists", "COLUMN_NAME": "id", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "wishlists", "COLUMN_NAME": "customer_id", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "wishlists", "COLUMN_NAME": "name", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "wishlists", "COLUMN_NAME": "is_public", "DATA_TYPE": "BOOL"},
            {"TABLE_NAME": "wishlists", "COLUMN_NAME": "token", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "wishlists", "COLUMN_NAME": "items", "DATA_TYPE": "JSON"},
        ]


class BigCommerceSegmentsTable(MetaAPIResource):
    """
    The table abstraction for the 'segments' (customer segmentation) resource of the BigCommerce API.
    """

    name = "segments"

    def list(
        self,
        conditions: list[FilterCondition] = None,
        limit: int = None,
        **kwargs,
    ):
        """Executes a parsed SELECT SQL query on the 'segments' resource of the BigCommerce API.

        Args:
            conditions (list[FilterCondition]): The list of parsed filter conditions.
            limit (int): The maximum number of records to return.

        Returns:
            pd.DataFrame: The resulting DataFrame.
        """
        client: BigCommerceAPIClient = self.handler.connect()

        simple_op_map = {
            ("id", FilterOperator.IN): "id:in",
        }

        filter = _make_filter(conditions, simple_op_map)

        result = client.get_segments(
            filter=filter,
            limit=limit,
        )
        result = _make_df(result, self)

        return result

    def get_columns(self) -> List[str]:
        """Retrieves the columns names of the 'segments' resource.

        Returns:
            list[str]: A list of columns names of the 'segments' resource.
        """
        columns = self.meta_get_columns()
        return [column["COLUMN_NAME"] for column in columns]

    def meta_get_tables(self, *args, **kwargs) -> dict:
        client: BigCommerceAPIClient = self.handler.connect()
        segments_count = client.get_segments_count()
        return {
            "table_name": self.name,
            "table_type": "BASE TABLE",
            "table_description": "",
            "row_count": segments_count,
        }

    def meta_get_columns(self, *args, **kwargs) -> List[str]:
        return [
            {"TABLE_NAME": "segments", "COLUMN_NAME": "id", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "segments", "COLUMN_NAME": "name", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "segments", "COLUMN_NAME": "description", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "segments", "COLUMN_NAME": "created_at", "DATA_TYPE": "DATETIME"},
            {"TABLE_NAME": "segments", "COLUMN_NAME": "updated_at", "DATA_TYPE": "DATETIME"},
        ]


class BigCommerceBrandsTable(MetaAPIResource):
    """
    The table abstraction for the 'brands' resource of the BigCommerce API.
    """

    name = "brands"

    def list(
        self,
        conditions: list[FilterCondition] = None,
        limit: int = None,
        sort: list[SortColumn] = None,
        targets: list[str] = None,
        **kwargs,
    ):
        """Executes a parsed SELECT SQL query on the 'brands' resource of the BigCommerce API.

        Args:
            conditions (list[FilterCondition]): The list of parsed filter conditions.
            limit (int): The maximum number of records to return.
            sort (list[SortColumn]): The list of parsed sort columns.
            targets (list[str]): The list of target columns to return.

        Returns:
            pd.DataFrame: The resulting DataFrame.
        """
        client: BigCommerceAPIClient = self.handler.connect()

        simple_op_map = {
            ("id", FilterOperator.EQUAL): "id",
            ("id", FilterOperator.IN): "id:in",
            ("id", FilterOperator.NOT_IN): "id:not_in",
            ("id", FilterOperator.GREATER_THAN): "id:greater",
            ("id", FilterOperator.LESS_THAN): "id:less",
            ("id", FilterOperator.GREATER_THAN_OR_EQUAL): "id:min",
            ("id", FilterOperator.LESS_THAN_OR_EQUAL): "id:max",
            ("name", FilterOperator.EQUAL): "name",
            ("name", FilterOperator.LIKE): "name:like",
            ("page_title", FilterOperator.EQUAL): "page_title",
        }

        filter = _make_filter(conditions, simple_op_map)

        if targets:
            available_columns = self.get_columns()
            for column_name in targets:
                if column_name not in available_columns:
                    raise ValueError(f"Field '{column_name}' does not exists")
            filter["include_fields"] = ",".join(targets)

        sortable_columns = ["name"]
        sort_condition = _make_sort_condition_v3(sort, sortable_columns)

        result = client.get_brands(
            filter=filter,
            sort_condition=sort_condition,
            limit=limit,
        )
        result = _make_df(result, self)

        return result

    def get_columns(self) -> List[str]:
        """Retrieves the columns names of the 'brands' resource.

        Returns:
            list[str]: A list of columns names of the 'brands' resource.
        """
        columns = self.meta_get_columns()
        return [column["COLUMN_NAME"] for column in columns]

    def meta_get_tables(self, *args, **kwargs) -> dict:
        client: BigCommerceAPIClient = self.handler.connect()
        brands_count = client.get_brands_count()
        return {
            "table_name": self.name,
            "table_type": "BASE TABLE",
            "table_description": "",
            "row_count": brands_count,
        }

    def meta_get_columns(self, *args, **kwargs) -> List[str]:
        return [
            {"TABLE_NAME": "brands", "COLUMN_NAME": "id", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "brands", "COLUMN_NAME": "name", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "brands", "COLUMN_NAME": "page_title", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "brands", "COLUMN_NAME": "meta_keywords", "DATA_TYPE": "JSON"},
            {"TABLE_NAME": "brands", "COLUMN_NAME": "meta_description", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "brands", "COLUMN_NAME": "search_keywords", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "brands", "COLUMN_NAME": "image_url", "DATA_TYPE": "TEXT"},
            {"TABLE_NAME": "brands", "COLUMN_NAME": "custom_url", "DATA_TYPE": "JSON"},
        ]
