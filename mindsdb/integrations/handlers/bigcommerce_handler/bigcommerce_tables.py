from typing import List
from decimal import Decimal

import pandas as pd

from mindsdb.integrations.handlers.bigcommerce_handler.bigcommerce_api_client import BigCommerceAPIClient
from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator, SortColumn
from mindsdb.integrations.libs.api_handler import MetaAPIResource
from mindsdb.utilities import log


logger = log.getLogger(__name__)


class BigCommerceOrdersTable(MetaAPIResource):
    """
    The table abstraction for the 'orders' resource of the BigCommerce API.
    """

    name = "orders"

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        **kwargs,
    ):
        """
        Args:
            conditions (List[FilterCondition]): The list of parsed filter conditions.
            limit (int): The maximum number of records to return.
            sort (List[SortColumn]): The list of parsed sort columns.
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

        filter = {}
        for condition in conditions:
            simple_op = simple_op_map.get((condition.column, condition.op))
            if simple_op:
                value = condition.value
                if isinstance(value, list):
                    value = ",".join(map(str, value))
                filter[simple_op] = value
                condition.applied = True

        sort_condition = None
        if sort:
            if len(sort) > 1:
                raise ValueError("Only one column may by used for order by")
            sort_column = sort[0]
            if sort_column.column not in [
                "id",
                "customer_id",
                "date_created",
                "date_modified",
                "status_id",
                "channel_id",
                "external_id",
            ]:
                raise ValueError(f"Unsupported sort column: {sort_column.column}")
            sort_condition = {"sort": sort_column.column, "direction": "asc" if sort_column.ascending else "desc"}

        result = client.get_orders(filter=filter, sort_condition=sort_condition, limit=limit)
        decimal_columns = [
            "base_handling_cost",
            "base_shipping_cost",
            "base_wrapping_cost",
            "coupon_discount",
            "currency_exchange_rate",
            "discount_amount",
            "gift_certificate_amount",
            "handling_cost_ex_tax",
            "handling_cost_inc_tax",
            "handling_cost_tax",
            "refunded_amount",
            "shipping_cost_ex_tax",
            "shipping_cost_inc_tax",
            "shipping_cost_tax",
            "store_credit_amount",
            "store_default_to_transactional_exchange_rate",
            "subtotal_ex_tax",
            "subtotal_inc_tax",
            "subtotal_tax",
            "total_ex_tax",
            "total_inc_tax",
            "total_tax",
            "wrapping_cost_ex_tax",
            "wrapping_cost_inc_tax",
            "wrapping_cost_tax",
        ]
        if len(result) == 0:
            result = pd.DataFrame([], columns=self.get_columns())
        else:
            result = pd.DataFrame(result)
            for column_name in decimal_columns:
                if column_name in result:
                    result[column_name] = result[column_name].apply(Decimal)

        return result

    def get_columns(self) -> List[str]:
        """
        Retrieves the attributes (columns) of the 'orders' resource.

        Returns:
            List[str]: A list of attributes (columns) of the 'orders' resource.
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

    def meta_get_columns(self, *args, **kwargs):
        return [
            {"TABLE_NAME": "orders", "COLUMN_NAME": "id", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "customer_id", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "date_created", "DATA_TYPE": "DATETIME"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "is_tax_inclusive_pricing", "DATA_TYPE": "BOOL"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "date_modified", "DATA_TYPE": "DATETIME"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "date_shipped", "DATA_TYPE": "VARCHAR"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "status_id", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "status", "DATA_TYPE": "VARCHAR"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "subtotal_ex_tax", "DATA_TYPE": "DECIMAL"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "subtotal_inc_tax", "DATA_TYPE": "DECIMAL"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "subtotal_tax", "DATA_TYPE": "DECIMAL"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "base_shipping_cost", "DATA_TYPE": "DECIMAL"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "shipping_cost_ex_tax", "DATA_TYPE": "DECIMAL"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "shipping_cost_inc_tax", "DATA_TYPE": "DECIMAL"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "shipping_cost_tax", "DATA_TYPE": "DECIMAL"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "shipping_cost_tax_class_id", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "base_handling_cost", "DATA_TYPE": "DECIMAL"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "handling_cost_ex_tax", "DATA_TYPE": "DECIMAL"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "handling_cost_inc_tax", "DATA_TYPE": "DECIMAL"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "handling_cost_tax", "DATA_TYPE": "DECIMAL"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "handling_cost_tax_class_id", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "base_wrapping_cost", "DATA_TYPE": "DECIMAL"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "wrapping_cost_ex_tax", "DATA_TYPE": "DECIMAL"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "wrapping_cost_inc_tax", "DATA_TYPE": "DECIMAL"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "wrapping_cost_tax", "DATA_TYPE": "DECIMAL"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "wrapping_cost_tax_class_id", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "total_ex_tax", "DATA_TYPE": "DECIMAL"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "total_inc_tax", "DATA_TYPE": "DECIMAL"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "total_tax", "DATA_TYPE": "DECIMAL"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "items_total", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "items_shipped", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "payment_method", "DATA_TYPE": "VARCHAR"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "payment_provider_id", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "payment_status", "DATA_TYPE": "VARCHAR"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "refunded_amount", "DATA_TYPE": "DECIMAL"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "order_is_digital", "DATA_TYPE": "BOOL"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "store_credit_amount", "DATA_TYPE": "DECIMAL"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "gift_certificate_amount", "DATA_TYPE": "DECIMAL"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "ip_address", "DATA_TYPE": "VARCHAR"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "ip_address_v6", "DATA_TYPE": "VARCHAR"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "geoip_country", "DATA_TYPE": "VARCHAR"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "geoip_country_iso2", "DATA_TYPE": "VARCHAR"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "currency_id", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "currency_code", "DATA_TYPE": "VARCHAR"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "currency_exchange_rate", "DATA_TYPE": "DECIMAL"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "default_currency_id", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "default_currency_code", "DATA_TYPE": "VARCHAR"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "staff_notes", "DATA_TYPE": "VARCHAR"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "customer_message", "DATA_TYPE": "VARCHAR"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "discount_amount", "DATA_TYPE": "DECIMAL"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "coupon_discount", "DATA_TYPE": "DECIMAL"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "shipping_address_count", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "is_deleted", "DATA_TYPE": "BOOL"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "ebay_order_id", "DATA_TYPE": "DECIMAL"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "cart_id", "DATA_TYPE": "VARCHAR"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "billing_address", "DATA_TYPE": "JSON"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "fees", "DATA_TYPE": "JSON"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "is_email_opt_in", "DATA_TYPE": "BOOL"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "credit_card_type", "DATA_TYPE": "VARCHAR"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "order_source", "DATA_TYPE": "VARCHAR"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "channel_id", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "external_source", "DATA_TYPE": "VARCHAR"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "consignments", "DATA_TYPE": "JSON"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "products", "DATA_TYPE": "JSON"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "shipping_addresses", "DATA_TYPE": "JSON"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "coupons", "DATA_TYPE": "JSON"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "external_id", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "external_merchant_id", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "tax_provider_id", "DATA_TYPE": "VARCHAR"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "customer_locale", "DATA_TYPE": "VARCHAR"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "external_order_id", "DATA_TYPE": "VARCHAR"},
            {"TABLE_NAME": "orders", "COLUMN_NAME": "store_default_currency_code", "DATA_TYPE": "VARCHAR"},
            {
                "TABLE_NAME": "orders",
                "COLUMN_NAME": "store_default_to_transactional_exchange_rate",
                "DATA_TYPE": "DECIMAL",
            },
            {"TABLE_NAME": "orders", "COLUMN_NAME": "custom_status", "DATA_TYPE": "VARCHAR"},
        ]


class BigCommerceProductsTable(MetaAPIResource):
    """
    The table abstraction for the 'products' resource of the BigCommerce API.
    """

    name = "products"

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[str] = None,
        **kwargs,
    ):
        """
        Executes a parsed SELECT SQL query on the 'products' resource of the BigCommerce API.

        Args:
            conditions (List[FilterCondition]): The list of parsed filter conditions.
            limit (int): The maximum number of records to return.
            sort (List[SortColumn]): The list of parsed sort columns.
            targets (List[str]): The list of target columns to return.
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

        if targets:
            available_columns = self.get_columns()
            for column_name in targets:
                if column_name not in available_columns:
                    raise ValueError(f"Field '{column_name}' does not exists")
            filter["include_fields"] = ",".join(targets)

        sort_condition = None
        if sort:
            if len(sort) > 1:
                raise ValueError("Only one column may by used for order by")
            sort_column = sort[0]
            if sort_column.column not in [
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
            ]:
                raise ValueError(f"Unsupported sort column: {sort_column.column}")
            sort_condition = {"sort": sort_column.column, "direction": "asc" if sort_column.ascending else "desc"}

        result = client.get_products(
            filter=filter,
            sort_condition=sort_condition,
            limit=limit,
        )
        result = pd.DataFrame(result)

        return result

    def get_columns(self) -> List[str]:
        """
        Retrieves the attributes (columns) of the 'products' resource.

        Returns:
            List[Text]: A list of attributes (columns) of the 'products' resource.
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

    def meta_get_columns(self, *args, **kwargs):
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
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[str] = None,
        **kwargs,
    ):
        """
        Executes a parsed SELECT SQL query on the 'customers' resource of the BigCommerce API.

        Args:
            conditions (List[FilterCondition]): The list of parsed filter conditions.
            limit (int): The maximum number of records to return.
            sort (List[SortColumn]): The list of parsed sort columns.
            targets (List[str]): The list of target columns to return.
        """
        # doc: https://developer.bigcommerce.com/docs/rest-management/customers
        client: BigCommerceAPIClient = self.handler.connect()

        simple_op_map = {
            ("id", FilterOperator.IN): "id:in",
            ("company", FilterOperator.IN): "company:in",
            ("customer_group_id", FilterOperator.IN): "customer_group_id:in",
            ("date_created", FilterOperator.EQUAL): "date_created",
            ("date_created", FilterOperator.LESS_THAN): "date_created:max",
            ("date_created", FilterOperator.GREATER_THAN): "date_created:min",
            ("date_modified", FilterOperator.EQUAL): "date_modified",
            ("date_modified", FilterOperator.LESS_THAN): "date_modified:max",
            ("date_modified", FilterOperator.GREATER_THAN): "date_modified:min",
            ("email", FilterOperator.IN): "email:in",
            ("name", FilterOperator.IN): "name:in",
            ("name", FilterOperator.LIKE): "name:like",
            ("phone", FilterOperator.IN): "phone:in",
            ("registration_ip_address", FilterOperator.IN): "registration_ip_address:in",
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

        sort_condition = None
        if (
            isinstance(sort, list)
            and len(sort) == 0
            and sort[0].column in ["date_created", "last_name", "date_modified"]
        ):
            sort_column = sort[0]
            sort_column.applied = True
            sort_condition = f"{sort_column.column}:{'asc' if sort_column.ascending else 'desc'}"

        result = client.get_customers(
            filter=filter,
            sort_condition=sort_condition,
            limit=limit,
        )
        result = pd.DataFrame(result)

        return result

    def get_columns(self) -> List[str]:
        """
        Retrieves the attributes (columns) of the 'customers' resource.

        Returns:
            List[Text]: A list of attributes (columns) of the 'customers' resource.
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

    def meta_get_columns(self, *args, **kwargs):
        return [
            {"TABLE_NAME": "customers", "COLUMN_NAME": "id", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "customers", "COLUMN_NAME": "authentication", "DATA_TYPE": "JSON"},
            {"TABLE_NAME": "customers", "COLUMN_NAME": "company", "DATA_TYPE": "VARCHAR"},
            {"TABLE_NAME": "customers", "COLUMN_NAME": "customer_group_id", "DATA_TYPE": "INT"},
            {"TABLE_NAME": "customers", "COLUMN_NAME": "email", "DATA_TYPE": "VARCHAR"},
            {"TABLE_NAME": "customers", "COLUMN_NAME": "first_name", "DATA_TYPE": "VARCHAR"},
            {"TABLE_NAME": "customers", "COLUMN_NAME": "last_name", "DATA_TYPE": "VARCHAR"},
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
