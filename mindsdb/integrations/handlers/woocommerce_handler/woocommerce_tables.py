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


class ProductsTable(APITable):
    """The woocommerce Products Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /products" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify Products matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query, "products", self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = (
            select_statement_parser.parse_query()
        )

        all_products = list()
        val = None
        page_num = 1
        while val != 0:
            response = self.get_products(page=page_num, per_page=result_limit)
            all_products = all_products + response
            page_num = page_num + 1
            val = len(response)

        products_df = pd.json_normalize(all_products)

        select_statement_executor = SELECTQueryExecutor(
            products_df, selected_columns, where_conditions, order_by_conditions
        )
        products_df = select_statement_executor.execute_query()

        return products_df

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
            supported_columns=[
                "name",
                "slug",
                "type",
                "status",
                "featured",
                "catalog_visibility",
                "description",
                "short_description",
                "sku",
                "regular_price",
                "sale_price",
                "date_on_sale_from",
                "date_on_sale_from_gmt",
                "date_on_sale_to",
                "date_on_sale_to_gmt",
                "virtual",
                "downloadable",
                "downloads",
                "download_limit",
                "download_expiry",
                "external_url",
                "button_text",
                "tax_status",
                "tax_class",
                "manage_stock",
                "stock_quantity",
                "stock_status",
                "backorders",
                "sold_individually",
                "weight",
                "dimensions",
                "shipping_class",
                "reviews_allowed",
                "upsell_ids",
                "cross_sell_ids",
                "parent_id",
                "purchase_note",
                "categories",
                "tags",
                "images",
                "attributes",
                "default_attributes",
                "grouped_products",
                "menu_order",
                "meta_data",
            ],
            mandatory_columns=["name"],
            all_mandatory=False,
        )
        product_data = insert_statement_parser.parse_query()
        self.create_products(product_data)

    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from the Shopify "DELETE /products" API endpoint.

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

        products_df = pd.json_normalize(self.get_products())

        delete_query_executor = DELETEQueryExecutor(products_df, where_conditions)

        products_df = delete_query_executor.execute_query()

        product_ids = products_df["id"].tolist()
        self.delete_products(product_ids)

    def update(self, query: ast.Update) -> None:
        """Updates data from the Shopify "PUT /products" API endpoint.

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

        products_df = pd.json_normalize(self.get_products())
        update_query_executor = UPDATEQueryExecutor(products_df, where_conditions)

        products_df = update_query_executor.execute_query()
        product_ids = products_df["id"].tolist()
        self.update_products(product_ids, values_to_update)

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_products(per_page=1)).columns.tolist()

    def get_products(self, **kwargs) -> List[Dict]:
        wcapi = self.handler.connect()
        products = wcapi.get("products", params=kwargs)
        return [product for product in products.json()]

    def update_products(
        self, product_ids: List[int], values_to_update: Dict[Text, Any]
    ) -> None:
        wcapi = self.handler.connect()

        for product_id in product_ids:
            product = wcapi.get(f"products/{product_id}")
            for key, value in values_to_update.items():
                setattr(product, key, value)
            wcapi.put(f"products/{product_id}", product)
            logger.info(f"Product {product_id} updated")

    def delete_products(self, product_ids: List[int]) -> None:
        wcapi = self.handler.connect()
        for product_id in product_ids:
            wcapi.delete(f"products/{product_id}")
            logger.info(f"Product {product_id} deleted")

    def create_products(self, product_data: List[Dict[Text, Any]]) -> None:
        wcapi = self.handler.connect()

        for product in product_data:
            created_product = wcapi.post("products", product)
            if "id" not in created_product.json():
                raise Exception("Product creation failed")
            else:
                logger.info(f'Product {created_product.json()["id"]} created')


class CustomersTable(APITable):
    """The woocommerce customers Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /customers" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify customers matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query, "customers", self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = (
            select_statement_parser.parse_query()
        )

        all_customers = list()
        val = None
        page_num = 1
        while val != 0:
            response = self.get_customers(page=page_num, per_page=result_limit)
            all_customers = all_customers + response
            page_num = page_num + 1
            val = len(response)

        customers_df = pd.json_normalize(all_customers)

        select_statement_executor = SELECTQueryExecutor(
            customers_df, selected_columns, where_conditions, order_by_conditions
        )
        customers_df = select_statement_executor.execute_query()

        return customers_df

    def insert(self, query: ast.Insert) -> None:
        """Inserts data into the Shopify "POST /customers" API endpoint.

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
            supported_columns=[
                "email",
                "first_name",
                "last_name",
                "username",
                "password",
                "billing",
                "shipping",
                "meta_data",
            ],
            mandatory_columns=["email"],
            all_mandatory=False,
        )
        customer_data = insert_statement_parser.parse_query()
        self.create_customers(customer_data)

    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from the Shopify "DELETE /customers" API endpoint.

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

        customers_df = pd.json_normalize(self.get_customers())

        delete_query_executor = DELETEQueryExecutor(customers_df, where_conditions)

        customers_df = delete_query_executor.execute_query()

        customer_ids = customers_df["id"].tolist()
        self.delete_customers(customer_ids)

    def update(self, query: ast.Update) -> None:
        """Updates data from the Shopify "PUT /customers" API endpoint.

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

        customers_df = pd.json_normalize(self.get_customers())
        update_query_executor = UPDATEQueryExecutor(customers_df, where_conditions)

        customers_df = update_query_executor.execute_query()
        customer_ids = customers_df["id"].tolist()
        self.update_customers(customer_ids, values_to_update)

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_customers(per_page=1)).columns.tolist()

    def get_customers(self, **kwargs) -> List[Dict]:
        wcapi = self.handler.connect()
        customers = wcapi.get("customers", params=kwargs)
        return [customer for customer in customers.json()]

    def update_customers(
        self, customer_ids: List[int], values_to_update: Dict[Text, Any]
    ) -> None:
        wcapi = self.handler.connect()

        for customer_id in customer_ids:
            customer = wcapi.get(f"customers/{customer_id}")
            for key, value in values_to_update.items():
                setattr(customer, key, value)
            wcapi.put(f"customers/{customer_id}", customer)
            logger.info(f"customer {customer_id} updated")

    def delete_customers(self, customer_ids: List[int]) -> None:
        wcapi = self.handler.connect()
        for customer_id in customer_ids:
            response = wcapi.delete(f"customers/{customer_id}", params={"force": True})
            logger.info(f"customer {customer_id} deleted")
            if response.status_code != 200:
                raise Exception("customer deletion failed")

    def create_customers(self, customer_data: List[Dict[Text, Any]]) -> None:
        wcapi = self.handler.connect()

        for customer in customer_data:
            created_customer = wcapi.post("customers", customer)
            if "id" not in created_customer.json():
                raise Exception("customer creation failed")
            else:
                logger.info(f'customer {created_customer.json()["id"]} created')


class OrdersTable(APITable):
    """The woocommerce orders Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /orders" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify orders matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(query, "orders", self.get_columns())
        selected_columns, where_conditions, order_by_conditions, result_limit = (
            select_statement_parser.parse_query()
        )

        all_orders = list()
        val = None
        page_num = 1
        while val != 0:
            response = self.get_orders(page=page_num, per_page=result_limit)
            all_orders = all_orders + response
            page_num = page_num + 1
            val = len(response)

        orders_df = pd.json_normalize(all_orders)

        select_statement_executor = SELECTQueryExecutor(
            orders_df, selected_columns, where_conditions, order_by_conditions
        )
        orders_df = select_statement_executor.execute_query()

        return orders_df

    def insert(self, query: ast.Insert) -> None:
        """Inserts data into the Shopify "POST /orders" API endpoint.

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
            supported_columns=[
                "parent_id",
                "status",
                "currency",
                "customer_id",
                "customer_note",
                "billing",
                "shipping",
                "payment_method",
                "payment_method_title",
                "transaction_id",
                "meta_data",
                "line_items",
                "shipping_lines",
                "fee_lines",
                "coupon_lines",
                "set_paid",
            ],
            mandatory_columns=["line_items"],
            all_mandatory=False,
        )
        order_data = insert_statement_parser.parse_query()
        self.create_orders(order_data)

    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from the Shopify "DELETE /orders" API endpoint.

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

        orders_df = pd.json_normalize(self.get_orders())

        delete_query_executor = DELETEQueryExecutor(orders_df, where_conditions)

        orders_df = delete_query_executor.execute_query()

        order_ids = orders_df["id"].tolist()
        self.delete_orders(order_ids)

    def update(self, query: ast.Update) -> None:
        """Updates data from the Shopify "PUT /orders" API endpoint.

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

        orders_df = pd.json_normalize(self.get_orders())
        update_query_executor = UPDATEQueryExecutor(orders_df, where_conditions)

        orders_df = update_query_executor.execute_query()
        order_ids = orders_df["id"].tolist()
        self.update_orders(order_ids, values_to_update)

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_orders(per_page=1)).columns.tolist()

    def get_orders(self, **kwargs) -> List[Dict]:
        wcapi = self.handler.connect()
        orders = wcapi.get("orders", params=kwargs)
        return [order for order in orders.json()]

    def update_orders(
        self, order_ids: List[int], values_to_update: Dict[Text, Any]
    ) -> None:
        wcapi = self.handler.connect()

        for order_id in order_ids:
            order = wcapi.get(f"orders/{order_id}")
            for key, value in values_to_update.items():
                setattr(order, key, value)
            wcapi.put(f"orders/{order_id}", order)
            logger.info(f"order {order_id} updated")

    def delete_orders(self, order_ids: List[int]) -> None:
        wcapi = self.handler.connect()
        for order_id in order_ids:
            wcapi.delete(f"orders/{order_id}")
            logger.info(f"order {order_id} deleted")

    def create_orders(self, order_data: List[Dict[Text, Any]]) -> None:
        wcapi = self.handler.connect()

        for order in order_data:
            created_order = wcapi.post("orders", order)
            if "id" not in created_order.json():
                raise Exception("order creation failed")
            else:
                logger.info(f'order {created_order.json()["id"]} created')


class TaxesTable(APITable):
    """The woocommerce taxes Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /taxes" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify taxes matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(query, "taxes", self.get_columns())
        selected_columns, where_conditions, tax_by_conditions, result_limit = (
            select_statement_parser.parse_query()
        )

        all_taxes = list()
        val = None
        page_num = 1
        while val != 0:
            response = self.get_taxes(page=page_num, per_page=result_limit)
            all_taxes = all_taxes + response
            page_num = page_num + 1
            val = len(response)

        taxes_df = pd.json_normalize(all_taxes)

        select_statement_executor = SELECTQueryExecutor(
            taxes_df, selected_columns, where_conditions, tax_by_conditions
        )
        taxes_df = select_statement_executor.execute_query()

        return taxes_df

    def insert(self, query: ast.Insert) -> None:
        """Inserts data into the Shopify "POST /taxes" API endpoint.

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
            supported_columns=[
                "country",
                "state",
                "postcode",
                "city",
                "postcodes",
                "cities",
                "rate",
                "name",
                "priority",
                "compound",
                "shipping",
                "order",
                "class",
            ],
            mandatory_columns=["name"],
            all_mandatory=False,
        )
        tax_data = insert_statement_parser.parse_query()
        self.create_taxes(tax_data)

    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from the Shopify "DELETE /taxes" API endpoint.

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

        taxes_df = pd.json_normalize(self.get_taxes())

        delete_query_executor = DELETEQueryExecutor(taxes_df, where_conditions)

        taxes_df = delete_query_executor.execute_query()

        tax_ids = taxes_df["id"].tolist()
        self.delete_taxes(tax_ids)

    def update(self, query: ast.Update) -> None:
        """Updates data from the Shopify "PUT /taxes" API endpoint.

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

        taxes_df = pd.json_normalize(self.get_taxes())
        update_query_executor = UPDATEQueryExecutor(taxes_df, where_conditions)

        taxes_df = update_query_executor.execute_query()
        tax_ids = taxes_df["id"].tolist()
        self.update_taxes(tax_ids, values_to_update)

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_taxes(per_page=1)).columns.tolist()

    def get_taxes(self, **kwargs) -> List[Dict]:
        wcapi = self.handler.connect()
        taxes = wcapi.get("taxes", params=kwargs)
        return [tax for tax in taxes.json()]

    def update_taxes(
        self, tax_ids: List[int], values_to_update: Dict[Text, Any]
    ) -> None:
        wcapi = self.handler.connect()

        for tax_id in tax_ids:
            tax = wcapi.get(f"taxes/{tax_id}")
            for key, value in values_to_update.items():
                setattr(tax, key, value)
            wcapi.put(f"taxes/{tax_id}", tax)
            logger.info(f"tax {tax_id} updated")

    def delete_taxes(self, tax_ids: List[int]) -> None:
        wcapi = self.handler.connect()
        for tax_id in tax_ids:
            wcapi.delete(f"taxes/{tax_id}")
            logger.info(f"tax {tax_id} deleted")

    def create_taxes(self, tax_data: List[Dict[Text, Any]]) -> None:
        wcapi = self.handler.connect()

        for tax in tax_data:
            created_tax = wcapi.post("taxes", tax)
            if "id" not in created_tax.json():
                raise Exception("tax creation failed")
            else:
                logger.info(f'tax {created_tax.json()["id"]} created')


class SettingsTable(APITable):
    """The woocommerce settings Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /settings" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify settings matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query, "settings", self.get_columns()
        )
        selected_columns, where_conditions, setting_by_conditions, result_limit = (
            select_statement_parser.parse_query()
        )

        settings_df = pd.json_normalize(self.get_settings(per_page=result_limit))

        select_statement_executor = SELECTQueryExecutor(
            settings_df, selected_columns, where_conditions, setting_by_conditions
        )
        settings_df = select_statement_executor.execute_query()

        return settings_df

        """Updates data from the Shopify "PUT /settings" API endpoint.

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

        settings_df = pd.json_normalize(self.get_settings())
        update_query_executor = UPDATEQueryExecutor(settings_df, where_conditions)

        settings_df = update_query_executor.execute_query()
        setting_ids = settings_df["id"].tolist()
        self.update_settings(setting_ids, values_to_update)

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_settings(per_page=1)).columns.tolist()

    def get_settings(self, **kwargs) -> List[Dict]:
        wcapi = self.handler.connect()
        settings = wcapi.get("settings", params=kwargs)
        return [setting for setting in settings.json()]


class CouponsTable(APITable):
    """The woocommerce coupons Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /coupons" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify coupons matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query, "coupons", self.get_columns()
        )
        selected_columns, where_conditions, coupon_by_conditions, result_limit = (
            select_statement_parser.parse_query()
        )

        all_coupons = list()
        val = None
        page_num = 1
        while val != 0:
            response = self.get_coupons(page=page_num, per_page=result_limit)
            all_coupons = all_coupons + response
            page_num = page_num + 1
            val = len(response)

        coupons_df = pd.json_normalize(all_coupons)

        select_statement_executor = SELECTQueryExecutor(
            coupons_df, selected_columns, where_conditions, coupon_by_conditions
        )
        coupons_df = select_statement_executor.execute_query()

        return coupons_df

    def insert(self, query: ast.Insert) -> None:
        """Inserts data into the Shopify "POST /coupons" API endpoint.

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
            supported_columns=[
                "code",
                "amount",
                "discount_type",
                "description",
                "date_expires",
                "date_expires",
                "individual_use",
                "product_ids",
                "excluded_product_ids",
                "usage_limit",
                "usage_limit_per_user",
                "limit_usage_to_x_items",
                "free_shipping",
                "product_categories",
                "excluded_product_categories",
                "exclude_sale_items",
                "minimum_amount",
                "maximum_amount",
                "email_restrictions",
                "meta_data",
            ],
            mandatory_columns=["code"],
            all_mandatory=False,
        )
        coupon_data = insert_statement_parser.parse_query()
        self.create_coupons(coupon_data)

    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from the Shopify "DELETE /coupons" API endpoint.

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

        coupons_df = pd.json_normalize(self.get_coupons())

        delete_query_executor = DELETEQueryExecutor(coupons_df, where_conditions)

        coupons_df = delete_query_executor.execute_query()

        coupon_ids = coupons_df["id"].tolist()
        self.delete_coupons(coupon_ids)

    def update(self, query: ast.Update) -> None:
        """Updates data from the Shopify "PUT /coupons" API endpoint.

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

        coupons_df = pd.json_normalize(self.get_coupons())
        update_query_executor = UPDATEQueryExecutor(coupons_df, where_conditions)

        coupons_df = update_query_executor.execute_query()
        coupon_ids = coupons_df["id"].tolist()
        self.update_coupons(coupon_ids, values_to_update)

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_coupons(per_page=1)).columns.tolist()

    def get_coupons(self, **kwargs) -> List[Dict]:
        wcapi = self.handler.connect()
        coupons = wcapi.get("coupons", params=kwargs)
        return [coupon for coupon in coupons.json()]

    def update_coupons(
        self, coupon_ids: List[int], values_to_update: Dict[Text, Any]
    ) -> None:
        wcapi = self.handler.connect()

        for coupon_id in coupon_ids:
            coupon = wcapi.get(f"coupons/{coupon_id}")
            for key, value in values_to_update.items():
                setattr(coupon, key, value)
            wcapi.put(f"coupons/{coupon_id}", coupon)
            logger.info(f"coupon {coupon_id} updated")

    def delete_coupons(self, coupon_ids: List[int]) -> None:
        wcapi = self.handler.connect()
        for coupon_id in coupon_ids:
            wcapi.delete(f"coupons/{coupon_id}")
            logger.info(f"coupon {coupon_id} deleted")

    def create_coupons(self, coupon_data: List[Dict[Text, Any]]) -> None:
        wcapi = self.handler.connect()

        for coupon in coupon_data:
            created_coupon = wcapi.post("coupons", coupon)
            if "id" not in created_coupon.json():
                raise Exception("coupon creation failed")
            else:
                logger.info(f'coupon {created_coupon.json()["id"]} created')


class ShippingZonesTable(APITable):
    """The woocommerce shipping_zones Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /shipping_zones" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify shipping_zones matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query, "shipping_zones", self.get_columns()
        )
        (
            selected_columns,
            where_conditions,
            shipping_zone_by_conditions,
            result_limit,
        ) = select_statement_parser.parse_query()

        shipping_zones_df = pd.json_normalize(self.get_shipping_zones())

        select_statement_executor = SELECTQueryExecutor(
            shipping_zones_df,
            selected_columns,
            where_conditions,
            shipping_zone_by_conditions,
        )
        shipping_zones_df = select_statement_executor.execute_query()

        return shipping_zones_df

    def insert(self, query: ast.Insert) -> None:
        """Inserts data into the Shopify "POST /shipping_zones" API endpoint.

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
            supported_columns=["name", "order"],
            mandatory_columns=["name"],
            all_mandatory=False,
        )
        shipping_zone_data = insert_statement_parser.parse_query()
        self.create_shipping_zones(shipping_zone_data)

    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from the Shopify "DELETE /shipping_zones" API endpoint.

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

        shipping_zones_df = pd.json_normalize(self.get_shipping_zones())

        delete_query_executor = DELETEQueryExecutor(shipping_zones_df, where_conditions)

        shipping_zones_df = delete_query_executor.execute_query()

        shipping_zone_ids = shipping_zones_df["id"].tolist()
        self.delete_shipping_zones(shipping_zone_ids)

    def update(self, query: ast.Update) -> None:
        """Updates data from the Shopify "PUT /shipping_zones" API endpoint.

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

        shipping_zones_df = pd.json_normalize(self.get_shipping_zones())
        update_query_executor = UPDATEQueryExecutor(shipping_zones_df, where_conditions)

        shipping_zones_df = update_query_executor.execute_query()
        shipping_zone_ids = shipping_zones_df["id"].tolist()
        self.update_shipping_zones(shipping_zone_ids, values_to_update)

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_shipping_zones(per_page=1)).columns.tolist()

    def get_shipping_zones(self, **kwargs) -> List[Dict]:
        wcapi = self.handler.connect()
        shipping_zones = wcapi.get("shipping/zones", params=kwargs)
        return [shipping_zone for shipping_zone in shipping_zones.json()]

    def update_shipping_zones(
        self, shipping_zone_ids: List[int], values_to_update: Dict[Text, Any]
    ) -> None:
        wcapi = self.handler.connect()

        for shipping_zone_id in shipping_zone_ids:
            shipping_zone = wcapi.get(f"shipping/zones/{shipping_zone_id}")
            for key, value in values_to_update.items():
                setattr(shipping_zone, key, value)
            wcapi.put(f"shipping_zones/{shipping_zone_id}", shipping_zone)
            logger.info(f"shipping_zone {shipping_zone_id} updated")

    def delete_shipping_zones(self, shipping_zone_ids: List[int]) -> None:
        wcapi = self.handler.connect()
        for shipping_zone_id in shipping_zone_ids:
            wcapi.delete(f"shipping/zones/{shipping_zone_id}")
            logger.info(f"Shipping_zone {shipping_zone_id} deleted")

    def create_shipping_zones(self, shipping_zone_data: List[Dict[Text, Any]]) -> None:
        wcapi = self.handler.connect()

        for shipping_zone in shipping_zone_data:
            created_shipping_zone = wcapi.post("shipping/zones", shipping_zone)
            if "id" not in created_shipping_zone.json():
                raise Exception("Shipping zone creation failed")
            else:
                logger.info(
                    f'Shipping_zone {created_shipping_zone.json()["id"]} created'
                )


class PaymentGatewaysTable(APITable):
    """The woocommerce payment_gateways Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /payment_gateways" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify payment_gateways matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query, "payment_gateways", self.get_columns()
        )
        (
            selected_columns,
            where_conditions,
            payment_gateway_by_conditions,
            result_limit,
        ) = select_statement_parser.parse_query()

        payment_gateways_df = pd.json_normalize(
            self.get_payment_gateways(per_page=result_limit)
        )

        select_statement_executor = SELECTQueryExecutor(
            payment_gateways_df,
            selected_columns,
            where_conditions,
            payment_gateway_by_conditions,
        )
        payment_gateways_df = select_statement_executor.execute_query()

        return payment_gateways_df

        """Updates data from the Shopify "PUT /payment_gateways" API endpoint.

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

        payment_gateways_df = pd.json_normalize(self.get_payment_gateways())
        update_query_executor = UPDATEQueryExecutor(
            payment_gateways_df, where_conditions
        )

        payment_gateways_df = update_query_executor.execute_query()
        payment_gateway_ids = payment_gateways_df["id"].tolist()
        self.update_payment_gateways(payment_gateway_ids, values_to_update)

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_payment_gateways(per_page=1)).columns.tolist()

    def get_payment_gateways(self, **kwargs) -> List[Dict]:
        wcapi = self.handler.connect()
        payment_gateways = wcapi.get("payment_gateways", params=kwargs)
        return [payment_gateway for payment_gateway in payment_gateways.json()]


class ReportSalesTable(APITable):
    """The woocommerce report_sales Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /report/sales" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify report_sales matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query, "report_sales", self.get_columns()
        )
        selected_columns, where_conditions, report_sale_by_conditions, result_limit = (
            select_statement_parser.parse_query()
        )

        report_sales_df = pd.json_normalize(self.get_report_sales())

        select_statement_executor = SELECTQueryExecutor(
            report_sales_df,
            selected_columns,
            where_conditions,
            report_sale_by_conditions,
        )
        report_sales_df = select_statement_executor.execute_query()

        return report_sales_df

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_report_sales(per_page=1)).columns.tolist()

    def get_report_sales(self, **kwargs) -> List[Dict]:
        wcapi = self.handler.connect()
        report_sales = wcapi.get("reports/sales", params=kwargs)
        return [report_sale for report_sale in report_sales.json()]


class ReportTopSellersTable(APITable):
    """The woocommerce report_top_sellers Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /report/sales" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify report_top_sellers matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query, "report_top_sellers", self.get_columns()
        )
        (
            selected_columns,
            where_conditions,
            report_top_seller_by_conditions,
            result_limit,
        ) = select_statement_parser.parse_query()

        report_top_sellers_df = pd.json_normalize(self.get_report_top_sellers())

        select_statement_executor = SELECTQueryExecutor(
            report_top_sellers_df,
            selected_columns,
            where_conditions,
            report_top_seller_by_conditions,
        )
        report_top_sellers_df = select_statement_executor.execute_query()

        return report_top_sellers_df

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(
            self.get_report_top_sellers(per_page=1)
        ).columns.tolist()

    def get_report_top_sellers(self, **kwargs) -> List[Dict]:
        wcapi = self.handler.connect()
        report_top_sellers = wcapi.get("reports/top_sellers", params=kwargs)
        return [report_top_seller for report_top_seller in report_top_sellers.json()]


class SettingOptionTable(APITable):
    """The woocommerce setting_options Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /report/sales" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify setting_options matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query, "setting_options", self.get_columns()
        )
        (
            selected_columns,
            where_conditions,
            setting_option_by_conditions,
            result_limit,
        ) = select_statement_parser.parse_query()

        setting_options_df = pd.json_normalize(self.get_setting_options())

        select_statement_executor = SELECTQueryExecutor(
            setting_options_df,
            selected_columns,
            where_conditions,
            setting_option_by_conditions,
        )
        setting_options_df = select_statement_executor.execute_query()

        return setting_options_df

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_setting_options(per_page=1)).columns.tolist()

    def get_setting_options(self, **kwargs) -> List[Dict]:
        wcapi = self.handler.connect()
        setting_options = wcapi.get("settings/general", params=kwargs)
        return [setting_option for setting_option in setting_options.json()]


class RefundsTable(APITable):
    """The woocommerce refunds Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /refunds" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify refunds matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query, "refunds", self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = (
            select_statement_parser.parse_query()
        )

        all_refunds = list()
        order_ids = self.get_order_with_refunds()

        for order_id in order_ids:
            val = None
            page_num = 1
            while val != 0:
                refunds = self.get_refunds(
                    order_id, page=page_num, per_page=result_limit
                )
                logger.info(f"response {refunds}")
                all_refunds = all_refunds + refunds
                page_num = page_num + 1
                val = len(refunds)

        logger.info(f"total refunds {all_refunds}")
        refunds_df = pd.json_normalize(all_refunds)

        select_statement_executor = SELECTQueryExecutor(
            refunds_df, selected_columns, where_conditions, order_by_conditions
        )
        refunds_df = select_statement_executor.execute_query()

        return refunds_df

    def insert(self, query: ast.Insert) -> None:
        """Inserts data into the Shopify "POST /customers" API endpoint.

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
            supported_columns=[
                "order_id",
                "amount",
                "reason",
                "refunded_by",
                "meta_data",
                "line_items",
              
            ],
            mandatory_columns=["order_id", 'amount'],
            all_mandatory=False,
        )
        refund_data = insert_statement_parser.parse_query()
        order_id = refund_data[0]
        refund_data.remove(order_id)
        self.create_refunds(order_id, refund_data=refund_data)


    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from the Shopify "DELETE /refunds" API endpoint.

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

        order_id = where_conditions["order_id"]

        refund_ids = where_conditions["id"]

        refunds_df = pd.json_normalize(self.get_refunds(order_id))

        delete_query_executor = DELETEQueryExecutor(refunds_df, where_conditions)

        refunds_df = delete_query_executor.execute_query()

        refund_ids = refunds_df["id"].tolist()
        self.delete_refunds(order_id, refund_ids)

    def get_columns(self) -> List[Text]:
        return [
            "id",
            "date_created",
            "date_created_gmt",
            "amount",
            "reason",
            "refunded_by",
            "refunded_payment",
            "meta_data",
            "line_items",
            "shipping_lines",
            "tax_lines",
            "fee_lines",
            "_links.self",
            "_links.collection",
            "_links.up",
        ]

    def get_refunds(self, order_id, **kwargs) -> List[Dict]:
        wcapi = self.handler.connect()
        refunds = wcapi.get(f"orders/{order_id}/refunds", params=kwargs)
        logger.info(f" this is {refunds.json()}")
        return [refund for refund in refunds.json()]

    def get_order_with_refunds(self, **kwargs):
        wcapi = self.handler.connect()
        order_refund_ids = list()
        val = None
        page_num = 1
        while val != 0:
            orders = wcapi.get(
                "orders", params={"page": page_num, "per_page": 20}
            ).json()

            for order in orders:
                if order["refunds"]:
                    order_refund_ids.append(order["id"])
            page_num = page_num + 1
            val = len(orders)
        return order_refund_ids

    def delete_refunds(self, order_id, refund_ids: List[int]) -> None:
        wcapi = self.handler.connect()
        for refund_id in refund_ids:
            wcapi.delete(f"orders/{order_id}/refunds/{refund_id}")
            logger.info(f"refund {refund_id} deleted")

    def create_refunds(self, order_id , refund_data: List[Dict[Text, Any]]) -> None:
        wcapi = self.handler.connect()

        for refund in refund_data:
            created_refund = wcapi.post(f"orders/{order_id}/refunds", refund)
            if "id" not in created_refund.json():
                raise Exception("refund creation failed")
            else:
                logger.info(f'refund {created_refund.json()["id"]} created')


class OrderNotesTable(APITable):
    """The woocommerce order_notes Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /order_notes" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify order_notes matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query, "order_notes", self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = (
            select_statement_parser.parse_query()
        )

        all_order_notes = list()
        order_ids = self.get_order_with_order_notes()

        for order_id in order_ids:     
            order_notes = self.get_order_notes(
                order_id
            )
            all_order_notes = all_order_notes + order_notes
            

        logger.info(f"total order_notes {all_order_notes}")
        order_notes_df = pd.json_normalize(all_order_notes)

        select_statement_executor = SELECTQueryExecutor(
            order_notes_df, selected_columns, where_conditions, order_by_conditions
        )
        order_notes_df = select_statement_executor.execute_query()

        return order_notes_df

    def insert(self, query: ast.Insert) -> None:
        """Inserts data into the Shopify "POST /customers" API endpoint.

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
            supported_columns=[
                "note",
                "customer_note",
                "added_by_user",
               
              
            ],
            mandatory_columns=["note"],
            all_mandatory=False,
        )
        order_note_data = insert_statement_parser.parse_query()
        order_id = order_note_data[0]
        order_note_data.remove(order_id)
        self.create_order_notes(order_id, order_note_data=order_note_data)


    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from the Shopify "DELETE /order_notes" API endpoint.

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

        order_id = where_conditions["order_id"]

        order_note_ids = where_conditions["id"]

        order_notes_df = pd.json_normalize(self.get_order_notes(order_id))

        delete_query_executor = DELETEQueryExecutor(order_notes_df, where_conditions)

        order_notes_df = delete_query_executor.execute_query()

        order_note_ids = order_notes_df["id"].tolist()
        self.delete_order_notes(order_id, order_note_ids)

    def get_columns(self) -> List[Text]:
        return ['id', 'author', 'date_created', 'date_created_gmt', 'note', 'customer_note', '_links.self', '_links.collection', '_links.up']

    def get_order_notes(self, order_id, **kwargs) -> List[Dict]:
        wcapi = self.handler.connect()
        order_notes = wcapi.get(f"orders/{order_id}/notes", params=kwargs)
        logger.info(f" this is {order_notes.json()}")
        return [order_note for order_note in order_notes.json()]

    def get_order_with_order_notes(self, **kwargs):
        wcapi = self.handler.connect()
        order_ids = list()
        val = None
        page_num = 1
        while val != 0:
            orders = wcapi.get(
                "orders", params={"page": page_num, "per_page": 20}
            ).json()
            
            order_ids = order_ids + [order["id"] for order in orders]
            page_num = page_num + 1
            val = len(orders)
                
        return order_ids

    def delete_order_notes(self, order_id, order_note_ids: List[int]) -> None:
        wcapi = self.handler.connect()
        for order_note_id in order_note_ids:
            wcapi.delete(f"orders/{order_id}/notes/{order_note_id}")
            logger.info(f"order_note {order_note_id} deleted")

    def create_order_notes(self, order_id , order_note_data: List[Dict[Text, Any]]) -> None:
        wcapi = self.handler.connect()

        for order_note in order_note_data:
            created_order_note = wcapi.post(f"orders/{order_id}/notes", order_note)
            if "id" not in created_order_note.json():
                raise Exception("order_note creation failed")
            else:
                logger.info(f'order_note {created_order_note.json()["id"]} created')


class ProductReviewTable(APITable):
    """The woocommerce product_reviews Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /product_reviews" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify product_reviews matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(query, "product_reviews", self.get_columns())
        selected_columns, where_conditions, product_review_by_conditions, result_limit = (
            select_statement_parser.parse_query()
        )

        all_product_reviews = list()
        val = None
        page_num = 1
        while val != 0:
            response = self.get_product_reviews(page=page_num, per_page=result_limit)
            all_product_reviews = all_product_reviews + response
            page_num = page_num + 1
            val = len(response)
            logger.info(f"all_product_reviews {all_product_reviews}")

        product_reviews_df = pd.json_normalize(all_product_reviews)

        select_statement_executor = SELECTQueryExecutor(
            product_reviews_df, selected_columns, where_conditions, product_review_by_conditions
        )
        product_reviews_df = select_statement_executor.execute_query()

        return product_reviews_df

    def insert(self, query: ast.Insert) -> None:
        """Inserts data into the Shopify "POST /product_reviews" API endpoint.

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
            supported_columns=[
                "product_id",
                "status",
                "reviewer",
                "reviewer_email",
                "review",
                "rating",
                "verified",
    
            ],
            mandatory_columns=["product_id"],
            all_mandatory=False,
        )
        product_review_data = insert_statement_parser.parse_query()
        self.create_product_reviews(product_review_data)

    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from the Shopify "DELETE /product_reviews" API endpoint.

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

        product_reviews_df = pd.json_normalize(self.get_product_reviews())

        delete_query_executor = DELETEQueryExecutor(product_reviews_df, where_conditions)

        product_reviews_df = delete_query_executor.execute_query()

        product_review_ids = product_reviews_df["id"].tolist()
        self.delete_product_reviews(product_review_ids)

    def update(self, query: ast.Update) -> None:
        """Updates data from the Shopify "PUT /product_reviews" API endpoint.

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

        product_reviews_df = pd.json_normalize(self.get_product_reviews())
        update_query_executor = UPDATEQueryExecutor(product_reviews_df, where_conditions)

        product_reviews_df = update_query_executor.execute_query()
        product_review_ids = product_reviews_df["id"].tolist()
        self.update_product_reviews(product_review_ids, values_to_update)

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_product_reviews(per_page=1)).columns.tolist()

    def get_product_reviews(self, **kwargs) -> List[Dict]:
        wcapi = self.handler.connect()
        product_reviews = wcapi.get("products/reviews", params=kwargs)
        return [product_review for product_review in product_reviews.json()]

    def update_product_reviews(
        self, product_review_ids: List[int], values_to_update: Dict[Text, Any]
    ) -> None:
        wcapi = self.handler.connect()

        for product_review_id in product_review_ids:
            product_review = wcapi.get(f"products/reviews/{product_review_id}")
            for key, value in values_to_update.items():
                setattr(product_review, key, value)
            wcapi.put(f"products/reviews/{product_review_id}", product_review)
            logger.info(f"product_review {product_review_id} updated")

    def delete_product_reviews(self, product_review_ids: List[int]) -> None:
        wcapi = self.handler.connect()
        for product_review_id in product_review_ids:
            wcapi.delete(f"products/reviews/{product_review_id}")
            logger.info(f"product_review {product_review_id} deleted")

    def create_product_reviews(self, product_review_data: List[Dict[Text, Any]]) -> None:
        wcapi = self.handler.connect()

        for product_review in product_review_data:
            created_product_review = wcapi.post("products/reviews", product_review)
            if "id" not in created_product_review.json():
                raise Exception("product_review creation failed")
            else:
                logger.info(f'product_review {created_product_review.json()["id"]} created')



class ShippingZoneMethodTable(APITable):
    """The woocommerce shipping_zone_methods Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /shipping_zone_methods" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify shipping_zone_methods matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query, "shipping_zone_methods", self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = (
            select_statement_parser.parse_query()
        )

        all_shipping_zone_methods = list()
        zone_ids = self.get_shipping_zone_id()
        logger.info(f"total shipping_zone_ids {zone_ids}")
        for zone_id in zone_ids:     
            shipping_zone_methods = self.get_shipping_zone_methods(
                zone_id
            )
            all_shipping_zone_methods = all_shipping_zone_methods + shipping_zone_methods
            logger.info(f"total shipping_zone_methods {all_shipping_zone_methods}")

        logger.info(f"total shipping_zone_methods {all_shipping_zone_methods}")
        shipping_zone_methods_df = pd.json_normalize(all_shipping_zone_methods)

        select_statement_executor = SELECTQueryExecutor(
            shipping_zone_methods_df, selected_columns, where_conditions, order_by_conditions
        )
        shipping_zone_methods_df = select_statement_executor.execute_query()

        return shipping_zone_methods_df

    def insert(self, query: ast.Insert) -> None:
        """Inserts data into the Shopify "POST /customers" API endpoint.

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
            supported_columns=[
                "order",
                "enabled",
                "method_id",
                "method_title",
                "method_description",
                "settings",
              
            ],
            mandatory_columns=["note"],
            all_mandatory=False,
        )
        shipping_zone_method_data = insert_statement_parser.parse_query()
        shipping_zone_id = shipping_zone_method_data[0]
        shipping_zone_method_data.remove(shipping_zone_id)
        self.create_shipping_zone_methods(shipping_zone_id, shipping_zone_method_data=shipping_zone_method_data)


    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from the Shopify "DELETE /shipping_zone_methods" API endpoint.

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

        shipping_zone_id = where_conditions["shipping_zone_id"]

        shipping_zone_method_ids = where_conditions["id"]

        shipping_zone_methods_df = pd.json_normalize(self.get_shipping_zone_methods(shipping_zone_id))

        delete_query_executor = DELETEQueryExecutor(shipping_zone_methods_df, where_conditions)

        shipping_zone_methods_df = delete_query_executor.execute_query()

        shipping_zone_method_ids = shipping_zone_methods_df["id"].tolist()
        self.delete_shipping_zone_methods(shipping_zone_id, shipping_zone_method_ids)

    def get_columns(self) -> List[Text]:
        return ['instance_id', 'title', 'order', 'enabled', 'method_id', 'method_title', 'method_description', 'settings.title.id', 'settings.title.label', 'settings.title.description', 'settings.title.type', 'settings.title.value', 'settings.title.default', 'settings.title.tip', 'settings.title.placeholder', 'settings.tax_status.id', 'settings.tax_status.label', 'settings.tax_status.description', 'settings.tax_status.type', 'settings.tax_status.value', 'settings.tax_status.default', 'settings.tax_status.tip', 'settings.tax_status.placeholder', 'settings.tax_status.options.taxable', 'settings.tax_status.options.none', 'settings.cost.id', 'settings.cost.label', 'settings.cost.description', 'settings.cost.type', 'settings.cost.value', 'settings.cost.default', 'settings.cost.tip', 'settings.cost.placeholder', 'settings.class_costs.id', 'settings.class_costs.label', 'settings.class_costs.description', 'settings.class_costs.type', 'settings.class_costs.value', 'settings.class_costs.default', 'settings.class_costs.tip', 'settings.class_costs.placeholder', 'settings.class_cost_92.id', 'settings.class_cost_92.label', 'settings.class_cost_92.description', 'settings.class_cost_92.type', 'settings.class_cost_92.value', 'settings.class_cost_92.default', 'settings.class_cost_92.tip', 'settings.class_cost_92.placeholder', 'settings.class_cost_91.id', 'settings.class_cost_91.label', 'settings.class_cost_91.description', 'settings.class_cost_91.type', 'settings.class_cost_91.value', 'settings.class_cost_91.default', 'settings.class_cost_91.tip', 'settings.class_cost_91.placeholder', 'settings.no_class_cost.id', 'settings.no_class_cost.label', 'settings.no_class_cost.description', 'settings.no_class_cost.type', 'settings.no_class_cost.value', 'settings.no_class_cost.default', 'settings.no_class_cost.tip', 'settings.no_class_cost.placeholder', 'settings.type.id', 'settings.type.label', 'settings.type.description', 'settings.type.type', 'settings.type.value', 'settings.type.default', 'settings.type.tip', 'settings.type.placeholder', 'settings.type.options.class', 'settings.type.options.order', '_links.self', '_links.collection', '_links.describes', 'settings.requires.id', 'settings.requires.label', 'settings.requires.description', 'settings.requires.type', 'settings.requires.value', 'settings.requires.default', 'settings.requires.tip', 'settings.requires.placeholder', 'settings.requires.options.', 'settings.requires.options.coupon', 'settings.requires.options.min_amount', 'settings.requires.options.either', 'settings.requires.options.both', 'settings.min_amount.id', 'settings.min_amount.label', 'settings.min_amount.description', 'settings.min_amount.type', 'settings.min_amount.value', 'settings.min_amount.default', 'settings.min_amount.tip', 'settings.min_amount.placeholder']

    def get_shipping_zone_methods(self, shipping_zone_id, **kwargs) -> List[Dict]:
        wcapi = self.handler.connect()
        shipping_zone_methods = wcapi.get(f"shipping/zones/{shipping_zone_id}/methods", params=kwargs)
        logger.info(f" this is {shipping_zone_methods.json()}")
        return [shipping_zone_method for shipping_zone_method in shipping_zone_methods.json()]

    def get_shipping_zone_id(self, **kwargs):
        wcapi = self.handler.connect()
        shipping_zone_ids = wcapi.get(
            "shipping/zones"
        ).json()
            
        return [shipping_zone_id["id"] for shipping_zone_id in shipping_zone_ids]

    def delete_shipping_zone_methods(self, shipping_zone_id, shipping_zone_method_ids: List[int]) -> None:
        wcapi = self.handler.connect()
        for shipping_zone_method_id in shipping_zone_method_ids:
            wcapi.delete(f"shipping/zones/{shipping_zone_id}/methods/{shipping_zone_method_id}")
            logger.info(f"shipping_zone_method {shipping_zone_method_id} deleted")
