import shopify
import requests
import pandas as pd
from typing import Text, List, Dict, Any

from mindsdb_sql.parser import ast
from mindsdb.integrations.libs.api_handler import APITable

from mindsdb.integrations.utilities.handlers.query_utilities import SELECTQueryParser, SELECTQueryExecutor
from mindsdb.integrations.utilities.handlers.query_utilities import INSERTQueryParser
from mindsdb.integrations.utilities.handlers.query_utilities import DELETEQueryParser, DELETEQueryExecutor
from mindsdb.integrations.utilities.handlers.query_utilities import UPDATEQueryParser, UPDATEQueryExecutor

from mindsdb.utilities import log

logger = log.getLogger(__name__)


class ProductsTable(APITable):
    """The Shopify Products Table implementation"""

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
            query,
            'products',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        products_df = pd.json_normalize(self.get_products(limit=result_limit))

        select_statement_executor = SELECTQueryExecutor(
            products_df,
            selected_columns,
            where_conditions,
            order_by_conditions
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
            supported_columns=['title', 'body_html', 'vendor', 'product_type', 'tags', 'status'],
            mandatory_columns=['title' ],
            all_mandatory=False
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

        delete_query_executor = DELETEQueryExecutor(
            products_df,
            where_conditions
        )

        products_df = delete_query_executor.execute_query()

        product_ids = products_df['id'].tolist()
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
        update_query_executor = UPDATEQueryExecutor(
            products_df,
            where_conditions
        )

        products_df = update_query_executor.execute_query()
        product_ids = products_df['id'].tolist()
        self.update_products(product_ids, values_to_update)

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_products(limit=1)).columns.tolist()

    def get_products(self, **kwargs) -> List[Dict]:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)
        products = shopify.Product.find(**kwargs)
        return [product.to_dict() for product in products]

    def update_products(self, product_ids: List[int], values_to_update: Dict[Text, Any]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for product_id in product_ids:
            product = shopify.Product.find(product_id)
            for key, value in values_to_update.items():
                setattr(product, key, value)
            product.save()
            logger.info(f'Product {product_id} updated')

    def delete_products(self, product_ids: List[int]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for product_id in product_ids:
            product = shopify.Product.find(product_id)
            product.destroy()
            logger.info(f'Product {product_id} deleted')

    def create_products(self, product_data: List[Dict[Text, Any]]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for product in product_data:
            created_product = shopify.Product.create(product)
            if 'id' not in created_product.to_dict():
                raise Exception('Product creation failed')
            else:
                logger.info(f'Product {created_product.to_dict()["id"]} created')


class CustomersTable(APITable):
    """The Shopify Customers Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /customers" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify Customers matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        select_statement_parser = SELECTQueryParser(
            query,
            'customers',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        customers_df = pd.json_normalize(self.get_customers(limit=result_limit))

        select_statement_executor = SELECTQueryExecutor(
            customers_df,
            selected_columns,
            where_conditions,
            order_by_conditions
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
            supported_columns=['first_name', 'last_name', 'email', 'phone', 'tags', 'currency'],
            mandatory_columns=['first_name', 'last_name', 'email', 'phone'],
            all_mandatory=False
        )
        customer_data = insert_statement_parser.parse_query()
        self.create_customers(customer_data)

    def update(self, query: ast.Update) -> None:
        """Updates data in the Shopify "PUT /customers" API endpoint.

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

        update_query_executor = UPDATEQueryExecutor(
            customers_df,
            where_conditions
        )

        customers_df = update_query_executor.execute_query()

        customer_ids = customers_df['id'].tolist()

        self.update_customers(customer_ids, values_to_update)

    def delete(self, query: ast.Delete) -> None:
        """Deletes data from the Shopify "DELETE /customers" API endpoint.

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

        delete_query_executor = DELETEQueryExecutor(
            customers_df,
            where_conditions
        )

        customers_df = delete_query_executor.execute_query()

        customer_ids = customers_df['id'].tolist()
        self.delete_customers(customer_ids)

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_customers(limit=1)).columns.tolist()

    def get_customers(self, **kwargs) -> List[Dict]:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)
        customers = shopify.Customer.find(**kwargs)
        return [customer.to_dict() for customer in customers]

    def create_customers(self, customer_data: List[Dict[Text, Any]]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for customer in customer_data:
            created_customer = shopify.Customer.create(customer)
            if 'id' not in created_customer.to_dict():
                raise Exception('Customer creation failed')
            else:
                logger.info(f'Customer {created_customer.to_dict()["id"]} created')

    def update_customers(self, customer_ids: List[int], values_to_update: List[Dict[Text, Any]]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for customer_id in customer_ids:
            customer = shopify.Customer.find(customer_id)
            for key, value in values_to_update.items():
                setattr(customer, key, value)
            customer.save()
            logger.info(f'Customer {customer_id} updated')

    def delete_customers(self, customer_ids: List[int]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for customer_id in customer_ids:
            customer = shopify.Customer.find(customer_id)
            customer.delete()
            logger.info(f'Customer {customer_id} deleted')


class OrdersTable(APITable):
    """The Shopify Orders Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /orders" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify Orders matching the query

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
        orders_df = pd.json_normalize(self.get_orders(limit=result_limit))

        select_statement_executor = SELECTQueryExecutor(
            orders_df,
            selected_columns,
            where_conditions,
            order_by_conditions
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
            supported_columns=['billing_address', 'line_items', 'buyer_accepts_marketing', 'cancel_reason', 'customer', 'discount_codes', 'email', 'fulfillments', 'fulfillment_status', 'merchant_of_record_app_id', 'note', 'note_attributes', 'number', 'phone',
                               'po_number', 'presentment_currency', 'processed_at', 'referring_site', 'shipping_address', 'shipping_lines', 'source_name', 'source_identifier', 'source_url', 'subtotal_price', 'subtotal_price_set', 'tags', 'tax_lines', 'taxes_included', 
                               'total_discounts', 'total_discounts_set', 'total_line_items_price', 'total_line_items_price_set', 'total_price', 'total_price_set', 'total_shipping_price_set', 'total_tax', 'total_tax_set', 'total_weight', 'user_id'],
            mandatory_columns=['line_items' ],
            all_mandatory=False
        )
        order_data = insert_statement_parser.parse_query()
        self.create_orders(order_data)
    
    def update(self, query: ast.Update) -> None:
        """Updates data in the Shopify "PUT /orders" API endpoint.

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

        update_statement_executor = UPDATEQueryExecutor(
            orders_df,
            where_conditions
        )
        orders_df = update_statement_executor.execute_query()
        orders_ids = orders_df['id'].tolist()
        self.update_orders(orders_ids, values_to_update)

    def delete(self, query: ast.Delete) -> None:
        """Deletes data from the Shopify "DELETE /orders" API endpoint.

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

        delete_query_executor = DELETEQueryExecutor(
            orders_df,
            where_conditions
        )

        orders_df = delete_query_executor.execute_query()

        order_ids = orders_df['id'].tolist()
        self.delete_orders(order_ids)

    def update_orders(self, order_ids: List[int], values_to_update: Dict[Text, Any]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for order_id in order_ids:
            order = shopify.Order.find(order_id)
            for key, value in values_to_update.items():
                setattr(order, key, value)
            order.save()
            logger.info(f'Order {order_id} updated')

    def delete_orders(self, order_ids: List[int]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for order_id in order_ids:
            order = shopify.Order.find(order_id)
            order.destroy()
            logger.info(f'Order {order_id} deleted')
    
    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_orders(limit=1)).columns.tolist()

    def get_orders(self, **kwargs) -> List[Dict]:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)
        orders = shopify.Order.find(**kwargs)
        return [order.to_dict() for order in orders]

    def create_orders(self, order_data: List[Dict[Text, Any]]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for order in order_data:
            created_order = shopify.Order.create(order)
            if 'id' not in created_order.to_dict():
                raise Exception('Order creation failed')
            else:
                logger.info(f'Order {created_order.to_dict()["id"]} created')


class InventoryLevelTable(APITable):
    """The Shopify Inventory Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /inventory" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame

            Shopify Inventory matching the inventory_item_id or/and location_id

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        select_statement_parser = SELECTQueryParser(
            query,
            'inventory_level',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        search_params = {}
        subset_where_conditions = []
        for op, arg1, arg2 in where_conditions:
            if arg1 == 'inventory_item_ids':
                if op == '=':
                    search_params["inventory_item_ids"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for inventory_item_ids column.")
            elif arg1 == 'location_ids':
                if op == '=':
                    search_params["location_ids"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for location_ids column.")
            elif arg1 in ['available', 'updated_at']:
                subset_where_conditions.append([op, arg1, arg2])

        filter_flag = ("inventory_item_ids" in search_params) or ("location_ids" in search_params)

        if not filter_flag:
            raise NotImplementedError("inventory_item_ids or location_ids column has to be present in where clause.")

        search_params["limit"] = result_limit

        inventory_df = pd.json_normalize(self.get_inventory(search_params))

        self.clean_selected_columns(selected_columns)

        select_statement_executor = SELECTQueryExecutor(
            inventory_df,
            selected_columns,
            subset_where_conditions,
            order_by_conditions
        )
        inventory_df = select_statement_executor.execute_query()

        return inventory_df

    def clean_selected_columns(self, selected_cols) -> List[Text]:
        if "inventory_item_ids" in selected_cols:
            selected_cols.remove("inventory_item_ids")
            selected_cols.append("inventory_item_id")
        if "location_ids" in selected_cols:
            selected_cols.remove("location_ids")
            selected_cols.append("location_id")

    def get_columns(self) -> List[Text]:
        return ["inventory_item_ids", "location_ids", "available", "updated_at"]

    def get_inventory(self, kwargs) -> List[Dict]:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)
        inventories = shopify.InventoryLevel.find(**kwargs)
        return [inventory.to_dict() for inventory in inventories]


class LocationTable(APITable):
    """The Shopify Location Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /locations" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame

            Shopify Locations matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        select_statement_parser = SELECTQueryParser(
            query,
            'locations',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        locations_df = pd.json_normalize(self.get_locations(limit=result_limit))

        select_statement_executor = SELECTQueryExecutor(
            locations_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )

        locations_df = select_statement_executor.execute_query()

        return locations_df

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_locations(limit=1)).columns.tolist()

    def get_locations(self, **kwargs) -> List[Dict]:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)
        locations = shopify.Location.find(**kwargs)
        return [location.to_dict() for location in locations]


class CustomerReviews(APITable):
    """The Shopify Customer Reviews Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Yotpo "GET https://api.yotpo.com/v1/apps/{app_key}/reviews?utoken={utoken}" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame

            Shopify customer reviews matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        select_statement_parser = SELECTQueryParser(
            query,
            'customer_reviews',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        customer_reviews_df = pd.json_normalize(self.get_customer_reviews(limit=result_limit))

        select_statement_executor = SELECTQueryExecutor(
            customer_reviews_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )

        customer_reviews_df = select_statement_executor.execute_query()

        return customer_reviews_df

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_customer_reviews(limit=1)).columns.tolist()

    def get_customer_reviews(self, **kwargs) -> List[Dict]:
        if self.handler.yotpo_app_key is None or self.handler.yotpo_access_token is None:
            raise Exception("You need to provide 'yotpo_app_key' and 'yotpo_access_token' to retrieve customer reviews.")
        url = f"https://api.yotpo.com/v1/apps/{self.handler.yotpo_app_key}/reviews?count=0&utoken={self.handler.yotpo_access_token}"
        headers = {
            "accept": "application/json",
            "Content-Type": "application/json"
        }
        json_response = requests.get(url, headers=headers).json()
        return [review for review in json_response['reviews']] if 'reviews' in json_response else []


class CarrierServiceTable(APITable):
    """The Shopify carrier service Table implementation. Example carrier services like usps, dhl etc."""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /carrier_services" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame

            Shopify Carrier service info

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        select_statement_parser = SELECTQueryParser(
            query,
            'carrier_service',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        carrier_service_df = pd.json_normalize(self.get_carrier_service())

        select_statement_executor = SELECTQueryExecutor(
            carrier_service_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        
        carrier_service_df = select_statement_executor.execute_query()

        return carrier_service_df
    
    def insert(self, query: ast.Insert) -> None:
        """Inserts data into the Shopify "POST /carrier_services" API endpoint.

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
            supported_columns=['name', 'callback_url', 'service_discovery'],
            mandatory_columns=['name', 'callback_url', 'service_discovery'],
            all_mandatory=True
        )
        carrier_service_data = insert_statement_parser.parse_query()
        self.create_carrier_service(carrier_service_data)

    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from the Shopify "DELETE /carrier_services" API endpoint.

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

        carrier_services_df = pd.json_normalize(self.get_carrier_service())

        delete_query_executor = DELETEQueryExecutor(
            carrier_services_df,
            where_conditions
        )

        carrier_services_df = delete_query_executor.execute_query()

        carrier_service_ids = carrier_services_df['id'].tolist()
        self.delete_carrier_services(carrier_service_ids)


    def update(self, query: ast.Update) -> None:
        """Updates data from the Shopify "PUT /carrier_services" API endpoint.

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
        carrier_services_df = pd.json_normalize(self.get_carrier_service())
        update_query_executor = UPDATEQueryExecutor(
            carrier_services_df,
            where_conditions
        )

        carrier_services_df = update_query_executor.execute_query()
        carrier_service_ids = carrier_services_df['id'].tolist()
        self.update_carrier_service(carrier_service_ids, values_to_update)



    def get_columns(self) -> List[Text]:
        return ["id", "name", "active", "service_discovery", "carrier_service_type", "admin_graphql_api_id"]

    def get_carrier_service(self) -> List[Dict]:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)
        services = shopify.CarrierService.find()
        return [service.to_dict() for service in services]
    
    def create_carrier_service(self, carrier_service_data: List[Dict[Text, Any]]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for carrier_service in carrier_service_data:
            created_carrier_service = shopify.CarrierService.create(carrier_service)
            if 'id' not in created_carrier_service.to_dict():
                raise Exception('Product creation failed')
            else:
                logger.info(f'Product {created_carrier_service.to_dict()["id"]} created')

    def delete_carrier_services(self, carrier_service_ids: List[int]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for carrier_service_id in carrier_service_ids:
            product = shopify.CarrierService.find(carrier_service_id)
            product.destroy()
            logger.info(f'Carrier Service {carrier_service_id} deleted')

    def update_carrier_service(self, carrier_service_id: int, values_to_update: Dict[Text, Any]) -> None:
        # Update the carrier service using the Shopify API
        session = self.handler.connect()
        shopify.ShopifyResource.activate_session(session)

        carrier_service = shopify.CarrierService.find(carrier_service_id)
        for key, value in values_to_update.items():
            setattr(carrier_service, key, value)
        carrier_service.save()
        logger.info(f'Carrier Service {carrier_service_id} updated')


class ShippingZoneTable(APITable):
    """The Shopify shipping zone Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /shipping_zone" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame

            Shopify Shipping Zone info

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        select_statement_parser = SELECTQueryParser(
            query,
            'shipping_zone',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        shipping_zone_df = pd.json_normalize(self.get_shipping_zone(), record_path="countries", meta=["id", "name"], record_prefix="countries_")


        select_statement_executor = SELECTQueryExecutor(
            shipping_zone_df,
            selected_columns,
            where_conditions,
            order_by_conditions,
            result_limit
        )
        
        shipping_zone_df = select_statement_executor.execute_query()

        return shipping_zone_df

    def get_columns(self) -> List[Text]:
        return ['countries_id', 'countries_name', 'countries_tax', 'countries_code', 'countries_tax_name', 'countries_shipping_zone_id', 'countries_provinces', 'id', 'name']

    def clean_response(self, res):
        temp = {}
        temp["id"] = res["id"]
        temp["name"] = res["name"]
        temp["countries"] = res["countries"]
        return temp

    def get_shipping_zone(self) -> List[Dict]:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)
        zones = shopify.ShippingZone.find()
        return [self.clean_response(zone.to_dict()) for zone in zones]


class SalesChannelTable(APITable):
    """The Shopify Sales Channel Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /publication API endpoint, as Channel API endpoint is deprecated
        

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
            query,
            'sales_channel',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        sales_channel_df = pd.json_normalize(self.get_sales_channel(limit=result_limit))

        select_statement_executor = SELECTQueryExecutor(
            sales_channel_df,
            selected_columns,
            where_conditions,
            order_by_conditions,
            result_limit
        )
        sales_channel_df = select_statement_executor.execute_query()
        return sales_channel_df


    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_sales_channel(limit=1)).columns.tolist()
    

    def get_sales_channel(self, **kwargs) -> List[Dict]:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)
        sales_channels = shopify.Publication.find(**kwargs)
        return  [sales_channel.to_dict() for  sales_channel in sales_channels]


class SmartCollectionsTable(APITable):
    """The Shopify Sales Channel Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /smart_collections API endpoint, as Channel API endpoint is deprecated
        

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify smart_collections matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'smart_collections',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        smart_collections_df = pd.json_normalize(self.get_smart_collections(limit=result_limit))

        select_statement_executor = SELECTQueryExecutor(
            smart_collections_df,
            selected_columns,
            where_conditions,
            order_by_conditions,
            result_limit
        )
        smart_collections_df = select_statement_executor.execute_query()
        return smart_collections_df
    
    def insert(self, query: ast.Insert) -> None:
        """Inserts data into the Shopify "POST /smart_collections" API endpoint.

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
            supported_columns=['title', 'body_html', 'handle', 'image', 'published_at', 'published_scope', 'rules', 'disjunctive', 'sort_order', 'template_suffix'],
            mandatory_columns=['title' ],
            all_mandatory=False
        )
        smart_collection_data = insert_statement_parser.parse_query()
        self.create_smart_collections(smart_collection_data)
    
    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from the Shopify "DELETE /smart_collections" API endpoint.

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

        smart_collections_df = pd.json_normalize(self.get_smart_collections())

        delete_query_executor = DELETEQueryExecutor(
            smart_collections_df,
            where_conditions
        )

        smart_collections_df = delete_query_executor.execute_query()

        smart_collections_ids = smart_collections_df['id'].tolist()
        self.delete_smart_collections(smart_collections_ids)

    def update(self, query: ast.Update) -> None:
        """Updates data from the Shopify "PUT /smart_collections" API endpoint.

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

        smart_collections_df = pd.json_normalize(self.get_smart_collections())
        update_query_executor = UPDATEQueryExecutor(
            smart_collections_df,
            where_conditions
        )

        smart_collections_df = update_query_executor.execute_query()
        smart_collections_ids = smart_collections_df['id'].tolist()
        self.update_smart_collections(smart_collections_ids, values_to_update)

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_smart_collections(limit=1)).columns.tolist()

    def get_smart_collections(self, **kwargs) -> List[Dict]:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)
        smart_collections = shopify.SmartCollection.find(**kwargs)
        if len(smart_collections) == 0:
            raise Exeption('Smart Collection query returned 0 results')
        return  [smart_collection.to_dict() for  smart_collection in smart_collections]

    def update_smart_collections(self, smart_collection_ids: List[int], values_to_update: Dict[Text, Any]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for smart_collection_id in smart_collection_ids:
            smart_collection = shopify.SmartCollection.find(smart_collection_id)
            for key, value in values_to_update.items():
                setattr(smart_collection, key, value)
            smart_collection.save()
            logger.info(f'Smart Collections {smart_collection_id} updated')

    def delete_smart_collections(self, smart_collection_ids: List[int]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for smart_collection_id in smart_collection_ids:
            smart_collection = shopify.SmartCollection.find(smart_collection_id)
            smart_collection.destroy()
            logger.info(f'Smart collection {smart_collection_id} deleted')

    def create_smart_collections(self, smart_collection_data: List[Dict[Text, Any]]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for smart_collection in smart_collection_data:
            created_smart_collection = shopify.SmartCollection.create(smart_collection)
            if 'id' not in created_smart_collection.to_dict():
                raise Exception('Smart collection creation failed')
            else:
                logger.info(f'Smart collection {created_smart_collection.to_dict()["id"]} created')


class CustomCollectionsTable(APITable):
    """The Shopify Sales Channel Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /custom_collections API endpoint, as Channel API endpoint is deprecated
        

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify custom_collections matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'custom_collections',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        custom_collections_df = pd.json_normalize(self.get_custom_collections(limit=result_limit))

        select_statement_executor = SELECTQueryExecutor(
            custom_collections_df,
            selected_columns,
            where_conditions,
            order_by_conditions,
            result_limit
        )
        custom_collections_df = select_statement_executor.execute_query()
        return custom_collections_df
    
    def insert(self, query: ast.Insert) -> None:
        """Inserts data into the Shopify "POST /custom_collections" API endpoint.

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
            supported_columns=['title', 'body_html', 'handle', 'image', 'published_at', 'published_scope', 'rules', 'disjunctive', 'sort_order', 'template_suffix'],
            mandatory_columns=['title' ],
            all_mandatory=False
        )
        custom_collection_data = insert_statement_parser.parse_query()
        self.create_custom_collections(custom_collection_data)
    
    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from the Shopify "DELETE /custom_collections" API endpoint.

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

        custom_collections_df = pd.json_normalize(self.get_custom_collections())

        delete_query_executor = DELETEQueryExecutor(
            custom_collections_df,
            where_conditions
        )

        custom_collections_df = delete_query_executor.execute_query()

        custom_collections_ids = custom_collections_df['id'].tolist()
        self.delete_custom_collections(custom_collections_ids)

    def update(self, query: ast.Update) -> None:
        """Updates data from the Shopify "PUT /custom_collections" API endpoint.

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

        custom_collections_df = pd.json_normalize(self.get_custom_collections())
        update_query_executor = UPDATEQueryExecutor(
            custom_collections_df,
            where_conditions
        )

        custom_collections_df = update_query_executor.execute_query()
        custom_collections_ids = custom_collections_df['id'].tolist()
        self.update_custom_collections(custom_collections_ids, values_to_update)

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_custom_collections(limit=1)).columns.tolist()

    def get_custom_collections(self, **kwargs) -> List[Dict]:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)
        custom_collections = shopify.CustomCollection.find(**kwargs)
        if len(custom_collections) == 0:
            raise Exeption('Custom Collection query returned 0 results')
        return  [custom_collection.to_dict() for  custom_collection in custom_collections]

    def update_custom_collections(self, custom_collection_ids: List[int], values_to_update: Dict[Text, Any]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for custom_collection_id in custom_collection_ids:
            custom_collection = shopify.CustomCollection.find(custom_collection_id)
            for key, value in values_to_update.items():
                setattr(custom_collection, key, value)
            custom_collection.save()
            logger.info(f'Custom Collections {custom_collection_id} updated')

    def delete_custom_collections(self, custom_collection_ids: List[int]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for custom_collection_id in custom_collection_ids:
            custom_collection = shopify.CustomCollection.find(custom_collection_id)
            custom_collection.destroy()
            logger.info(f'Custom collection {custom_collection_id} deleted')

    def create_custom_collections(self, custom_collection_data: List[Dict[Text, Any]]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for custom_collection in custom_collection_data:
            created_custom_collection = shopify.CustomCollection.create(custom_collection)
            if 'id' not in created_custom_collection.to_dict():
                raise Exception('Custom collection creation failed')
            else:
                logger.info(f'Custom collection {created_custom_collection.to_dict()["id"]} created')


class DraftOrdersTable(APITable):
    """The Shopify draft_orders Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /draft_orders" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify draft_orders matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        select_statement_parser = SELECTQueryParser(
            query,
            'draft_orders',
            self.get_columns()
        )
        selected_columns, where_conditions, draft_order_by_conditions, result_limit = select_statement_parser.parse_query()
        draft_orders_df = pd.json_normalize(self.get_draft_orders(limit=result_limit))

        select_statement_executor = SELECTQueryExecutor(
            draft_orders_df,
            selected_columns,
            where_conditions,
            draft_order_by_conditions
        )
        draft_orders_df = select_statement_executor.execute_query()

        return draft_orders_df
    
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
            supported_columns=['customer', 'shipping_address', 'billing_address', 'note', 'note_attributes', 'email', 'line_items', 'shipping_line', 'tags',
                               'tax_exempt', 'tax_exemptions', 'applied_discount'],
            mandatory_columns=['line_items' ],
            all_mandatory=False
        )
        order_data = insert_statement_parser.parse_query()
        self.create_draft_orders(order_data)
    
    def update(self, query: ast.Update) -> None:
        """Updates data in the Shopify "PUT /draft_orders" API endpoint.

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
        draft_orders_df = pd.json_normalize(self.get_draft_orders())

        update_statement_executor = UPDATEQueryExecutor(
            draft_orders_df,
            where_conditions
        )
        draft_orders_df = update_statement_executor.execute_query()
        draft_orders_ids = draft_orders_df['id'].tolist()
        self.update_draft_orders(draft_orders_ids, values_to_update)

    def delete(self, query: ast.Delete) -> None:
        """Deletes data from the Shopify "DELETE /draft_orders" API endpoint.

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

        draft_orders_df = pd.json_normalize(self.get_draft_orders())

        delete_query_executor = DELETEQueryExecutor(
            draft_orders_df,
            where_conditions
        )

        draft_orders_df = delete_query_executor.execute_query()

        draft_order_ids = draft_orders_df['id'].tolist()
        self.delete_draft_orders(draft_order_ids)

    def update_draft_orders(self, draft_order_ids: List[int], values_to_update: Dict[Text, Any]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for draft_order_id in draft_order_ids:
            draft_order = shopify.DraftOrder.find(draft_order_id)
            for key, value in values_to_update.items():
                setattr(draft_order, key, value)
            draft_order.save()
            logger.info(f'Draft_order {draft_order_id} updated')

    def delete_draft_orders(self, draft_order_ids: List[int]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for draft_order_id in draft_order_ids:
            draft_order = shopify.DraftOrder.find(draft_order_id)
            draft_order.destroy()
            logger.info(f'Draft_order {draft_order_id} deleted')
    
    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_draft_orders(limit=1)).columns.tolist()

    def get_draft_orders(self, **kwargs) -> List[Dict]:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)
        draft_orders = shopify.DraftOrder.find(**kwargs)
        if len(draft_orders) == 0:
            raise Exception('Draft Order query returned 0 results')
        return [draft_order.to_dict() for draft_order in draft_orders]

    def create_draft_orders(self, draft_order_data: List[Dict[Text, Any]]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for draft_order in draft_order_data:
            created_draft_order = shopify.DraftOrder.create(draft_order)
            if 'id' not in created_draft_order.to_dict():
                raise Exception('Order creation failed')
            else:
                logger.info(f'Order {created_draft_order.to_dict()["id"]} created')


class CheckoutTable(APITable):
    """The Shopify Sales Channel Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /Checkout API endpoint, as Channel API endpoint is deprecated
        

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
            query,
            'checkout',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        checkout_df = pd.json_normalize(self.get_checkout(limit=result_limit))

        select_statement_executor = SELECTQueryExecutor(
            checkout_df,
            selected_columns,
            where_conditions,
            order_by_conditions,
            result_limit
        )
        checkout_df = select_statement_executor.execute_query()
        return checkout_df


    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_checkout(limit=1)).columns.tolist()
    

    def get_checkout(self, **kwargs) -> List[Dict]:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)
        checkouts = shopify.Checkout.find(**kwargs)
        if len(checkouts) == 0:
            raise Exception('Checkout query returned 0 results')
        return  [checkout.to_dict() for  checkout in checkouts]


class PriceRuleTable(APITable):
    """The Shopify price_rules Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /price_rules" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify price_rules matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'price_rules',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        price_rules_df = pd.json_normalize(self.get_price_rules(limit=result_limit))

        select_statement_executor = SELECTQueryExecutor(
            price_rules_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        price_rules_df = select_statement_executor.execute_query()

        return price_rules_df
    
    def insert(self, query: ast.Insert) -> None:
        """Inserts data into the Shopify "POST /price_rules" API endpoint.

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
            supported_columns=['allocation_method', 'customer_selection', 'ends_at', 'entitled_collection_ids', 'entitled_country_ids', 'entitled_product_ids'
                               'entitled_variant_ids', 'once_per_customer', 'prerequisite_customer_ids', 'prerequisite_quantity_range', 'customer_segment_prerequisite_ids'
                               'prerequisite_shipping_price_range', 'prerequisite_subtotal_range', 'prerequisite_to_entitlement_purchase', 'starts_at', 'target_selection', 
                               'target_type', 'title', 'usage_limit', 'prerequisite_product_ids', 'prerequisite_variant_ids', 'prerequisite_collection_ids', 'value',
                               'value_type', 'prerequisite_to_entitlement_quantity_ratio', 'allocation_limit'],
            mandatory_columns=['title', 'value_type', 'value'],
            all_mandatory=False
        )
        price_rule_data = insert_statement_parser.parse_query()
        self.create_price_rules(price_rule_data)
    
    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from the Shopify "DELETE /price_rules" API endpoint.

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

        price_rules_df = pd.json_normalize(self.get_price_rules())

        delete_query_executor = DELETEQueryExecutor(
            price_rules_df,
            where_conditions
        )

        price_rules_df = delete_query_executor.execute_query()

        price_rule_ids = price_rules_df['id'].tolist()
        self.delete_price_rules(price_rule_ids)

    def update(self, query: ast.Update) -> None:
        """Updates data from the Shopify "PUT /price_rules" API endpoint.

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

        price_rules_df = pd.json_normalize(self.get_price_rules())
        update_query_executor = UPDATEQueryExecutor(
            price_rules_df,
            where_conditions
        )

        price_rules_df = update_query_executor.execute_query()
        price_rule_ids = price_rules_df['id'].tolist()
        self.update_price_rules(price_rule_ids, values_to_update)

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_price_rules(limit=1)).columns.tolist()

    def get_price_rules(self, **kwargs) -> List[Dict]:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)
        price_rules = shopify.PriceRule.find(**kwargs)
        if len(price_rules) == 0:
            raise Exception('Price_rules query returned 0 results')
        return [price_rule.to_dict() for price_rule in price_rules]

    def update_price_rules(self, price_rule_ids: List[int], values_to_update: Dict[Text, Any]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for price_rule_id in price_rule_ids:
            price_rule = shopify.PriceRule.find(price_rule_id)
            for key, value in values_to_update.items():
                setattr(price_rule, key, value)
            price_rule.save()
            logger.info(f'Price_rule {price_rule_id} updated')

    def delete_price_rules(self, price_rule_ids: List[int]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for price_rule_id in price_rule_ids:
            price_rule = shopify.PriceRule.find(price_rule_id)
            price_rule.destroy()
            logger.info(f'Price_rule {price_rule_id} deleted')

    def create_price_rules(self, price_rule_data: List[Dict[Text, Any]]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for price_rule in price_rule_data:
            created_price_rule = shopify.PriceRule.create(price_rule)
            if 'id' not in created_price_rule.to_dict():
                raise Exception('Price_rule creation failed')
            else:
                logger.info(f'Price_rule {created_price_rule.to_dict()["id"]} created')


class RefundsTable(APITable):
    """The Shopify refunds Table implementation"""

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
            query,
            'refunds',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        refunds_df = pd.json_normalize(self.get_refunds(limit=result_limit))

        select_statement_executor = SELECTQueryExecutor(
            refunds_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        refunds_df = select_statement_executor.execute_query()

        return refunds_df
    
    def insert(self, query: ast.Insert) -> None:
        """Inserts data into the Shopify "POST /refunds" API endpoint.

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
            supported_columns=['note', 'refund_duties', 'refund_line_items', 'transactions', 'transactions', 'order_id'],
            mandatory_columns=['order_id' ],
            all_mandatory=False
        )
        refund_data = insert_statement_parser.parse_query()
        self.create_refunds(refund_data)
    
    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_refunds()).columns.tolist()

    def get_refunds(self, **kwargs) -> List[Dict]:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)
        orders = shopify.Order.find(**kwargs, type="any")
        refunds_list = list()
        for order in orders:
            order = order.to_dict()
            order_id = order['id']
            refunds = shopify.Refund.find(order_id=order_id, **kwargs)
            refunds_list = refunds_list + [refund.to_dict() for refund in refunds]
        if len(refunds_list) == 0:
            raise Exception('Refunds query 0 results')
        return refunds_list

    def create_refunds(self, refund_data: List[Dict[Text, Any]]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for refund in refund_data:
            created_refund = shopify.Refund.create(refund)
            if 'id' not in created_refund.to_dict():
                raise Exception('Refund creation failed')
            else:
                logger.info(f'Refund {created_refund.to_dict()["id"]} created')


class DiscountCodesTable(APITable):
    """The Shopify discount_codes Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /discount_codes" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify discount_codes matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query, "discount_codes", self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = (
            select_statement_parser.parse_query()
        )

        discount_codes_df = pd.json_normalize(
            self.get_discount_codes(limit=result_limit)
        )

        select_statement_executor = SELECTQueryExecutor(
            discount_codes_df, selected_columns, where_conditions, order_by_conditions
        )
        discount_codes_df = select_statement_executor.execute_query()

        return discount_codes_df

    def insert(self, query: ast.Insert) -> None:
        """Inserts data into the Shopify "POST /discount_codes" API endpoint.

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
                "price_rule_id"
            ],
            mandatory_columns=["price_rule_id", "code"],
            all_mandatory=False,
        )
        discount_code_data = insert_statement_parser.parse_query()
        self.create_discount_codes(discount_code_data)

    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from the Shopify "DELETE /discount_codes" API endpoint.

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

        discount_codes_df = pd.json_normalize(self.get_discount_codes())

        delete_query_executor = DELETEQueryExecutor(discount_codes_df, where_conditions)

        discount_codes_df = delete_query_executor.execute_query()

        discount_code_ids = discount_codes_df["id"].tolist()
        price_rule_ids = discount_codes_df["price_rule_id"].tolist()
        self.delete_discount_codes(price_rule_ids, discount_code_ids)

    def update(self, query: ast.Update) -> None:
        """Updates data from the Shopify "PUT /discount_codes" API endpoint.

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

        discount_codes_df = pd.json_normalize(self.get_discount_codes())
        update_query_executor = UPDATEQueryExecutor(discount_codes_df, where_conditions)

        discount_codes_df = update_query_executor.execute_query()
        discount_code_ids = discount_codes_df["id"].tolist()
        price_rule_ids = discount_codes_df["price_rule_id"].tolist()
        self.update_discount_codes(price_rule_ids, discount_code_ids, values_to_update)

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_discount_codes()).columns.tolist()

    def get_discount_codes(self, **kwargs) -> List[Dict]:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)
        price_rules = shopify.PriceRule.find(**kwargs)
        discount_code_list = list()
        for price_rule in price_rules:
            price_rule = price_rule.to_dict()
            price_rule_id = price_rule["id"]
            discount_codes = shopify.DiscountCode.find(
                price_rule_id=price_rule_id, **kwargs
            )
            discount_code_list = discount_code_list + discount_codes
        if len(discount_code_list) == 0:
            raise Exception("Discount_code query 0 results")
        return [discount_code.to_dict() for discount_code in discount_code_list]

    def update_discount_codes(
        self,
        price_rule_ids: List[int],
        discount_code_ids: List[int],
        values_to_update: Dict[Text, Any],
    ) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for i in range(len(discount_code_ids)):
            discount_code = shopify.DiscountCode.find(
                discount_code_ids[i], price_rule_id=price_rule_ids[i]
            )
            for key, value in values_to_update.items():
                setattr(discount_code, key, value)
            discount_code.save()
            logger.info(f"Discount_code {discount_code_ids[i]} updated")

    def delete_discount_codes(
        self, price_rule_ids: List[int], discount_code_ids: List[int]
    ) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)
        number_of_deleted_items = len(discount_code_ids)
        for i in range(number_of_deleted_items):
            discount_code = shopify.DiscountCode.find(
                discount_code_ids[i], price_rule_id=price_rule_ids[i]
            )
            discount_code.destroy()
            logger.info(f"Discount_code {discount_code_ids[i]} deleted")

    def create_discount_codes(self, discount_code_data: List[Dict[Text, Any]]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for discount_code in discount_code_data:
            created_discount_code = shopify.DiscountCode.create(discount_code)
            if "id" not in created_discount_code.to_dict():
                raise Exception("Discount_code creation failed")
            else:
                logger.info(
                    f'Discount_code {created_discount_code.to_dict()["id"]} created'
                )


class MarketingEventTable(APITable):
    """The Shopify marketing_events Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /marketing_events" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify marketing_events matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'marketing_events',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        marketing_events_df = pd.json_normalize(self.get_marketing_events(limit=result_limit))

        select_statement_executor = SELECTQueryExecutor(
            marketing_events_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        marketing_events_df = select_statement_executor.execute_query()

        return marketing_events_df
    
    def insert(self, query: ast.Insert) -> None:
        """Inserts data into the Shopify "POST /marketing_events" API endpoint.

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
            supported_columns=['remote_id', 'event_type', 'marketing_channel', 'paid', 'referring_domain', 'budget', 'currency', 'budget_type', 'started_at', 'scheduled_to_end_at', 'ended_at', 'UTM parameters', 'description', 'manage_url', 'preview_url', 'marketed_resources'],
            mandatory_columns=['event_type' , 'marketing_channel'],
            all_mandatory=False
        )
        marketing_event_data = insert_statement_parser.parse_query()
        self.create_marketing_events(marketing_event_data)
    
    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from the Shopify "DELETE /marketing_events" API endpoint.

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

        marketing_events_df = pd.json_normalize(self.get_marketing_events())

        delete_query_executor = DELETEQueryExecutor(
            marketing_events_df,
            where_conditions
        )

        marketing_events_df = delete_query_executor.execute_query()

        marketing_event_ids = marketing_events_df['id'].tolist()
        self.delete_marketing_events(marketing_event_ids)

    def update(self, query: ast.Update) -> None:
        """Updates data from the Shopify "PUT /marketing_events" API endpoint.

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

        marketing_events_df = pd.json_normalize(self.get_marketing_events())
        update_query_executor = UPDATEQueryExecutor(
            marketing_events_df,
            where_conditions
        )

        marketing_events_df = update_query_executor.execute_query()
        marketing_event_ids = marketing_events_df['id'].tolist()
        self.update_marketing_events(marketing_event_ids, values_to_update)

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_marketing_events(limit=1)).columns.tolist()

    def get_marketing_events(self, **kwargs) -> List[Dict]:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)
        marketing_events = shopify.MarketingEvent.find(**kwargs)
        if len(marketing_events) == 0:
            raise Exception('Marketing_events query returned 0 results')
        return [marketing_event.to_dict() for marketing_event in marketing_events]

    def update_marketing_events(self, marketing_event_ids: List[int], values_to_update: Dict[Text, Any]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for marketing_event_id in marketing_event_ids:
            marketing_event = shopify.MarketingEvent.find(marketing_event_id)
            for key, value in values_to_update.items():
                setattr(marketing_event, key, value)
            marketing_event.save()
            logger.info(f'marketing_event {marketing_event_id} updated')

    def delete_marketing_events(self, marketing_event_ids: List[int]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for marketing_event_id in marketing_event_ids:
            marketing_event = shopify.MarketingEvent.find(marketing_event_id)
            marketing_event.destroy()
            logger.info(f'marketing_event {marketing_event_id} deleted')

    def create_marketing_events(self, marketing_event_data: List[Dict[Text, Any]]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for marketing_event in marketing_event_data:
            created_marketing_event = shopify.MarketingEvent.create(marketing_event)
            if 'id' not in created_marketing_event.to_dict():
                raise Exception('marketing_event creation failed')
            else:
                logger.info(f'marketing_event {created_marketing_event.to_dict()["id"]} created')


class BlogTable(APITable):  
    """The Shopify blogs Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /blogs" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify blogs matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'blogs',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        blogs_df = pd.json_normalize(self.get_blogs(limit=result_limit))

        select_statement_executor = SELECTQueryExecutor(
            blogs_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        blogs_df = select_statement_executor.execute_query()

        return blogs_df
    
    def insert(self, query: ast.Insert) -> None:
        """Inserts data into the Shopify "POST /blogs" API endpoint.

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
            supported_columns=['commentable', 'feedburner', 'feedburner_location', 'handle', 'metafields', 'template_suffix', 'title'],
            mandatory_columns=['title'],
            all_mandatory=False
        )
        blog_data = insert_statement_parser.parse_query()
        self.create_blogs(blog_data)
    
    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from the Shopify "DELETE /blogs" API endpoint.

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

        blogs_df = pd.json_normalize(self.get_blogs())

        delete_query_executor = DELETEQueryExecutor(
            blogs_df,
            where_conditions
        )

        blogs_df = delete_query_executor.execute_query()

        blog_ids = blogs_df['id'].tolist()
        self.delete_blogs(blog_ids)

    def update(self, query: ast.Update) -> None:
        """Updates data from the Shopify "PUT /blogs" API endpoint.

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

        blogs_df = pd.json_normalize(self.get_blogs())
        update_query_executor = UPDATEQueryExecutor(
            blogs_df,
            where_conditions
        )

        blogs_df = update_query_executor.execute_query()
        blog_ids = blogs_df['id'].tolist()
        self.update_blogs(blog_ids, values_to_update)

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_blogs(limit=1)).columns.tolist()

    def get_blogs(self, **kwargs) -> List[Dict]:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)
        blogs = shopify.MarketingEvent.find(**kwargs)
        if len(blogs) == 0:
            raise Exception('blogs query returned 0 results')
        return [blog.to_dict() for blog in blogs]

    def update_blogs(self, blog_ids: List[int], values_to_update: Dict[Text, Any]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for blog_id in blog_ids:
            blog = shopify.MarketingEvent.find(blog_id)
            for key, value in values_to_update.items():
                setattr(blog, key, value)
            blog.save()
            logger.info(f'Blog {blog_id} updated')

    def delete_blogs(self, blog_ids: List[int]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for blog_id in blog_ids:
            blog = shopify.MarketingEvent.find(blog_id)
            blog.destroy()
            logger.info(f'Blog {blog_id} deleted')

    def create_blogs(self, blog_data: List[Dict[Text, Any]]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for blog in blog_data:
            created_blog = shopify.MarketingEvent.create(blog)
            if 'id' not in created_blog.to_dict():
                raise Exception('Blog creation failed')
            else:
                logger.info(f'Blog {created_blog.to_dict()["id"]} created')


class ThemeTable(APITable):  
    """The Shopify themes Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /themes" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify themes matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'themes',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        themes_df = pd.json_normalize(self.get_themes(limit=result_limit))

        select_statement_executor = SELECTQueryExecutor(
            themes_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        themes_df = select_statement_executor.execute_query()

        return themes_df
    
    def insert(self, query: ast.Insert) -> None:
        """Inserts data into the Shopify "POST /themes" API endpoint.

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
            supported_columns=['name', 'role', 'src'],
            mandatory_columns=['name'],
            all_mandatory=False
        )
        theme_data = insert_statement_parser.parse_query()
        self.create_themes(theme_data)
    
    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from the Shopify "DELETE /themes" API endpoint.

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

        themes_df = pd.json_normalize(self.get_themes())

        delete_query_executor = DELETEQueryExecutor(
            themes_df,
            where_conditions
        )

        themes_df = delete_query_executor.execute_query()

        theme_ids = themes_df['id'].tolist()
        self.delete_themes(theme_ids)

    def update(self, query: ast.Update) -> None:
        """Updates data from the Shopify "PUT /themes" API endpoint.

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

        themes_df = pd.json_normalize(self.get_themes())
        update_query_executor = UPDATEQueryExecutor(
            themes_df,
            where_conditions
        )

        themes_df = update_query_executor.execute_query()
        theme_ids = themes_df['id'].tolist()
        self.update_themes(theme_ids, values_to_update)

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_themes(limit=1)).columns.tolist()

    def get_themes(self, **kwargs) -> List[Dict]:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)
        themes = shopify.Theme.find(**kwargs)
        if len(themes) == 0:
            raise Exeption('Themes query returned 0 results')
        return [theme.to_dict() for theme in themes]

    def update_themes(self, theme_ids: List[int], values_to_update: Dict[Text, Any]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for theme_id in theme_ids:
            theme = shopify.Theme.find(theme_id)
            for key, value in values_to_update.items():
                setattr(theme, key, value)
            theme.save()
            logger.info(f'Theme {theme_id} updated')

    def delete_themes(self, theme_ids: List[int]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for theme_id in theme_ids:
            theme = shopify.Theme.find(theme_id)
            theme.destroy()
            logger.info(f'Theme {theme_id} deleted')

    def create_themes(self, theme_data: List[Dict[Text, Any]]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for theme in theme_data:
            created_theme = shopify.Theme.create(theme)
            if 'id' not in created_theme.to_dict():
                raise Exception('Theme creation failed')
            else:
                logger.info(f'Theme {created_theme.to_dict()["id"]} created')


class ArticleTable(APITable):
    """The Shopify articles Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /articles" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify articles matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query, "articles", self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = (
            select_statement_parser.parse_query()
        )

        articles_df = pd.json_normalize(
            self.get_articles(limit=result_limit)
        )

        select_statement_executor = SELECTQueryExecutor(
            articles_df, selected_columns, where_conditions, order_by_conditions
        )
        articles_df = select_statement_executor.execute_query()

        return articles_df

    def insert(self, query: ast.Insert) -> None:
        """Inserts data into the Shopify "POST /articles" API endpoint.

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
                "author",
                "blog_id",
                "body_html", 
                "handle",
                "image",
                "metafields",
                "published",
                "published_at",
                "summary_html",
                "template_suffix",
                "tags",
                "title"
            ],
            mandatory_columns=["blog_id", "code"],
            all_mandatory=False,
        )
        article_data = insert_statement_parser.parse_query()
        self.create_articles(article_data)

    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from the Shopify "DELETE /articles" API endpoint.

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

        articles_df = pd.json_normalize(self.get_articles())

        delete_query_executor = DELETEQueryExecutor(articles_df, where_conditions)

        articles_df = delete_query_executor.execute_query()

        article_ids = articles_df["id"].tolist()
        blog_ids = articles_df["blog_id"].tolist()
        self.delete_articles(blog_ids, article_ids)

    def update(self, query: ast.Update) -> None:
        """Updates data from the Shopify "PUT /articles" API endpoint.

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

        articles_df = pd.json_normalize(self.get_articles())
        update_query_executor = UPDATEQueryExecutor(articles_df, where_conditions)

        articles_df = update_query_executor.execute_query()
        article_ids = articles_df["id"].tolist()
        blog_ids = articles_df["blog_id"].tolist()
        self.update_articles(blog_ids, article_ids, values_to_update)

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_articles()).columns.tolist()

    def get_articles(self, **kwargs) -> List[Dict]:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)
        blogs = shopify.Blog.find(**kwargs)
        article_list = list()
        for blog in blogs:
            blog = blog.to_dict()
            blog_id = blog["id"]
            articles = shopify.Article.find(
                blog_id=blog_id, **kwargs
            )
            article_list = article_list + articles
        if len(article_list) == 0:
            logger.info("articles query returned 0 results")
        return [article.to_dict() for article in article_list]

    def update_articles(
        self,
        blog_ids: List[int],
        article_ids: List[int],
        values_to_update: Dict[Text, Any],
    ) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for i in range(len(article_ids)):
            article = shopify.Article.find(
                article_ids[i], blog_id=blog_ids[i]
            )
            for key, value in values_to_update.items():
                setattr(article, key, value)
            article.save()
            logger.info(f"article {article_ids[i]} updated")

    def delete_articles(
        self, blog_ids: List[int], article_ids: List[int]
    ) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)
        number_of_deleted_items = len(article_ids)
        for i in range(number_of_deleted_items):
            article = shopify.Article.find(
                article_ids[i], blog_id=blog_ids[i]
            )
            article.destroy()
            logger.info(f"article {article_ids[i]} deleted")

    def create_articles(self, article_data: List[Dict[Text, Any]]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for article in article_data:
            created_article = shopify.Article.create(article)
            if "id" not in created_article.to_dict():
                raise Exception("article creation failed")
            else:
                logger.info(
                    f'article {created_article.to_dict()["id"]} created'
                )


class CommentTable(APITable):  
    """The Shopify comments Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /comments" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify comments matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'comments',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        comments_df = pd.json_normalize(self.get_comments(limit=result_limit))

        select_statement_executor = SELECTQueryExecutor(
            comments_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        comments_df = select_statement_executor.execute_query()

        return comments_df
    
    def insert(self, query: ast.Insert) -> None:
        """Inserts data into the Shopify "POST /comments" API endpoint.

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
            supported_columns=['article_id', 'author', 'blog_id', 'body', 'body_html', 'email', 'ip', 'published_at', 'user_agent'],
            mandatory_columns=['body', 'author', 'email'],
            all_mandatory=False
        )
        comment_data = insert_statement_parser.parse_query()
        self.create_comments(comment_data)
    
    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from the Shopify "DELETE /comments" API endpoint.

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

        comments_df = pd.json_normalize(self.get_comments())

        delete_query_executor = DELETEQueryExecutor(
            comments_df,
            where_conditions
        )

        comments_df = delete_query_executor.execute_query()

        comment_ids = comments_df['id'].tolist()
        self.delete_comments(comment_ids)

    def update(self, query: ast.Update) -> None:
        """Updates data from the Shopify "PUT /comments" API endpoint.

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

        comments_df = pd.json_normalize(self.get_comments())
        update_query_executor = UPDATEQueryExecutor(
            comments_df,
            where_conditions
        )

        comments_df = update_query_executor.execute_query()
        comment_ids = comments_df['id'].tolist()
        self.update_comments(comment_ids, values_to_update)

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_comments(limit=1)).columns.tolist()

    def get_comments(self, **kwargs) -> List[Dict]:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)
        comments = shopify.Comment.find(**kwargs)
        if len(comments) == 0:
            raise Exception('Comments query returned 0 results')
        return [comment.to_dict() for comment in comments]

    def update_comments(self, comment_ids: List[int], values_to_update: Dict[Text, Any]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for comment_id in comment_ids:
            comment = shopify.Comment.find(comment_id)
            for key, value in values_to_update.items():
                setattr(comment, key, value)
            comment.save()
            logger.info(f'Comment {comment_id} updated')

    def delete_comments(self, comment_ids: List[int]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for comment_id in comment_ids:
            comment = shopify.Comment.find(comment_id)
            comment.destroy()
            logger.info(f'Comment {comment_id} deleted')

    def create_comments(self, comment_data: List[Dict[Text, Any]]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for comment in comment_data:
            created_comment = shopify.Comment.create(comment)
            if 'id' not in created_comment.to_dict():
                raise Exception('comment creation failed')
            else:
                logger.info(f'Comment {created_comment.to_dict()["id"]} created')


class PageTable(APITable):  
    """The Shopify pages Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /pages" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify pages matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'pages',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        pages_df = pd.json_normalize(self.get_pages(limit=result_limit))

        select_statement_executor = SELECTQueryExecutor(
            pages_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        pages_df = select_statement_executor.execute_query()

        return pages_df
    
    def insert(self, query: ast.Insert) -> None:
        """Inserts data into the Shopify "POST /pages" API endpoint.

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
            supported_columns=['author', 'body_html', 'handle', 'metafield', 'published_at', 'template_suffix', 'title'],
            mandatory_columns=['title'],
            all_mandatory=False
        )
        page_data = insert_statement_parser.parse_query()
        self.create_pages(page_data)
    
    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from the Shopify "DELETE /pages" API endpoint.

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

        pages_df = pd.json_normalize(self.get_pages())

        delete_query_executor = DELETEQueryExecutor(
            pages_df,
            where_conditions
        )

        pages_df = delete_query_executor.execute_query()

        page_ids = pages_df['id'].tolist()
        self.delete_pages(page_ids)

    def update(self, query: ast.Update) -> None:
        """Updates data from the Shopify "PUT /pages" API endpoint.

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

        pages_df = pd.json_normalize(self.get_pages())
        update_query_executor = UPDATEQueryExecutor(
            pages_df,
            where_conditions
        )

        pages_df = update_query_executor.execute_query()
        page_ids = pages_df['id'].tolist()
        self.update_pages(page_ids, values_to_update)

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_pages(limit=1)).columns.tolist()

    def get_pages(self, **kwargs) -> List[Dict]:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)
        pages = shopify.Page.find(**kwargs)
        if len(pages) == 0:
            raise Exception('Pages query returned 0 results')
        return [page.to_dict() for page in pages]

    def update_pages(self, page_ids: List[int], values_to_update: Dict[Text, Any]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for page_id in page_ids:
            page = shopify.Page.find(page_id)
            for key, value in values_to_update.items():
                setattr(page, key, value)
            page.save()
            logger.info(f'Page {page_id} updated')

    def delete_pages(self, page_ids: List[int]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for page_id in page_ids:
            page = shopify.Page.find(page_id)
            page.destroy()
            logger.info(f'Page {page_id} deleted')

    def create_pages(self, page_data: List[Dict[Text, Any]]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for page in page_data:
            created_page = shopify.Page.create(page)
            if 'id' not in created_page.to_dict():
                raise Exception('Page creation failed')
            else:
                logger.info(f'Page {created_page.to_dict()["id"]} created')


class redirectTable(APITable):  
    """The Shopify redirects Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /redirects" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify redirects matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'redirects',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        redirects_df = pd.json_normalize(self.get_redirects(limit=result_limit))

        select_statement_executor = SELECTQueryExecutor(
            redirects_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        redirects_df = select_statement_executor.execute_query()

        return redirects_df
    
    def insert(self, query: ast.Insert) -> None:
        """Inserts data into the Shopify "POST /redirects" API endpoint.

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
            supported_columns=['author', 'body_html', 'handle', 'metafield', 'published_at', 'template_suffix', 'title'],
            mandatory_columns=['title'],
            all_mandatory=False
        )
        redirect_data = insert_statement_parser.parse_query()
        self.create_redirects(redirect_data)
    
    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from the Shopify "DELETE /redirects" API endpoint.

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

        redirects_df = pd.json_normalize(self.get_redirects())

        delete_query_executor = DELETEQueryExecutor(
            redirects_df,
            where_conditions
        )

        redirects_df = delete_query_executor.execute_query()

        redirect_ids = redirects_df['id'].tolist()
        self.delete_redirects(redirect_ids)

    def update(self, query: ast.Update) -> None:
        """Updates data from the Shopify "PUT /redirects" API endpoint.

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

        redirects_df = pd.json_normalize(self.get_redirects())
        update_query_executor = UPDATEQueryExecutor(
            redirects_df,
            where_conditions
        )

        redirects_df = update_query_executor.execute_query()
        redirect_ids = redirects_df['id'].tolist()
        self.update_redirects(redirect_ids, values_to_update)

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_redirects(limit=1)).columns.tolist()

    def get_redirects(self, **kwargs) -> List[Dict]:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)
        redirects = shopify.Redirect.find(**kwargs)
        if len(redirects) == 0:
            raise Exception("Redirects query returned 0 results")
        return [redirect.to_dict() for redirect in redirects]

    def update_redirects(self, redirect_ids: List[int], values_to_update: Dict[Text, Any]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for redirect_id in redirect_ids:
            redirect = shopify.Redirect.find(redirect_id)
            for key, value in values_to_update.items():
                setattr(redirect, key, value)
            redirect.save()
            logger.info(f'Redirect {redirect_id} updated')

    def delete_redirects(self, redirect_ids: List[int]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for redirect_id in redirect_ids:
            redirect = shopify.Redirect.find(redirect_id)
            redirect.destroy()
            logger.info(f'Redirect {redirect_id} deleted')

    def create_redirects(self, redirect_data: List[Dict[Text, Any]]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for redirect in redirect_data:
            created_redirect = shopify.Redirect.create(redirect)
            if 'id' not in created_redirect.to_dict():
                raise Exception('Redirect creation failed')
            else:
                logger.info(f'Redirect {created_redirect.to_dict()["id"]} created')


class TenderTransactionTable(APITable):  
    """The Shopify tender_transactions Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /tender_transactions" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify tender_transactions matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'tender_transactions',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        tender_transactions_df = pd.json_normalize(self.get_tender_transactions(limit=result_limit))

        select_statement_executor = SELECTQueryExecutor(
            tender_transactions_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        tender_transactions_df = select_statement_executor.execute_query()

        return tender_transactions_df
    
    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_tender_transactions(limit=1)).columns.tolist()

    def get_tender_transactions(self, **kwargs) -> List[Dict]:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)
        tender_transactions = shopify.TenderTransaction.find(**kwargs)
        if len(tender_transactions) == 0:
            raise Exception('Tender_transactions query returned 0 results')
        return [tender_transaction.to_dict() for tender_transaction in tender_transactions]


class PolicyTable(APITable):  
    """The Shopify policies Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /policies" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify policies matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'policies',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        policies_df = pd.json_normalize(self.get_policies(limit=result_limit))

        select_statement_executor = SELECTQueryExecutor(
            policies_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        policies_df = select_statement_executor.execute_query()

        return policies_df
    
    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_policies(limit=1)).columns.tolist()

    def get_policies(self, **kwargs) -> List[Dict]:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)
        policies = shopify.Policy.find(**kwargs)
        if len(policies) == 0:
            raise Exception('Policies query returned 0 results')
        return [policy.to_dict() for policy in policies]


class ShopTable(APITable):  
    """The Shopify shops Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /shops" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify shops matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'shops',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        shops_df = pd.json_normalize(self.get_shops())

        select_statement_executor = SELECTQueryExecutor(
            shops_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        shops_df = select_statement_executor.execute_query()

        return shops_df
    
    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_shops()).columns.tolist()

    def get_shops(self, **kwargs) -> List[Dict]:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)
        shop = shopify.Shop.current(**kwargs)
        return [shop.to_dict()]


# class UserTable(APITable):  
#     """The userify users Table implementation"""

#     def select(self, query: ast.Select) -> pd.DataFrame:
#         """Pulls data from the userify "GET /users" API endpoint.

#         Parameters
#         ----------
#         query : ast.Select
#            Given SQL SELECT query

#         Returns
#         -------
#         pd.DataFrame
#             userify users matching the query

#         Raises
#         ------
#         ValueError
#             If the query contains an unsupported condition
#         """

#         select_statement_parser = SELECTQueryParser(
#             query,
#             'users',
#             self.get_columns()
#         )
#         selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

#         users_df = pd.json_normalize(self.get_users())

#         select_statement_executor = SELECTQueryExecutor(
#             users_df,
#             selected_columns,
#             where_conditions,
#             order_by_conditions
#         )
#         users_df = select_statement_executor.execute_query()

#         return users_df
    
#     def get_columns(self) -> List[Text]:
#         return pd.json_normalize(self.get_users()).columns.tolist()

#     def get_users(self, **kwargs) -> List[Dict]:
#         api_session = self.handler.connect()
#         shopify.ShopifyResource.activate_session(api_session)
#         users = shopify.User.current(**kwargs)
#         return [user.to_dict() for user in users]


# class GiftCardTable(APITable):
#     """The Shopify gift_cards Table implementation"""

#     def select(self, query: ast.Select) -> pd.DataFrame:
#         """Pulls data from the Shopify "GET /gift_cards" API endpoint.

#         Parameters
#         ----------
#         query : ast.Select
#            Given SQL SELECT query

#         Returns
#         -------
#         pd.DataFrame
#             Shopify gift_cards matching the query

#         Raises
#         ------
#         ValueError
#             If the query contains an unsupported condition
#         """

#         select_statement_parser = SELECTQueryParser(
#             query,
#             'gift_cards',
#             self.get_columns()
#         )
#         selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

#         gift_cards_df = pd.json_normalize(self.get_gift_cards(limit=result_limit))

#         select_statement_executor = SELECTQueryExecutor(
#             gift_cards_df,
#             selected_columns,
#             where_conditions,
#             order_by_conditions
#         )
#         gift_cards_df = select_statement_executor.execute_query()

#         return gift_cards_df
    
#     def insert(self, query: ast.Insert) -> None:
#         """Inserts data into the Shopify "POST /gift_cards" API endpoint.

#         Parameters
#         ----------
#         query : ast.Insert
#            Given SQL INSERT query

#         Returns
#         -------
#         None

#         Raises
#         ------
#         ValueError
#             If the query contains an unsupported condition
#         """
#         insert_statement_parser = INSERTQueryParser(
#             query,
#             supported_columns=['api_client_id', 'balance', 'code', 'created_at', 'currency', 'customer_id', 'disabled_at', 'expires_on', 'initial_value', 'last_characters', 'line_item_id'],
#             mandatory_columns=['code'],
#             all_mandatory=False
#         )
#         gift_card_data = insert_statement_parser.parse_query()
#         self.create_gift_cards(gift_card_data)
    
#     def delete(self, query: ast.Delete) -> None:
#         """
#         Deletes data from the Shopify "DELETE /gift_cards" API endpoint.

#         Parameters
#         ----------
#         3
#         query : ast.Delete
#            Given SQL DELETE query

#         Returns
#         -------
#         None

#         Raises
#         ------
#         ValueError
#             If the query contains an unsupported condition
#         """
#         delete_statement_parser = DELETEQueryParser(query)
#         where_conditions = delete_statement_parser.parse_query()

#         gift_cards_df = pd.json_normalize(self.get_gift_cards())

#         delete_query_executor = DELETEQueryExecutor(
#             gift_cards_df,
#             where_conditions
#         )

#         gift_cards_df = delete_query_executor.execute_query()

#         gift_card_ids = gift_cards_df['id'].tolist()
#         self.delete_gift_cards(gift_card_ids)

#     def update(self, query: ast.Update) -> None:
#         """Updates data from the Shopify "PUT /gift_cards" API endpoint.

#         Parameters
#         ----------
#         query : ast.Update
#            Given SQL UPDATE query

#         Returns
#         -------
#         None

#         Raises
#         ------
#         ValueError
#             If the query contains an unsupported condition
#         """
#         update_statement_parser = UPDATEQueryParser(query)
#         values_to_update, where_conditions = update_statement_parser.parse_query()

#         gift_cards_df = pd.json_normalize(self.get_gift_cards())
#         update_query_executor = UPDATEQueryExecutor(
#             gift_cards_df,
#             where_conditions
#         )

#         gift_cards_df = update_query_executor.execute_query()
#         gift_card_ids = gift_cards_df['id'].tolist()
#         self.update_gift_cards(gift_card_ids, values_to_update)

#     def get_columns(self) -> List[Text]:
#         return pd.json_normalize(self.get_gift_cards(limit=1)).columns.tolist()

#     def get_gift_cards(self, **kwargs) -> List[Dict]:
#         api_session = self.handler.connect()
#         shopify.ShopifyResource.activate_session(api_session)
#         gift_cards = shopify.GiftCard.find(**kwargs)
#         return [gift_card.to_dict() for gift_card in gift_cards]

#     def update_gift_cards(self, gift_card_ids: List[int], values_to_update: Dict[Text, Any]) -> None:
#         api_session = self.handler.connect()
#         shopify.ShopifyResource.activate_session(api_session)

#         for gift_card_id in gift_card_ids:
#             gift_card = shopify.GiftCard.find(gift_card_id)
#             for key, value in values_to_update.items():
#                 setattr(gift_card, key, value)
#             gift_card.save()
#             logger.info(f'gift_card {gift_card_id} updated')

#     def delete_gift_cards(self, gift_card_ids: List[int]) -> None:
#         api_session = self.handler.connect()
#         shopify.ShopifyResource.activate_session(api_session)

#         for gift_card_id in gift_card_ids:
#             gift_card = shopify.GiftCard.find(gift_card_id)
#             gift_card.destroy()
#             logger.info(f'gift_card {gift_card_id} deleted')

#     def create_gift_cards(self, gift_card_data: List[Dict[Text, Any]]) -> None:
#         api_session = self.handler.connect()
#         shopify.ShopifyResource.activate_session(api_session)

#         for gift_card in gift_card_data:
#             created_gift_card = shopify.GiftCard.create(gift_card)
#             if 'id' not in created_gift_card.to_dict():
#                 raise Exception('gift_card creation failed')
#             else:
#                 logger.info(f'gift_card {created_gift_card.to_dict()["id"]} created')


class ResourceFeedbackTable(APITable):
    """The
    Shopify resource_feedbacks Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /resource_feedbacks" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify resource_feedbacks matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'resource_feedbacks',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        resource_feedbacks_df = pd.json_normalize(self.get_resource_feedbacks(limit=result_limit))

        select_statement_executor = SELECTQueryExecutor(
            resource_feedbacks_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        resource_feedbacks_df = select_statement_executor.execute_query()

        return resource_feedbacks_df
    
    def insert(self, query: ast.Insert) -> None:
        """Inserts data into the Shopify "POST /resource_feedbacks" API endpoint.

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
            supported_columns=['state', 'messages', 'feedback_generated_at'],
            mandatory_columns=['state', 'messages', 'feedback_generated_at'],
            all_mandatory=False
        )
        resource_feedback_data = insert_statement_parser.parse_query()
        self.create_resource_feedbacks(resource_feedback_data)

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_resource_feedbacks(limit=1)).columns.tolist()

    def get_resource_feedbacks(self, **kwargs) -> List[Dict]:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)
        resource_feedbacks = shopify.ResourceFeedback.find(**kwargs)
        if not resource_feedbacks:
            raise Exception('Resource_feedbacks query returned 0 results')
        return [resource_feedback.to_dict() for resource_feedback in resource_feedbacks]

    def create_resource_feedbacks(self, resource_feedback_data: List[Dict[Text, Any]]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for resource_feedback in resource_feedback_data:
            created_resource_feedback = shopify.ResourceFeedback.create(resource_feedback)
            if 'id' not in created_resource_feedback.to_dict():
                raise Exception('Resource_feedback creation failed')
            else:
                logger.info(f'Resource_feedback {created_resource_feedback.to_dict()["id"]} created')


class OrderRiskTable(APITable):
    """The Shopify order_risks Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Shopify "GET /order_risks" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Shopify order_risks matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        select_statement_parser = SELECTQueryParser(
            query,
            'order_risks',
            self.get_columns()
        )
        selected_columns, where_conditions, order_risk_by_conditions, result_limit = select_statement_parser.parse_query()
        order_risks_df = pd.json_normalize(self.get_order_risks(limit=result_limit))

        select_statement_executor = SELECTQueryExecutor(
            order_risks_df,
            selected_columns,
            where_conditions,
            order_risk_by_conditions
        )
        order_risks_df = select_statement_executor.execute_query()

        return order_risks_df

    def insert(self, query: ast.Insert) -> None:
        """Inserts data into the Shopify "POST /order_risks" API endpoint.

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
            supported_columns=['cause_cancel', 'checkout_id', 'display', 'message', 'order_id', 'recommendation', 'score', 'source'],
            mandatory_columns=['order_id' ],
            all_mandatory=False
        )
        order_risk_data = insert_statement_parser.parse_query()
        self.create_order_risks(order_risk_data)
    
    def update(self, query: ast.Update) -> None:
        """Updates data in the Shopify "PUT /order_risks" API endpoint.

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
        order_risks_df = pd.json_normalize(self.get_order_risks())

        update_statement_executor = UPDATEQueryExecutor(
            order_risks_df,
            where_conditions
        )
        order_risks_df = update_statement_executor.execute_query()
        order_risks_ids = order_risks_df['id'].tolist()
        self.update_order_risks(order_risks_ids, values_to_update)

    def delete(self, query: ast.Delete) -> None:
        """Deletes data from the Shopify "DELETE /order_risks" API endpoint.

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

        order_risks_df = pd.json_normalize(self.get_order_risks())

        delete_query_executor = DELETEQueryExecutor(
            order_risks_df,
            where_conditions
        )

        order_risks_df = delete_query_executor.execute_query()

        order_risk_ids = order_risks_df['id'].tolist()
        self.delete_order_risks(order_risk_ids)

    def update_order_risks(self, order_risk_ids: List[int], values_to_update: Dict[Text, Any]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for order_risk_id in order_risk_ids:
            order_risk = shopify.OrderRisk.find(order_risk_id)
            for key, value in values_to_update.items():
                setattr(order_risk, key, value)
            order_risk.save()
            logger.info(f'Order risk {order_risk_id} updated')

    def delete_order_risks(self, order_risk_ids: List[int]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for order_risk_id in order_risk_ids:
            order_risk = shopify.OrderRisk.find(order_risk_id)
            order_risk.destroy()
            logger.info(f'Order risk {order_risk_id} deleted')
    
    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_order_risks(limit=1)).columns.tolist()

    def get_order_risks(self, **kwargs) -> List[Dict]:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)
        orders = shopify.Order.find(**kwargs, type="any")
        order_risks_list = list()
        for order in orders:
            order = order.to_dict()
            order_id = order['id']
            order_risks = shopify.OrderRisk.find(order_id=order_id, **kwargs)
            order_risks_list = order_risks_list + [order_risk.to_dict() for order_risk in order_risks]
        if len(order_risks_list) == 0:
            raise Exception('Order risk query returned 0 results')
        return order_risks_list

    def create_order_risks(self, order_risk_data: List[Dict[Text, Any]]) -> None:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        for order_risk in order_risk_data:
            created_order_risk = shopify.OrderRisk.create(order_risk)
            if 'id' not in created_order_risk.to_dict():
                raise Exception('Order risk creation failed')
            else:
                logger.info(f'Order risk {created_order_risk.to_dict()["id"]} created')

