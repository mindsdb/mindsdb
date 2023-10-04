import shopify
import pandas as pd
from typing import Text, List, Dict, Any

from mindsdb_sql.parser import ast
from mindsdb.integrations.libs.api_handler import APITable

from mindsdb.integrations.handlers.utilities.query_utilities import SELECTQueryParser, SELECTQueryExecutor
from mindsdb.integrations.handlers.utilities.query_utilities.insert_query_utilities import INSERTQueryParser

from mindsdb.utilities.log import get_log

logger = get_log("integrations.shopify_handler")


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

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_products(limit=1)).columns.tolist()

    def get_products(self, **kwargs) -> List[Dict]:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)
        products = shopify.Product.find(**kwargs)
        return [product.to_dict() for product in products]


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

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_orders(limit=1)).columns.tolist()

    def get_orders(self, **kwargs) -> List[Dict]:
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)
        orders = shopify.Order.find(**kwargs)
        return [order.to_dict() for order in orders]

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
