import json
import shopify
import requests
import pandas as pd
from typing import Text, List, Dict, Any, Set

from mindsdb_sql_parser import ast
from mindsdb.integrations.libs.api_handler import APITable, APIResource, MetaAPIResource

from mindsdb.integrations.utilities.handlers.query_utilities import SELECTQueryParser, SELECTQueryExecutor
from mindsdb.integrations.utilities.handlers.query_utilities import INSERTQueryParser
from mindsdb.integrations.utilities.handlers.query_utilities import DELETEQueryParser, DELETEQueryExecutor
from mindsdb.integrations.utilities.handlers.query_utilities import UPDATEQueryParser, UPDATEQueryExecutor
from mindsdb.utilities import log
from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator, SortColumn

from .utils import query_graphql_nodes, get_graphql_columns, query_graphql
from .models.products import Products, columns as products_columns
from .models.customers import Customers, columns as customers_columns
from .models.orders import Orders, columns as orders_columns

logger = log.getLogger(__name__)


class ShopifyMetaAPIResource(MetaAPIResource):
    def list(self, *args, **kwargs):
        return self.query(*args, **kwargs)


class ProductsTable(ShopifyMetaAPIResource):
    """The Shopify Products Table implementation"""

    def query(self, conditions, limit, sort, targets, **kwargs):
        sorter = {}
        if sort:
            order_by = sort[0].column.lower()
            asc = sort[0].ascending
            sort_map = {
                Products.createdAt: "CREATED_AT",
                Products.id: "ID",
                Products.totalInventory: "INVENTORY_TOTAL",
                Products.productType: "PRODUCT_TYPE",
                Products.publishedAt: "PUBLISHED_AT",
                Products.title: "TITLE",
                Products.updatedAt: "UPDATED_AT",
                Products.vendor: "VENDOR",
            }
            sort_map = {key.name.lower(): value for key, value in sort_map.items()}
            if order_by not in sort_map:
                raise KeyError(f"Unsopported column for order by: {order_by}, available columns are: {list(sort_map.keys())}")

            sorter = {
                "field": sort_map[order_by],
                "reverse": not asc
            }

        op_map = {
            ("createdat", FilterOperator.GREATER_THAN): "createdAt:>",
            ("createdat", FilterOperator.GREATER_THAN_OR_EQUAL): "createdAt:>=",
            ("createdat", FilterOperator.LESS_THAN): "createdAt:<",
            ("createdat", FilterOperator.LESS_THAN_OR_EQUAL): "createdAt:<=",
            ("createdat", FilterOperator.EQUAL): "createdAt:",

            ("id", FilterOperator.GREATER_THAN): "id:>",
            ("id", FilterOperator.GREATER_THAN_OR_EQUAL): "id:>=",
            ("id", FilterOperator.LESS_THAN): "id:<",
            ("id", FilterOperator.LESS_THAN_OR_EQUAL): "id:<=",
            ("id", FilterOperator.EQUAL): "id:",

            ("handle", FilterOperator.EQUAL): "handle:",
            ("handle", FilterOperator.IN): "handle:",

            ("status", FilterOperator.EQUAL): "status:",
            ("status", FilterOperator.IN): "status:",

            ("totalinventory", FilterOperator.EQUAL): "inventory_total:",
        }
        query_conditions = []
        for condition in conditions:
            op = condition.op
            column = condition.column.lower()
            mapped_op = op_map.get((column, op))
            if mapped_op:
                value = condition.value
                if isinstance(value, list):
                    value = ','.join(value)
                query_conditions.append(f"{mapped_op}{value}")
                condition.applied = True
        query_conditions = ' AND '.join(query_conditions)

        products_df = pd.DataFrame(
            self.get_products(limit=limit, sort=sorter, query=query_conditions)
        )
        return products_df

    def get_products(self, limit: int = None, sort: dict | None = None, query: str | None = None, **kwargs) -> List[Dict]:
        # https://shopify.dev/docs/api/admin-graphql/latest/queries/products

        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        columns = get_graphql_columns(Products)
        data = query_graphql_nodes('products', Products, columns, sort=sort, query=query)

        return data

    def get_columns(self) -> list[str]:
        return [column["COLUMN_NAME"] for column in products_columns]

    def meta_get_tables(self, *args, **kwargs) -> dict:
        response = query_graphql("""{
            productsCount(limit:null) {
                count
        }""")
        row_count = response["productsCount"]["count"]

        return {
            "table_name": self.name,
            "table_type": "BASE TABLE",
            "table_description": "List of products. Products are the items that merchants can sell in their store.",
            "row_count": row_count,
        }

    def meta_get_columns(self, *args, **kwargs):
        # parsed from https://shopify.dev/docs/api/admin-graphql/latest/objects/Product.md
        return products_columns

    def meta_get_primary_keys(self, table_name: str) -> List[Dict]:
        return [
            {
                "table_name": table_name,
                "column_name": "id",
            }
        ]

    def meta_get_foreign_keys(self, table_name: str, all_tables: List[str]) -> List[Dict]:
        return []

    # meta_get_column_statistics


class CustomersTable(ShopifyMetaAPIResource):
    """The Shopify Customers Table implementation"""

    def query(self, conditions, limit, sort, targets, **kwargs):
        products_df = pd.DataFrame(
            self.get_customers(limit=limit)
        )
        return products_df

    def get_customers(self, limit: int | None = None):
        # https://shopify.dev/docs/api/admin-graphql/latest/queries/customers

        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        columns = get_graphql_columns(Customers)
        data = query_graphql_nodes('customers', Customers, columns)

        return data

    # def get_columns(self) -> List[Text]:
    #     return pd.json_normalize(self.get_customers(limit=1)).columns.tolist()

    def get_columns(self) -> list[str]:
        return [column["COLUMN_NAME"] for column in customers_columns]

    def meta_get_tables(self, *args, **kwargs) -> dict:
        response = query_graphql("""{
            customersCount(limit:null) {
                count
        }""")
        row_count = response["customersCount"]["count"]

        return {
            "table_name": self.name,
            "table_type": "BASE TABLE",
            "table_description": "List of customers in your Shopify store, including key information such as name, email, location, and purchase history",
            "row_count": row_count,
        }

    def meta_get_columns(self, *args, **kwargs):
        return customers_columns

    def meta_get_primary_keys(self, table_name: str) -> List[Dict]:
        return [
            {
                "table_name": table_name,
                "column_name": "id",
            }
        ]

    def meta_get_foreign_keys(self, table_name: str, all_tables: List[str]) -> List[Dict]:
        return []


class OrdersTable(ShopifyMetaAPIResource):
    """The Shopify Orders Table implementation"""

    def query(self, conditions, limit, sort, targets, **kwargs):
        result = self.get_orders(limit=limit)
        if len(result) == 0:
            products_df = pd.DataFrame([], columns=[column.name for column in Orders])
        else:
            products_df = pd.DataFrame(result)
        return products_df

    def get_orders(self, limit: int | None = None):
        # https://shopify.dev/docs/api/admin-graphql/latest/queries/orders

        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        columns = get_graphql_columns(Orders)
        data = query_graphql_nodes('orders', Orders, columns)

        return data

    def get_columns(self) -> list[str]:
        return [column["COLUMN_NAME"] for column in orders_columns]

    def meta_get_tables(self, *args, **kwargs) -> dict:
        response = query_graphql("""{
            ordersCount(limit:null) {
                count
        }""")
        row_count = response["ordersCount"]["count"]

        return {
            "table_name": self.name,
            "table_type": "BASE TABLE",
            "table_description": "List of orders placed in the store, including data such as order status, customer, and line item details.",
            "row_count": row_count,
        }

    def meta_get_columns(self, *args, **kwargs):
        return orders_columns

    def meta_get_primary_keys(self, table_name: str) -> List[Dict]:
        return [
            {
                "table_name": table_name,
                "column_name": "id",
            }
        ]

    def meta_get_foreign_keys(self, table_name: str, all_tables: List[str]) -> List[Dict]:
        return [{
            "PARENT_TABLE_NAME": table_name,
            "PARENT_COLUMN_NAME": "customerId",
            "CHILD_TABLE_NAME": "customers",
            "CHILD_COLUMN_NAME": "id"
        }]
