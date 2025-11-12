import shopify
import pandas as pd
from typing import List, Dict

from mindsdb.integrations.libs.api_handler import MetaAPIResource

from mindsdb.utilities import log
from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator, SortColumn

from .utils import query_graphql_nodes, get_graphql_columns, query_graphql
from .models.products import Products, columns as products_columns
from .models.customers import Customers, columns as customers_columns
from .models.orders import Orders, columns as orders_columns

logger = log.getLogger(__name__)


class ShopifyMetaAPIResource(MetaAPIResource):
    def list(
        self,
        conditions: list[FilterCondition] | None = None,
        limit: int | None = None,
        sort: list[SortColumn] | None = None,
        targets: list[str] | None = None,
        **kwargs,
    ):
        sort_key, sort_reverse = self._get_sort(sort)
        query_conditions = self._get_query_conditions(conditions)

        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        columns = get_graphql_columns(self.model, targets)
        data = query_graphql_nodes(
            self.model_name,
            self.model,
            columns,
            sort_key=sort_key,
            sort_reverse=sort_reverse,
            query=query_conditions,
            limit=limit,
        )

        if len(data) == 0:
            df_columns = targets
            if targets is None or len(targets) == 0:
                df_columns = list(self.model)
            products_df = pd.DataFrame(data, columns=df_columns)
        else:
            products_df = pd.DataFrame(data)

        return products_df

    def _get_sort(self, sort: List[SortColumn] | None) -> tuple[str, bool]:
        sort_key = None
        sort_reverse = None
        sort_map = self.sort_map or {}
        if sort:
            order_by = sort[0].column.lower()
            asc = sort[0].ascending
            if order_by not in sort_map:
                raise KeyError(f"Unsopported column for order by: {order_by}, available columns are: {list(self.sort_map.keys())}")

            sort_key = sort_map[order_by]
            sort_reverse = not asc
        return sort_key, sort_reverse

    def _get_query_conditions(self, conditions: List[FilterCondition] | None) -> str:
        query_conditions = []
        conditions_op_map = self.conditions_op_map or {}
        for condition in (conditions or []):
            op = condition.op
            column = condition.column.lower()
            mapped_op = conditions_op_map.get((column, op))
            if mapped_op:
                value = condition.value
                if isinstance(value, list):
                    value = ','.join(value)
                elif isinstance(value, bool):
                    value = f'{value}'.lower()
                query_conditions.append(f"{mapped_op}{value}")
                condition.applied = True
        query_conditions = ' AND '.join(query_conditions)
        return query_conditions


class ProductsTable(ShopifyMetaAPIResource):
    """The Shopify Products Table implementation"""
    # https://shopify.dev/docs/api/admin-graphql/latest/queries/products

    def __init__(self, *args, **kwargs):
        self.model = Products
        self.model_name = 'products'

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
        self.sort_map = {key.name.lower(): value for key, value in sort_map.items()}

        self.conditions_op_map = {
            ("createdat", FilterOperator.GREATER_THAN): "created_at:>",
            ("createdat", FilterOperator.GREATER_THAN_OR_EQUAL): "created_at:>=",
            ("createdat", FilterOperator.LESS_THAN): "created_at:<",
            ("createdat", FilterOperator.LESS_THAN_OR_EQUAL): "created_at:<=",
            ("createdat", FilterOperator.EQUAL): "created_at:",

            ("id", FilterOperator.GREATER_THAN): "id:>",
            ("id", FilterOperator.GREATER_THAN_OR_EQUAL): "id:>=",
            ("id", FilterOperator.LESS_THAN): "id:<",
            ("id", FilterOperator.LESS_THAN_OR_EQUAL): "id:<=",
            ("id", FilterOperator.EQUAL): "id:",

            ("isgiftcard", FilterOperator.EQUAL): "gift_card:",

            ("handle", FilterOperator.EQUAL): "handle:",
            ("handle", FilterOperator.IN): "handle:",

            ("totalinventory", FilterOperator.EQUAL): "inventory_total:",

            ("producttype", FilterOperator.EQUAL): "product_type:",
            ("producttype", FilterOperator.IN): "product_type:",

            ("publishedat", FilterOperator.GREATER_THAN): "published_at:>",
            ("publishedat", FilterOperator.GREATER_THAN_OR_EQUAL): "published_at:>=",
            ("publishedat", FilterOperator.LESS_THAN): "published_at:<",
            ("publishedat", FilterOperator.LESS_THAN_OR_EQUAL): "published_at:<=",
            ("publishedat", FilterOperator.EQUAL): "published_at:",

            ("status", FilterOperator.EQUAL): "status:",
            ("status", FilterOperator.IN): "status:",

            ("title", FilterOperator.EQUAL): "title:",

            ("updatedat", FilterOperator.GREATER_THAN): "updated_at:>",
            ("updatedat", FilterOperator.GREATER_THAN_OR_EQUAL): "updated_at:>=",
            ("updatedat", FilterOperator.LESS_THAN): "updated_at:<",
            ("updatedat", FilterOperator.LESS_THAN_OR_EQUAL): "updated_at:<=",
            ("updatedat", FilterOperator.EQUAL): "updated_at:",

            ("vendor", FilterOperator.EQUAL): "vendor:",
        }
        super().__init__(*args, **kwargs)

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

    def meta_get_primary_keys(self, table_name: str) -> list[Dict]:
        return [
            {
                "table_name": table_name,
                "column_name": "id",
            }
        ]

    def meta_get_foreign_keys(self, table_name: str, all_tables: list[str]) -> list[Dict]:
        return []

    # meta_get_column_statistics


class CustomersTable(ShopifyMetaAPIResource):
    """The Shopify Customers Table implementation"""

    def __init__(self, *args, **kwargs):
        self.model = Customers
        self.model_name = 'customers'

        sort_map = {
            Customers.createdAt: "CREATED_AT",
            Customers.id: "ID",
            Customers.updatedAt: "UPDATED_AT",
        }
        self.sort_map = {key.name.lower(): value for key, value in sort_map.items()}

        self.conditions_op_map = {
            ("country", FilterOperator.EQUAL): "country:",

            ("createdat", FilterOperator.GREATER_THAN): "customer_date:>",
            ("createdat", FilterOperator.GREATER_THAN_OR_EQUAL): "customer_date:>=",
            ("createdat", FilterOperator.LESS_THAN): "customer_date:<",
            ("createdat", FilterOperator.LESS_THAN_OR_EQUAL): "customer_date:<=",
            ("createdat", FilterOperator.EQUAL): "customer_date:",

            ("email", FilterOperator.EQUAL): "email:",

            ("firstname", FilterOperator.EQUAL): "first_name:",

            ("id", FilterOperator.GREATER_THAN): "id:>",
            ("id", FilterOperator.GREATER_THAN_OR_EQUAL): "id:>=",
            ("id", FilterOperator.LESS_THAN): "id:<",
            ("id", FilterOperator.LESS_THAN_OR_EQUAL): "id:<=",
            ("id", FilterOperator.EQUAL): "id:",

            ("lastname", FilterOperator.EQUAL): "last_name:",

            ("phonenumber", FilterOperator.EQUAL): "phone:",

            ("updatedat", FilterOperator.GREATER_THAN): "updated_at:>",
            ("updatedat", FilterOperator.GREATER_THAN_OR_EQUAL): "updated_at:>=",
            ("updatedat", FilterOperator.LESS_THAN): "updated_at:<",
            ("updatedat", FilterOperator.LESS_THAN_OR_EQUAL): "updated_at:<=",
            ("updatedat", FilterOperator.EQUAL): "updated_at:",
        }
        super().__init__(*args, **kwargs)

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
    # https://shopify.dev/docs/api/admin-graphql/latest/queries/orders


    def __init__(self, *args, **kwargs):
        self.model = Orders
        self.model_name = 'orders'

        sort_map = {
            Orders.createdAt: "CREATED_AT",
            Orders.id: "ID",
            Orders.number: "ORDER_NUMBER",
            Orders.poNumber: "PO_NUMBER",
            Orders.processedAt: "PROCESSED_AT",
            Orders.updatedAt: "UPDATED_AT",
        }
        self.sort_map = {key.name.lower(): value for key, value in sort_map.items()}

        self.conditions_op_map = {
            ("confirmationnumber", FilterOperator.EQUAL): "confirmation_number:",

            ("createdat", FilterOperator.GREATER_THAN): "created_at:>",
            ("createdat", FilterOperator.GREATER_THAN_OR_EQUAL): "created_at:>=",
            ("createdat", FilterOperator.LESS_THAN): "created_at:<",
            ("createdat", FilterOperator.LESS_THAN_OR_EQUAL): "created_at:<=",
            ("createdat", FilterOperator.EQUAL): "created_at:",

            ("customerid", FilterOperator.EQUAL): "customer_id:",

            ("discountcode", FilterOperator.EQUAL): "discount_code:",

            ("email", FilterOperator.EQUAL): "email:",

            ("id", FilterOperator.GREATER_THAN): "id:>",
            ("id", FilterOperator.GREATER_THAN_OR_EQUAL): "id:>=",
            ("id", FilterOperator.LESS_THAN): "id:<",
            ("id", FilterOperator.LESS_THAN_OR_EQUAL): "id:<=",
            ("id", FilterOperator.EQUAL): "id:",

            ("name", FilterOperator.EQUAL): "name:",

            ("ponumber", FilterOperator.EQUAL): "po_number:",

            ("processedat", FilterOperator.GREATER_THAN): "processed_at:>",
            ("processedat", FilterOperator.GREATER_THAN_OR_EQUAL): "processed_at:>=",
            ("processedat", FilterOperator.LESS_THAN): "processed_at:<",
            ("processedat", FilterOperator.LESS_THAN_OR_EQUAL): "processed_at:<=",
            ("processedat", FilterOperator.EQUAL): "processed_at:",

            ("returnstatus", FilterOperator.EQUAL): "return_status:",

            ("sourceidentifier", FilterOperator.EQUAL): "source_identifier:",

            ("sourcename", FilterOperator.EQUAL): "source_name:",

            ("test", FilterOperator.EQUAL): "test:",

            ("totalweight", FilterOperator.GREATER_THAN): "total_weight:>",
            ("totalweight", FilterOperator.GREATER_THAN_OR_EQUAL): "total_weight:>=",
            ("totalweight", FilterOperator.LESS_THAN): "total_weight:<",
            ("totalweight", FilterOperator.LESS_THAN_OR_EQUAL): "total_weight:<=",
            ("totalweight", FilterOperator.EQUAL): "total_weight:",

            ("updatedat", FilterOperator.GREATER_THAN): "updated_at:>",
            ("updatedat", FilterOperator.GREATER_THAN_OR_EQUAL): "updated_at:>=",
            ("updatedat", FilterOperator.LESS_THAN): "updated_at:<",
            ("updatedat", FilterOperator.LESS_THAN_OR_EQUAL): "updated_at:<=",
            ("updatedat", FilterOperator.EQUAL): "updated_at:",
        }
        super().__init__(*args, **kwargs)

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
