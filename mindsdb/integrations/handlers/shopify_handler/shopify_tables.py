import json
from typing import List, Dict

import shopify
import pandas as pd

from mindsdb.integrations.libs.api_handler import MetaAPIResource
from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator, SortColumn
from mindsdb.utilities import log

from .utils import query_graphql_nodes, get_graphql_columns, _format_error, ShopifyQuery, MAX_PAGE_LIMIT, PAGE_INFO
from .models.products import Products, columns as products_columns
from .models.product_variants import ProductVariants, columns as product_variants_columns
from .models.customers import Customers, columns as customers_columns
from .models.orders import Orders, columns as orders_columns
from .models.marketing_events import MarketingEvents, columns as marketing_events_columns
from .models.inventory_items import InventoryItems, columns as inventory_items_columns
from .models.staff_members import StaffMembers, columns as staff_members_columns
from .models.gift_cards import GiftCards, columns as gift_cards_columns
from .models.collections import Collections, columns as collections_columns
from .models.fulfillment_orders import FulfillmentOrders, columns as fulfillment_orders_columns
from .models.locations import Locations, columns as locations_columns
from .models.draft_orders import DraftOrders, columns as draft_orders_columns
from .models.inventory_levels import InventoryLevels, INVENTORY_QUANTITY_NAMES, columns as inventory_levels_columns
from .models.transactions import columns as transactions_columns
from .models.refunds import columns as refunds_columns
from .models.discount_codes import DiscountCodes, columns as discount_codes_columns
from .models.pages import Pages, columns as pages_columns
from .models.blogs import Blogs, columns as blogs_columns
from .models.articles import Articles, columns as articles_columns
from .models.shop import columns as shop_columns

logger = log.getLogger(__name__)


class ShopifyMetaAPIResource(MetaAPIResource):
    """A class to represent a Shopify Meta API resource."""

    def list(
        self,
        conditions: list[FilterCondition] | None = None,
        limit: int | None = None,
        sort: list[SortColumn] | None = None,
        targets: list[str] | None = None,
        **kwargs,
    ):
        """Query the Shopify API to get the resources data.

        Args:
            conditions: The conditions to apply to the query.
            limit: The limit of the resources to return.
            sort: The sort to apply to the query.
            targets: The columns to include in the query.

        Returns:
            pd.DataFrame: The data of the resources.
        """
        sort_key, sort_reverse = self._get_sort(sort)
        query_conditions = self._get_query_conditions(conditions)

        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        # region Validate that all requested target fields exist in the table schema
        if isinstance(targets, list):
            lower_names = [el[0].lower() for el in self.model.aliases()]
            missed_targets = [t for t in targets if t.lower() not in lower_names]
            if len(missed_targets) > 0:
                raise ValueError(
                    f"The specified fields were not found in the table schema: {', '.join(missed_targets)}"
                )
        # endregion

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
                df_columns = [column.name for column in self.model]
            products_df = pd.DataFrame(data, columns=df_columns)
        else:
            products_df = pd.DataFrame(data)

        return products_df

    def _get_sort(self, sort: List[SortColumn] | None) -> tuple[str, bool]:
        """Get the sort key and reverse from the sort list.

        Args:
            sort: The sort list.

        Returns:
            tuple[str, bool]: The sort key and reverse flag.
        """
        sort_key = None
        sort_reverse = None
        sort_map = self.sort_map or {}
        if sort:
            order_by = sort[0].column.lower()
            asc = sort[0].ascending
            if order_by not in sort_map:
                logger.info(
                    f"Used unsupported column for order by: {order_by}, available columns are: {list(self.sort_map.keys())}."
                )
                return None, None

            sort_key = sort_map[order_by]
            sort_reverse = not asc
            sort[0].applied = True
        return sort_key, sort_reverse

    def _get_query_conditions(self, conditions: List[FilterCondition] | None) -> str:
        """Get the GraphQL query conditions from the conditions list.

        Args:
            conditions: The conditions list.

        Returns:
            str: The query conditions.
        """
        query_conditions = []
        conditions_op_map = self.conditions_op_map or {}
        for condition in conditions or []:
            op = condition.op
            column = condition.column.lower()
            mapped_op = conditions_op_map.get((column, op))
            if mapped_op:
                value = condition.value
                if isinstance(value, list):
                    value = ",".join(value)
                elif isinstance(value, bool):
                    value = f"{value}".lower()
                query_conditions.append(f"{mapped_op}{value}")
                condition.applied = True
        query_conditions = " AND ".join(query_conditions)
        return query_conditions

    def get_columns(self) -> List[str]:
        """Get the table columns names.

        Returns:
            List[str]: The columns names.
        """
        return [column["COLUMN_NAME"] for column in self.columns]

    def meta_get_columns(self, *args, **kwargs) -> List[dict]:
        """Get the table columns metadata.

        Returns:
            List[dict]: The columns metadata.
        """
        return self.columns

    def query_graphql(self, query: str) -> dict:
        """Query the GraphQL API.

        Args:
            query: The GraphQL query to execute.

        Returns:
            dict: The result of the GraphQL query.
        """
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)
        result = shopify.GraphQL().execute(query)
        result = json.loads(result)
        if "errors" in result:
            raise Exception(_format_error(result["errors"]))
        return result


class ProductsTable(ShopifyMetaAPIResource):
    """The Shopify Products Table implementation
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/queries/products
    """

    def __init__(self, *args, **kwargs):
        self.name = "products"
        self.model = Products
        self.model_name = "products"
        self.columns = products_columns

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

    def meta_get_tables(self, *args, **kwargs) -> dict:
        response = self.query_graphql("""{
            productsCount(limit:null) {
                count
        } }""")
        row_count = response["data"]["productsCount"]["count"]

        return {
            "table_name": self.name,
            "table_type": "BASE TABLE",
            "table_description": "List of products. Products are the items that merchants can sell in their store.",
            "row_count": row_count,
        }

    def meta_get_primary_keys(self, table_name: str) -> list[Dict]:
        return [
            {
                "table_name": table_name,
                "column_name": "id",
            }
        ]

    def meta_get_foreign_keys(self, table_name: str, all_tables: list[str]) -> list[Dict]:
        return []


class ProductVariantsTable(ShopifyMetaAPIResource):
    """The Shopify Product Variants Table implementation
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/queries/productvariants
    """

    def __init__(self, *args, **kwargs):
        self.name = "product_variants"
        self.model = ProductVariants
        self.model_name = "productVariants"
        self.columns = product_variants_columns

        sort_map = {
            ProductVariants.id: "ID",
            ProductVariants.inventoryQuantity: "INVENTORY_QUANTITY",
            ProductVariants.displayName: "NAME",
            ProductVariants.position: "POSITION",
            ProductVariants.sku: "SKU",
            ProductVariants.title: "TITLE",
        }
        self.sort_map = {key.name.lower(): value for key, value in sort_map.items()}

        self.conditions_op_map = {
            ("barcode", FilterOperator.EQUAL): "barcode:",
            ("id", FilterOperator.GREATER_THAN): "id:>",
            ("id", FilterOperator.GREATER_THAN_OR_EQUAL): "id:>=",
            ("id", FilterOperator.LESS_THAN): "id:<",
            ("id", FilterOperator.LESS_THAN_OR_EQUAL): "id:<=",
            ("id", FilterOperator.EQUAL): "id:",
            ("inventoryquantity", FilterOperator.GREATER_THAN): "inventoryquantity:>",
            ("inventoryquantity", FilterOperator.GREATER_THAN_OR_EQUAL): "inventoryquantity:>=",
            ("inventoryquantity", FilterOperator.LESS_THAN): "inventoryquantity:<",
            ("inventoryquantity", FilterOperator.LESS_THAN_OR_EQUAL): "inventoryquantity:<=",
            ("inventoryquantity", FilterOperator.EQUAL): "inventoryquantity:",
            ("productid", FilterOperator.GREATER_THAN): "product_id:>",
            ("productid", FilterOperator.GREATER_THAN_OR_EQUAL): "product_id:>=",
            ("productid", FilterOperator.LESS_THAN): "product_id:<",
            ("productid", FilterOperator.LESS_THAN_OR_EQUAL): "product_id:<=",
            ("productid", FilterOperator.EQUAL): "product_id:",
            ("productid", FilterOperator.IN): "toproduct_ids:",
            ("sku", FilterOperator.EQUAL): "sku:",
            ("title", FilterOperator.EQUAL): "title:",
            ("updatedat", FilterOperator.GREATER_THAN): "updated_at:>",
            ("updatedat", FilterOperator.GREATER_THAN_OR_EQUAL): "updated_at:>=",
            ("updatedat", FilterOperator.LESS_THAN): "updated_at:<",
            ("updatedat", FilterOperator.LESS_THAN_OR_EQUAL): "updated_at:<=",
            ("updatedat", FilterOperator.EQUAL): "updated_at:",
        }
        super().__init__(*args, **kwargs)

    def meta_get_tables(self, *args, **kwargs) -> dict:
        response = self.query_graphql("""{
            productVariantsCount(limit:null) {
                count
        } }""")
        row_count = response["data"]["productVariantsCount"]["count"]

        return {
            "table_name": self.name,
            "table_type": "BASE TABLE",
            "table_description": "List of product variants. A product variant is a specific version of a product that comes in more than one option, such as size or color.",
            "row_count": row_count,
        }

    def meta_get_primary_keys(self, table_name: str) -> list[Dict]:
        return [
            {
                "table_name": table_name,
                "column_name": "id",
            }
        ]

    def meta_get_foreign_keys(self, table_name: str, all_tables: list[str]) -> list[Dict]:
        return [
            {
                "PARENT_TABLE_NAME": table_name,
                "PARENT_COLUMN_NAME": "productId",
                "CHILD_TABLE_NAME": "products",
                "CHILD_COLUMN_NAME": "id",
            }
        ]


class CustomersTable(ShopifyMetaAPIResource):
    """The Shopify Customers Table implementation
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/queries/customers
    """

    def __init__(self, *args, **kwargs):
        self.name = "customers"
        self.model = Customers
        self.model_name = "customers"
        self.columns = customers_columns

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

    def meta_get_tables(self, *args, **kwargs) -> dict:
        response = self.query_graphql("""{
            customersCount(limit:null) {
                count
        } }""")
        row_count = response["data"]["customersCount"]["count"]

        return {
            "table_name": self.name,
            "table_type": "BASE TABLE",
            "table_description": "List of customers in your Shopify store, including key information such as name, email, location, and purchase history",
            "row_count": row_count,
        }

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
    """The Shopify Orders Table implementation
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/queries/orders
    """

    def __init__(self, *args, **kwargs):
        self.name = "orders"
        self.model = Orders
        self.model_name = "orders"
        self.columns = orders_columns

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

    def meta_get_tables(self, *args, **kwargs) -> dict:
        response = self.query_graphql("""{
            ordersCount(limit:null) {
                count
        } }""")
        row_count = response["data"]["ordersCount"]["count"]

        return {
            "table_name": self.name,
            "table_type": "BASE TABLE",
            "table_description": "List of orders placed in the store, including data such as order status, customer, and line item details.",
            "row_count": row_count,
        }

    def meta_get_primary_keys(self, table_name: str) -> List[Dict]:
        return [
            {
                "table_name": table_name,
                "column_name": "id",
            }
        ]

    def meta_get_foreign_keys(self, table_name: str, all_tables: List[str]) -> List[Dict]:
        return [
            {
                "PARENT_TABLE_NAME": table_name,
                "PARENT_COLUMN_NAME": "customerId",
                "CHILD_TABLE_NAME": "customers",
                "CHILD_COLUMN_NAME": "id",
            }
        ]


class MarketingEventsTable(ShopifyMetaAPIResource):
    """The Shopify MarketingEvents table implementation
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/queries/marketingevents
    """

    def __init__(self, *args, **kwargs):
        self.name = "marketing_events"
        self.model = MarketingEvents
        self.model_name = "marketingEvents"
        self.columns = marketing_events_columns

        sort_map = {
            MarketingEvents.id: "ID",
            MarketingEvents.startedAt: "STARTED_AT",
        }
        self.sort_map = {key.name.lower(): value for key, value in sort_map.items()}

        self.conditions_op_map = {
            ("id", FilterOperator.GREATER_THAN): "id:>",
            ("id", FilterOperator.GREATER_THAN_OR_EQUAL): "id:>=",
            ("id", FilterOperator.LESS_THAN): "id:<",
            ("id", FilterOperator.LESS_THAN_OR_EQUAL): "id:<=",
            ("id", FilterOperator.EQUAL): "id:",
            ("startedat", FilterOperator.GREATER_THAN): "started_at:>",
            ("startedat", FilterOperator.GREATER_THAN_OR_EQUAL): "started_at:>=",
            ("startedat", FilterOperator.LESS_THAN): "started_at:<",
            ("startedat", FilterOperator.LESS_THAN_OR_EQUAL): "started_at:<=",
            ("startedat", FilterOperator.EQUAL): "started_at:",
            ("description", FilterOperator.EQUAL): "description:",
            ("description", FilterOperator.LIKE): "description:",
            ("type", FilterOperator.EQUAL): "type:",
            ("type", FilterOperator.LIKE): "type:",
        }
        super().__init__(*args, **kwargs)

    def meta_get_tables(self, *args, **kwargs) -> dict:
        data = query_graphql_nodes(
            self.model_name,
            self.model,
            "id",
        )
        row_count = len(data)

        return {
            "table_name": self.name,
            "table_type": "BASE TABLE",
            "table_description": "A list of marketing events.",
            "row_count": row_count,
        }

    def meta_get_primary_keys(self, table_name: str) -> List[Dict]:
        return [
            {
                "table_name": table_name,
                "column_name": "id",
            }
        ]

    def meta_get_foreign_keys(self, table_name: str, all_tables: List[str]) -> List[Dict]:
        return []


class InventoryItemsTable(ShopifyMetaAPIResource):
    """The Shopify InventoryItems table implementation
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/queries/inventoryitems
    """

    def __init__(self, *args, **kwargs):
        self.name = "inventory_items"
        self.model = InventoryItems
        self.model_name = "inventoryItems"
        self.columns = inventory_items_columns

        self.sort_map = {}

        self.conditions_op_map = {
            ("id", FilterOperator.GREATER_THAN): "id:>",
            ("id", FilterOperator.GREATER_THAN_OR_EQUAL): "id:>=",
            ("id", FilterOperator.LESS_THAN): "id:<",
            ("id", FilterOperator.LESS_THAN_OR_EQUAL): "id:<=",
            ("id", FilterOperator.EQUAL): "id:",
            ("createdat", FilterOperator.GREATER_THAN): "created_at:>",
            ("createdat", FilterOperator.GREATER_THAN_OR_EQUAL): "created_at:>=",
            ("createdat", FilterOperator.LESS_THAN): "created_at:<",
            ("createdat", FilterOperator.LESS_THAN_OR_EQUAL): "created_at:<=",
            ("createdat", FilterOperator.EQUAL): "created_at:",
            ("sku", FilterOperator.EQUAL): "sku:",
            ("updatedat", FilterOperator.GREATER_THAN): "updated_at:>",
            ("updatedat", FilterOperator.GREATER_THAN_OR_EQUAL): "updated_at:>=",
            ("updatedat", FilterOperator.LESS_THAN): "updated_at:<",
            ("updatedat", FilterOperator.LESS_THAN_OR_EQUAL): "updated_at:<=",
            ("updatedat", FilterOperator.EQUAL): "updated_at:",
        }
        super().__init__(*args, **kwargs)

    def meta_get_tables(self, *args, **kwargs) -> dict:
        data = query_graphql_nodes(
            self.model_name,
            self.model,
            "id",
        )
        row_count = len(data)

        return {
            "table_name": self.name,
            "table_type": "BASE TABLE",
            "table_description": "A list of inventory items.",
            "row_count": row_count,
        }

    def meta_get_primary_keys(self, table_name: str) -> List[Dict]:
        return [
            {
                "table_name": table_name,
                "column_name": "id",
            }
        ]

    def meta_get_foreign_keys(self, table_name: str, all_tables: List[str]) -> List[Dict]:
        return []


class StaffMembersTable(ShopifyMetaAPIResource):
    """The Shopify StaffMembers table implementation
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/queries/staffmembers
    """

    def __init__(self, *args, **kwargs):
        self.name = "staff_members"
        self.model = StaffMembers
        self.model_name = "staffMembers"
        self.columns = staff_members_columns

        sort_map = {
            StaffMembers.id: "ID",
            StaffMembers.email: "EMAIL",
            StaffMembers.firstName: "FIRST_NAME",
            StaffMembers.lastName: "LAST_NAME",
        }
        self.sort_map = {key.name.lower(): value for key, value in sort_map.items()}

        self.conditions_op_map = {
            ("accounttype", FilterOperator.EQUAL): "account_type:",
            ("email", FilterOperator.EQUAL): "email:",
            ("firstname", FilterOperator.EQUAL): "first_name:",
            ("firstname", FilterOperator.LIKE): "first_name:",
            ("lastname", FilterOperator.EQUAL): "last_name:",
            ("lastname", FilterOperator.LIKE): "last_name:",
            ("id", FilterOperator.GREATER_THAN): "id:>",
            ("id", FilterOperator.GREATER_THAN_OR_EQUAL): "id:>=",
            ("id", FilterOperator.LESS_THAN): "id:<",
            ("id", FilterOperator.LESS_THAN_OR_EQUAL): "id:<=",
            ("id", FilterOperator.EQUAL): "id:",
        }
        super().__init__(*args, **kwargs)

    def meta_get_tables(self, *args, **kwargs) -> dict:
        data = query_graphql_nodes(
            self.model_name,
            self.model,
            "id",
        )
        row_count = len(data)

        return {
            "table_name": self.name,
            "table_type": "BASE TABLE",
            "table_description": "The shop staff members.",
            "row_count": row_count,
        }

    def meta_get_primary_keys(self, table_name: str) -> List[Dict]:
        return [
            {
                "table_name": table_name,
                "column_name": "id",
            }
        ]

    def meta_get_foreign_keys(self, table_name: str, all_tables: List[str]) -> List[Dict]:
        return []


class GiftCardsTable(ShopifyMetaAPIResource):
    """The Shopify GiftCards table implementation
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/queries/giftcards
    """

    def __init__(self, *args, **kwargs):
        self.name = "gift_cards"
        self.model = GiftCards
        self.model_name = "giftCards"
        self.columns = gift_cards_columns

        sort_map = {
            GiftCards.balance: "BALANCE",
            GiftCards.createdAt: "CREATED_AT",
            GiftCards.deactivatedAt: "DISABLED_AT",
            GiftCards.expiresOn: "EXPIRES_ON",
            GiftCards.id: "ID",
            GiftCards.initialValue: "INITIAL_VALUE",
            GiftCards.updatedAt: "UPDATED_AT",
        }
        self.sort_map = {key.name.lower(): value for key, value in sort_map.items()}

        self.conditions_op_map = {
            ("createdat", FilterOperator.GREATER_THAN): "created_at:>",
            ("createdat", FilterOperator.GREATER_THAN_OR_EQUAL): "created_at:>=",
            ("createdat", FilterOperator.LESS_THAN): "created_at:<",
            ("createdat", FilterOperator.LESS_THAN_OR_EQUAL): "created_at:<=",
            ("createdat", FilterOperator.EQUAL): "created_at:",
            ("expireson", FilterOperator.GREATER_THAN): "expires_on:>",
            ("expireson", FilterOperator.GREATER_THAN_OR_EQUAL): "expires_on:>=",
            ("expireson", FilterOperator.LESS_THAN): "expires_on:<",
            ("expireson", FilterOperator.LESS_THAN_OR_EQUAL): "expires_on:<=",
            ("expireson", FilterOperator.EQUAL): "expires_on:",
            ("id", FilterOperator.GREATER_THAN): "id:>",
            ("id", FilterOperator.GREATER_THAN_OR_EQUAL): "id:>=",
            ("id", FilterOperator.LESS_THAN): "id:<",
            ("id", FilterOperator.LESS_THAN_OR_EQUAL): "id:<=",
            ("id", FilterOperator.EQUAL): "id:",
        }
        super().__init__(*args, **kwargs)

    def meta_get_tables(self, *args, **kwargs) -> dict:
        response = self.query_graphql("""{
            giftCardsCount(limit:null) {
                count
        } }""")
        row_count = response["data"]["giftCardsCount"]["count"]

        return {
            "table_name": self.name,
            "table_type": "BASE TABLE",
            "table_description": "List of gift cards.",
            "row_count": row_count,
        }

    def meta_get_primary_keys(self, table_name: str) -> List[Dict]:
        return [
            {
                "table_name": table_name,
                "column_name": "id",
            }
        ]

    def meta_get_foreign_keys(self, table_name: str, all_tables: List[str]) -> List[Dict]:
        return [
            {
                "PARENT_TABLE_NAME": table_name,
                "PARENT_COLUMN_NAME": "customerId",
                "CHILD_TABLE_NAME": "customers",
                "CHILD_COLUMN_NAME": "id",
            },
            {
                "PARENT_TABLE_NAME": table_name,
                "PARENT_COLUMN_NAME": "orderId",
                "CHILD_TABLE_NAME": "orders",
                "CHILD_COLUMN_NAME": "id",
            },
        ]


class CollectionsTable(ShopifyMetaAPIResource):
    """The Shopify Collections Table implementation
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/queries/collections
    """

    def __init__(self, *args, **kwargs):
        self.name = "collections"
        self.model = Collections
        self.model_name = "collections"
        self.columns = collections_columns

        sort_map = {
            Collections.id: "ID",
            Collections.title: "TITLE",
            Collections.updatedAt: "UPDATED_AT",
        }
        self.sort_map = {key.name.lower(): value for key, value in sort_map.items()}

        self.conditions_op_map = {
            ("id", FilterOperator.EQUAL): "id:",
            ("id", FilterOperator.GREATER_THAN): "id:>",
            ("id", FilterOperator.GREATER_THAN_OR_EQUAL): "id:>=",
            ("id", FilterOperator.LESS_THAN): "id:<",
            ("id", FilterOperator.LESS_THAN_OR_EQUAL): "id:<=",
            ("title", FilterOperator.EQUAL): "title:",
            ("handle", FilterOperator.EQUAL): "handle:",
            ("updatedat", FilterOperator.GREATER_THAN): "updated_at:>",
            ("updatedat", FilterOperator.GREATER_THAN_OR_EQUAL): "updated_at:>=",
            ("updatedat", FilterOperator.LESS_THAN): "updated_at:<",
            ("updatedat", FilterOperator.LESS_THAN_OR_EQUAL): "updated_at:<=",
            ("updatedat", FilterOperator.EQUAL): "updated_at:",
        }
        super().__init__(*args, **kwargs)

    def meta_get_tables(self, *args, **kwargs) -> dict:
        response = self.query_graphql("""{
            collectionsCount(limit:null) {
                count
        } }""")
        row_count = response["data"]["collectionsCount"]["count"]

        return {
            "table_name": self.name,
            "table_type": "BASE TABLE",
            "table_description": "List of collections (product groupings) in the store.",
            "row_count": row_count,
        }

    def meta_get_primary_keys(self, table_name: str) -> List[Dict]:
        return [{"table_name": table_name, "column_name": "id"}]

    def meta_get_foreign_keys(self, table_name: str, all_tables: List[str]) -> List[Dict]:
        return []


class FulfillmentOrdersTable(ShopifyMetaAPIResource):
    """The Shopify FulfillmentOrders Table implementation
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/queries/fulfillmentorders
    """

    def __init__(self, *args, **kwargs):
        self.name = "fulfillment_orders"
        self.model = FulfillmentOrders
        self.model_name = "fulfillmentOrders"
        self.columns = fulfillment_orders_columns

        self.sort_map = {
            FulfillmentOrders.id.name.lower(): "ID",
        }

        self.conditions_op_map = {
            ("id", FilterOperator.EQUAL): "id:",
            ("id", FilterOperator.GREATER_THAN): "id:>",
            ("id", FilterOperator.GREATER_THAN_OR_EQUAL): "id:>=",
            ("id", FilterOperator.LESS_THAN): "id:<",
            ("id", FilterOperator.LESS_THAN_OR_EQUAL): "id:<=",
            ("status", FilterOperator.EQUAL): "status:",
            ("orderid", FilterOperator.EQUAL): "order_id:",
            ("updatedat", FilterOperator.GREATER_THAN): "updated_at:>",
            ("updatedat", FilterOperator.GREATER_THAN_OR_EQUAL): "updated_at:>=",
            ("updatedat", FilterOperator.LESS_THAN): "updated_at:<",
            ("updatedat", FilterOperator.LESS_THAN_OR_EQUAL): "updated_at:<=",
            ("updatedat", FilterOperator.EQUAL): "updated_at:",
        }
        super().__init__(*args, **kwargs)

    def meta_get_tables(self, *args, **kwargs) -> dict:
        return {
            "table_name": self.name,
            "table_type": "BASE TABLE",
            "table_description": "List of fulfillment orders, representing where and how an order's items will be fulfilled.",
            "row_count": None,
        }

    def meta_get_primary_keys(self, table_name: str) -> List[Dict]:
        return [{"table_name": table_name, "column_name": "id"}]

    def meta_get_foreign_keys(self, table_name: str, all_tables: List[str]) -> List[Dict]:
        return [
            {
                "PARENT_TABLE_NAME": table_name,
                "PARENT_COLUMN_NAME": "orderId",
                "CHILD_TABLE_NAME": "orders",
                "CHILD_COLUMN_NAME": "id",
            }
        ]


class LocationsTable(ShopifyMetaAPIResource):
    """The Shopify Locations Table implementation
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/queries/locations
    """

    def __init__(self, *args, **kwargs):
        self.name = "locations"
        self.model = Locations
        self.model_name = "locations"
        self.columns = locations_columns

        sort_map = {
            Locations.id: "ID",
            Locations.name: "NAME",
        }
        self.sort_map = {key.name.lower(): value for key, value in sort_map.items()}

        self.conditions_op_map = {
            ("id", FilterOperator.EQUAL): "id:",
            ("name", FilterOperator.EQUAL): "name:",
            ("name", FilterOperator.LIKE): "name:",
            ("isactive", FilterOperator.EQUAL): "active:",
        }
        super().__init__(*args, **kwargs)

    def meta_get_tables(self, *args, **kwargs) -> dict:
        return {
            "table_name": self.name,
            "table_type": "BASE TABLE",
            "table_description": "List of store locations (warehouses and retail stores).",
            "row_count": None,
        }

    def meta_get_primary_keys(self, table_name: str) -> List[Dict]:
        return [{"table_name": table_name, "column_name": "id"}]

    def meta_get_foreign_keys(self, table_name: str, all_tables: List[str]) -> List[Dict]:
        return []


class DraftOrdersTable(ShopifyMetaAPIResource):
    """The Shopify DraftOrders Table implementation
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/queries/draftorders
    """

    def __init__(self, *args, **kwargs):
        self.name = "draft_orders"
        self.model = DraftOrders
        self.model_name = "draftOrders"
        self.columns = draft_orders_columns

        sort_map = {
            DraftOrders.createdAt: "CREATED_AT",
            DraftOrders.id: "ID",
            DraftOrders.status: "STATUS",
            DraftOrders.updatedAt: "UPDATED_AT",
        }
        self.sort_map = {key.name.lower(): value for key, value in sort_map.items()}

        self.conditions_op_map = {
            ("id", FilterOperator.EQUAL): "id:",
            ("id", FilterOperator.GREATER_THAN): "id:>",
            ("id", FilterOperator.GREATER_THAN_OR_EQUAL): "id:>=",
            ("id", FilterOperator.LESS_THAN): "id:<",
            ("id", FilterOperator.LESS_THAN_OR_EQUAL): "id:<=",
            ("status", FilterOperator.EQUAL): "status:",
            ("email", FilterOperator.EQUAL): "email:",
            ("customerid", FilterOperator.EQUAL): "customer_id:",
            ("createdat", FilterOperator.GREATER_THAN): "created_at:>",
            ("createdat", FilterOperator.GREATER_THAN_OR_EQUAL): "created_at:>=",
            ("createdat", FilterOperator.LESS_THAN): "created_at:<",
            ("createdat", FilterOperator.LESS_THAN_OR_EQUAL): "created_at:<=",
            ("createdat", FilterOperator.EQUAL): "created_at:",
            ("updatedat", FilterOperator.GREATER_THAN): "updated_at:>",
            ("updatedat", FilterOperator.GREATER_THAN_OR_EQUAL): "updated_at:>=",
            ("updatedat", FilterOperator.LESS_THAN): "updated_at:<",
            ("updatedat", FilterOperator.LESS_THAN_OR_EQUAL): "updated_at:<=",
            ("updatedat", FilterOperator.EQUAL): "updated_at:",
        }
        super().__init__(*args, **kwargs)

    def meta_get_tables(self, *args, **kwargs) -> dict:
        response = self.query_graphql("""{
            draftOrdersCount(limit:null) {
                count
        } }""")
        row_count = response["data"]["draftOrdersCount"]["count"]

        return {
            "table_name": self.name,
            "table_type": "BASE TABLE",
            "table_description": "List of draft orders (incomplete orders and wholesale quotes).",
            "row_count": row_count,
        }

    def meta_get_primary_keys(self, table_name: str) -> List[Dict]:
        return [{"table_name": table_name, "column_name": "id"}]

    def meta_get_foreign_keys(self, table_name: str, all_tables: List[str]) -> List[Dict]:
        return [
            {
                "PARENT_TABLE_NAME": table_name,
                "PARENT_COLUMN_NAME": "customerId",
                "CHILD_TABLE_NAME": "customers",
                "CHILD_COLUMN_NAME": "id",
            }
        ]


class InventoryLevelsTable(ShopifyMetaAPIResource):
    """The Shopify InventoryLevels Table implementation.
    Inventory levels represent stock quantities per item per location.
    Uses a custom list() because the quantities field requires a `names` argument.
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/queries/inventorylevels
    """

    def __init__(self, *args, **kwargs):
        self.name = "inventory_levels"
        self.model = InventoryLevels
        self.model_name = "inventoryLevels"
        self.columns = inventory_levels_columns

        self.sort_map = {}

        self.conditions_op_map = {
            ("inventoryitemid", FilterOperator.EQUAL): "inventory_item_id:",
            ("locationid", FilterOperator.EQUAL): "location_id:",
            ("updatedat", FilterOperator.GREATER_THAN): "updated_at:>",
            ("updatedat", FilterOperator.GREATER_THAN_OR_EQUAL): "updated_at:>=",
            ("updatedat", FilterOperator.LESS_THAN): "updated_at:<",
            ("updatedat", FilterOperator.LESS_THAN_OR_EQUAL): "updated_at:<=",
            ("updatedat", FilterOperator.EQUAL): "updated_at:",
        }
        super().__init__(*args, **kwargs)

    def list(self, conditions=None, limit=None, sort=None, targets=None, **kwargs):
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        query_conditions = self._get_query_conditions(conditions)

        qty_names = ", ".join(f'"{n}"' for n in INVENTORY_QUANTITY_NAMES)
        columns_gql = (
            "id "
            "item { id sku legacyResourceId } "
            "location { id name } "
            "canDeactivate "
            "createdAt "
            "updatedAt "
            f"quantities(names: [{qty_names}]) {{ name quantity }}"
        )

        data = query_graphql_nodes(
            "inventoryLevels",
            InventoryLevels,
            columns_gql,
            query=query_conditions,
            limit=limit,
        )

        rows = []
        for row in data:
            flat = {
                "id": row.get("id"),
                "inventoryItemId": (row.get("item") or {}).get("id"),
                "sku": (row.get("item") or {}).get("sku"),
                "locationId": (row.get("location") or {}).get("id"),
                "locationName": (row.get("location") or {}).get("name"),
                "canDeactivate": row.get("canDeactivate"),
                "createdAt": row.get("createdAt"),
                "updatedAt": row.get("updatedAt"),
            }
            for qty in row.get("quantities") or []:
                flat[qty["name"]] = qty["quantity"]
            rows.append(flat)

        if not rows:
            return pd.DataFrame(columns=[c["COLUMN_NAME"] for c in self.columns])

        df = pd.DataFrame(rows)
        if targets:
            available = [c for c in targets if c in df.columns]
            if available:
                df = df[available]
        return df

    def meta_get_tables(self, *args, **kwargs) -> dict:
        return {
            "table_name": self.name,
            "table_type": "BASE TABLE",
            "table_description": "Inventory quantities per item per location.",
            "row_count": None,
        }

    def meta_get_primary_keys(self, table_name: str) -> List[Dict]:
        return [{"table_name": table_name, "column_name": "id"}]

    def meta_get_foreign_keys(self, table_name: str, all_tables: List[str]) -> List[Dict]:
        return [
            {
                "PARENT_TABLE_NAME": table_name,
                "PARENT_COLUMN_NAME": "inventoryItemId",
                "CHILD_TABLE_NAME": "inventory_items",
                "CHILD_COLUMN_NAME": "id",
            },
            {
                "PARENT_TABLE_NAME": table_name,
                "PARENT_COLUMN_NAME": "locationId",
                "CHILD_TABLE_NAME": "locations",
                "CHILD_COLUMN_NAME": "id",
            },
        ]


class TransactionsTable(ShopifyMetaAPIResource):
    """The Shopify Transactions Table.
    Transactions are fetched by querying orders with their nested transactions.
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/OrderTransaction
    """

    def __init__(self, *args, **kwargs):
        self.name = "transactions"
        self.model = None
        self.model_name = "transactions"
        self.columns = transactions_columns

        self.sort_map = {}
        self.conditions_op_map = {
            ("orderid", FilterOperator.EQUAL): "order_id_eq",
        }
        super().__init__(*args, **kwargs)

    def list(self, conditions=None, limit=None, sort=None, targets=None, **kwargs):
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        # Extract order_id filter if provided (used to scope the query)
        order_id_filter = None
        order_query = None
        for cond in conditions or []:
            if cond.column.lower() == "orderid" and cond.op == FilterOperator.EQUAL:
                order_id_filter = cond.value
                order_query = f"id:{order_id_filter}"
                cond.applied = True

        transactions_gql = (
            "id kind status "
            "amountSet { shopMoney { amount currencyCode } } "
            "createdAt processedAt gateway authorization formattedGateway test errorCode"
        )
        orders_columns_gql = f"id transactions {{ {transactions_gql} }}"

        data = query_graphql_nodes(
            "orders",
            Orders,
            orders_columns_gql,
            query=order_query,
            limit=None,
        )

        rows = []
        for order in data:
            order_id = order.get("id")
            for txn in order.get("transactions") or []:
                flat = {
                    "id": txn.get("id"),
                    "orderId": order_id,
                    "kind": txn.get("kind"),
                    "status": txn.get("status"),
                    "amount": ((txn.get("amountSet") or {}).get("shopMoney") or {}).get("amount"),
                    "currencyCode": ((txn.get("amountSet") or {}).get("shopMoney") or {}).get("currencyCode"),
                    "gateway": txn.get("gateway"),
                    "authorization": txn.get("authorization"),
                    "errorCode": txn.get("errorCode"),
                    "formattedGateway": txn.get("formattedGateway"),
                    "test": txn.get("test"),
                    "processedAt": txn.get("processedAt"),
                    "createdAt": txn.get("createdAt"),
                }
                rows.append(flat)

        if limit:
            rows = rows[:limit]

        if not rows:
            return pd.DataFrame(columns=[c["COLUMN_NAME"] for c in self.columns])

        df = pd.DataFrame(rows)
        if targets:
            available = [c for c in targets if c in df.columns]
            if available:
                df = df[available]
        return df

    def meta_get_tables(self, *args, **kwargs) -> dict:
        return {
            "table_name": self.name,
            "table_type": "BASE TABLE",
            "table_description": "Order payment transactions (sales, refunds, captures, voids).",
            "row_count": None,
        }

    def meta_get_primary_keys(self, table_name: str) -> List[Dict]:
        return [{"table_name": table_name, "column_name": "id"}]

    def meta_get_foreign_keys(self, table_name: str, all_tables: List[str]) -> List[Dict]:
        return [
            {
                "PARENT_TABLE_NAME": table_name,
                "PARENT_COLUMN_NAME": "orderId",
                "CHILD_TABLE_NAME": "orders",
                "CHILD_COLUMN_NAME": "id",
            }
        ]


class RefundsTable(ShopifyMetaAPIResource):
    """The Shopify Refunds Table.
    Refunds are fetched by querying orders with their nested refunds.
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/Refund
    """

    def __init__(self, *args, **kwargs):
        self.name = "refunds"
        self.model = None
        self.model_name = "refunds"
        self.columns = refunds_columns

        self.sort_map = {}
        self.conditions_op_map = {
            ("orderid", FilterOperator.EQUAL): "order_id_eq",
        }
        super().__init__(*args, **kwargs)

    def list(self, conditions=None, limit=None, sort=None, targets=None, **kwargs):
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        order_query = None
        for cond in conditions or []:
            if cond.column.lower() == "orderid" and cond.op == FilterOperator.EQUAL:
                order_query = f"id:{cond.value}"
                cond.applied = True

        refunds_gql = (
            "id note createdAt updatedAt "
            "totalRefundedSet { shopMoney { amount currencyCode } } "
            "refundLineItems(first: 50) { nodes { "
            "  quantity "
            "  priceSet { shopMoney { amount currencyCode } } "
            "  lineItem { id title } "
            "} }"
        )
        orders_columns_gql = f"id refunds {{ {refunds_gql} }}"

        data = query_graphql_nodes(
            "orders",
            Orders,
            orders_columns_gql,
            query=order_query,
            limit=None,
        )

        rows = []
        for order in data:
            order_id = order.get("id")
            for refund in order.get("refunds") or []:
                shop_money = ((refund.get("totalRefundedSet") or {}).get("shopMoney") or {})
                flat = {
                    "id": refund.get("id"),
                    "orderId": order_id,
                    "note": refund.get("note"),
                    "createdAt": refund.get("createdAt"),
                    "updatedAt": refund.get("updatedAt"),
                    "totalRefundedAmount": shop_money.get("amount"),
                    "totalRefundedCurrencyCode": shop_money.get("currencyCode"),
                    "refundLineItems": (refund.get("refundLineItems") or {}).get("nodes"),
                }
                rows.append(flat)

        if limit:
            rows = rows[:limit]

        if not rows:
            return pd.DataFrame(columns=[c["COLUMN_NAME"] for c in self.columns])

        df = pd.DataFrame(rows)
        if targets:
            available = [c for c in targets if c in df.columns]
            if available:
                df = df[available]
        return df

    def meta_get_tables(self, *args, **kwargs) -> dict:
        return {
            "table_name": self.name,
            "table_type": "BASE TABLE",
            "table_description": "Order refunds with line items and amounts.",
            "row_count": None,
        }

    def meta_get_primary_keys(self, table_name: str) -> List[Dict]:
        return [{"table_name": table_name, "column_name": "id"}]

    def meta_get_foreign_keys(self, table_name: str, all_tables: List[str]) -> List[Dict]:
        return [
            {
                "PARENT_TABLE_NAME": table_name,
                "PARENT_COLUMN_NAME": "orderId",
                "CHILD_TABLE_NAME": "orders",
                "CHILD_COLUMN_NAME": "id",
            }
        ]


class DiscountCodesTable(ShopifyMetaAPIResource):
    """The Shopify DiscountCodes Table implementation
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/queries/discountcodes
    """

    def __init__(self, *args, **kwargs):
        self.name = "discount_codes"
        self.model = DiscountCodes
        self.model_name = "discountCodes"
        self.columns = discount_codes_columns

        sort_map = {
            DiscountCodes.id: "ID",
            DiscountCodes.code: "CODE",
            DiscountCodes.createdAt: "CREATED_AT",
        }
        self.sort_map = {key.name.lower(): value for key, value in sort_map.items()}

        self.conditions_op_map = {
            ("code", FilterOperator.EQUAL): "code:",
            ("code", FilterOperator.LIKE): "code:",
            ("createdat", FilterOperator.GREATER_THAN): "created_at:>",
            ("createdat", FilterOperator.GREATER_THAN_OR_EQUAL): "created_at:>=",
            ("createdat", FilterOperator.LESS_THAN): "created_at:<",
            ("createdat", FilterOperator.LESS_THAN_OR_EQUAL): "created_at:<=",
            ("createdat", FilterOperator.EQUAL): "created_at:",
        }
        super().__init__(*args, **kwargs)

    def meta_get_tables(self, *args, **kwargs) -> dict:
        return {
            "table_name": self.name,
            "table_type": "BASE TABLE",
            "table_description": "List of discount codes (promo codes) that customers can apply at checkout.",
            "row_count": None,
        }

    def meta_get_primary_keys(self, table_name: str) -> List[Dict]:
        return [{"table_name": table_name, "column_name": "id"}]

    def meta_get_foreign_keys(self, table_name: str, all_tables: List[str]) -> List[Dict]:
        return []


class PagesTable(ShopifyMetaAPIResource):
    """The Shopify Pages Table implementation
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/queries/pages
    """

    def __init__(self, *args, **kwargs):
        self.name = "pages"
        self.model = Pages
        self.model_name = "pages"
        self.columns = pages_columns

        sort_map = {
            Pages.id: "ID",
            Pages.title: "TITLE",
            Pages.createdAt: "CREATED_AT",
            Pages.publishedAt: "PUBLISHED_AT",
            Pages.updatedAt: "UPDATED_AT",
        }
        self.sort_map = {key.name.lower(): value for key, value in sort_map.items()}

        self.conditions_op_map = {
            ("id", FilterOperator.EQUAL): "id:",
            ("title", FilterOperator.EQUAL): "title:",
            ("title", FilterOperator.LIKE): "title:",
            ("handle", FilterOperator.EQUAL): "handle:",
            ("ispublished", FilterOperator.EQUAL): "published_status:",
            ("updatedat", FilterOperator.GREATER_THAN): "updated_at:>",
            ("updatedat", FilterOperator.GREATER_THAN_OR_EQUAL): "updated_at:>=",
            ("updatedat", FilterOperator.LESS_THAN): "updated_at:<",
            ("updatedat", FilterOperator.LESS_THAN_OR_EQUAL): "updated_at:<=",
            ("updatedat", FilterOperator.EQUAL): "updated_at:",
        }
        super().__init__(*args, **kwargs)

    def meta_get_tables(self, *args, **kwargs) -> dict:
        return {
            "table_name": self.name,
            "table_type": "BASE TABLE",
            "table_description": "List of online store pages.",
            "row_count": None,
        }

    def meta_get_primary_keys(self, table_name: str) -> List[Dict]:
        return [{"table_name": table_name, "column_name": "id"}]

    def meta_get_foreign_keys(self, table_name: str, all_tables: List[str]) -> List[Dict]:
        return []


class BlogsTable(ShopifyMetaAPIResource):
    """The Shopify Blogs Table implementation
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/queries/blogs
    """

    def __init__(self, *args, **kwargs):
        self.name = "blogs"
        self.model = Blogs
        self.model_name = "blogs"
        self.columns = blogs_columns

        sort_map = {
            Blogs.id: "ID",
            Blogs.title: "TITLE",
        }
        self.sort_map = {key.name.lower(): value for key, value in sort_map.items()}

        self.conditions_op_map = {
            ("id", FilterOperator.EQUAL): "id:",
            ("title", FilterOperator.EQUAL): "title:",
            ("title", FilterOperator.LIKE): "title:",
            ("handle", FilterOperator.EQUAL): "handle:",
        }
        super().__init__(*args, **kwargs)

    def meta_get_tables(self, *args, **kwargs) -> dict:
        return {
            "table_name": self.name,
            "table_type": "BASE TABLE",
            "table_description": "List of store blogs.",
            "row_count": None,
        }

    def meta_get_primary_keys(self, table_name: str) -> List[Dict]:
        return [{"table_name": table_name, "column_name": "id"}]

    def meta_get_foreign_keys(self, table_name: str, all_tables: List[str]) -> List[Dict]:
        return []


class ArticlesTable(ShopifyMetaAPIResource):
    """The Shopify Articles Table implementation
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/queries/articles
    """

    def __init__(self, *args, **kwargs):
        self.name = "articles"
        self.model = Articles
        self.model_name = "articles"
        self.columns = articles_columns

        sort_map = {
            Articles.id: "ID",
            Articles.title: "TITLE",
            Articles.createdAt: "CREATED_AT",
            Articles.publishedAt: "PUBLISHED_AT",
            Articles.updatedAt: "UPDATED_AT",
        }
        self.sort_map = {key.name.lower(): value for key, value in sort_map.items()}

        self.conditions_op_map = {
            ("id", FilterOperator.EQUAL): "id:",
            ("title", FilterOperator.EQUAL): "title:",
            ("title", FilterOperator.LIKE): "title:",
            ("handle", FilterOperator.EQUAL): "handle:",
            ("blogid", FilterOperator.EQUAL): "blog_id:",
            ("ispublished", FilterOperator.EQUAL): "published_status:",
            ("updatedat", FilterOperator.GREATER_THAN): "updated_at:>",
            ("updatedat", FilterOperator.GREATER_THAN_OR_EQUAL): "updated_at:>=",
            ("updatedat", FilterOperator.LESS_THAN): "updated_at:<",
            ("updatedat", FilterOperator.LESS_THAN_OR_EQUAL): "updated_at:<=",
            ("updatedat", FilterOperator.EQUAL): "updated_at:",
        }
        super().__init__(*args, **kwargs)

    def meta_get_tables(self, *args, **kwargs) -> dict:
        return {
            "table_name": self.name,
            "table_type": "BASE TABLE",
            "table_description": "List of blog articles (posts).",
            "row_count": None,
        }

    def meta_get_primary_keys(self, table_name: str) -> List[Dict]:
        return [{"table_name": table_name, "column_name": "id"}]

    def meta_get_foreign_keys(self, table_name: str, all_tables: List[str]) -> List[Dict]:
        return [
            {
                "PARENT_TABLE_NAME": table_name,
                "PARENT_COLUMN_NAME": "blogId",
                "CHILD_TABLE_NAME": "blogs",
                "CHILD_COLUMN_NAME": "id",
            }
        ]


class ShopTable(ShopifyMetaAPIResource):
    """The Shopify Shop Table — a singleton resource with store configuration.
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/Shop
    """

    def __init__(self, *args, **kwargs):
        self.name = "shop"
        self.model = None
        self.model_name = "shop"
        self.columns = shop_columns

        self.sort_map = {}
        self.conditions_op_map = {}
        super().__init__(*args, **kwargs)

    def list(self, conditions=None, limit=None, sort=None, targets=None, **kwargs):
        api_session = self.handler.connect()
        shopify.ShopifyResource.activate_session(api_session)

        result = self.query_graphql("""{
            shop {
                id name email contactEmail myshopifyDomain
                primaryDomain { host url }
                plan { displayName partnerDevelopment shopifyPlus }
                currencyCode
                currencyFormats { moneyFormat moneyWithCurrencyFormat }
                timezoneAbbreviation timezoneOffset ianaTimezone
                description url createdAt updatedAt
            }
        }""")

        shop = result.get("data", {}).get("shop", {})
        if not shop:
            return pd.DataFrame(columns=[c["COLUMN_NAME"] for c in self.columns])

        row = {
            "id": shop.get("id"),
            "name": shop.get("name"),
            "email": shop.get("email"),
            "contactEmail": shop.get("contactEmail"),
            "myshopifyDomain": shop.get("myshopifyDomain"),
            "primaryDomain": shop.get("primaryDomain"),
            "plan": shop.get("plan"),
            "currencyCode": shop.get("currencyCode"),
            "currencyFormats": shop.get("currencyFormats"),
            "timezoneAbbreviation": shop.get("timezoneAbbreviation"),
            "timezoneOffset": shop.get("timezoneOffset"),
            "ianaTimezone": shop.get("ianaTimezone"),
            "description": shop.get("description"),
            "url": shop.get("url"),
            "createdAt": shop.get("createdAt"),
            "updatedAt": shop.get("updatedAt"),
        }

        df = pd.DataFrame([row])
        if targets:
            available = [c for c in targets if c in df.columns]
            if available:
                df = df[available]
        return df

    def meta_get_tables(self, *args, **kwargs) -> dict:
        return {
            "table_name": self.name,
            "table_type": "BASE TABLE",
            "table_description": "Store configuration and plan information (single row).",
            "row_count": 1,
        }

    def meta_get_primary_keys(self, table_name: str) -> List[Dict]:
        return [{"table_name": table_name, "column_name": "id"}]

    def meta_get_foreign_keys(self, table_name: str, all_tables: List[str]) -> List[Dict]:
        return []
