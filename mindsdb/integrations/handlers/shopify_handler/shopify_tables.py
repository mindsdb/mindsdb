import shopify
import pandas as pd
from typing import List, Dict

from mindsdb.integrations.libs.api_handler import MetaAPIResource
from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator, SortColumn
from mindsdb.utilities import log

from .utils import query_graphql_nodes, get_graphql_columns, query_graphql
from .models.products import Products, columns as products_columns
from .models.product_variants import ProductVariants, columns as product_variants_columns
from .models.customers import Customers, columns as customers_columns
from .models.orders import Orders, columns as orders_columns
from .models.marketing_events import MarketingEvents, columns as marketing_events_columns
from .models.inventory_items import InventoryItems, columns as inventory_items_columns
from .models.staff_members import StaffMembers, columns as staff_members_columns
from .models.gift_cards import GiftCards, columns as gift_cards_columns

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
        response = query_graphql("""{
            productVariantsCount(limit:null) {
                count
        }""")
        row_count = response["productVariantsCount"]["count"]

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
        response = query_graphql("""{
            giftCardsCount(limit:null) {
                count
        }""")
        row_count = response["giftCardsCount"]["count"]

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
