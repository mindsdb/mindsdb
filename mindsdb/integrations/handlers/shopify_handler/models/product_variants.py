from .common import (
    AliasesEnum,
    Count,
    MoneyV2,
)
from .utils import Extract


class ProductVariants(AliasesEnum):
    """A class to represent a Shopify GraphQL product variant.
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/ProductVariant
    Require `read_products` permission.
    """
    availableForSale = "availableForSale"
    barcode = "barcode"
    compareAtPrice = "compareAtPrice"
    # contextualPricing = "contextualPricing"
    createdAt = "createdAt"
    # defaultCursor = "defaultCursor"
    # deliveryProfile = "deliveryProfile"
    displayName = "displayName"
    # events = "events"
    id = "id"
    # inventoryItem = "inventoryItem"
    # inventoryPolicy = "inventoryPolicy"
    inventoryQuantity = "inventoryQuantity"
    # legacyResourceId = "legacyResourceId"
    # media = "media"
    # metafield = "metafield"
    # metafields = "metafields"
    position = "position"
    price = "price"
    # product = "product"
    productId = Extract("product", "id")  # Custom
    # productParents = "productParents"
    # productVariantComponents = "productVariantComponents"
    requiresComponents = "requiresComponents"
    # selectedOptions = "selectedOptions"
    sellableOnlineQuantity = "sellableOnlineQuantity"
    # sellingPlanGroups = "sellingPlanGroups"
    sellingPlanGroupsCount = Count
    showUnitPrice = "showUnitPrice"
    sku = "sku"
    taxable = "taxable"
    title = "title"
    # translations = "translations"
    unitPrice = MoneyV2
    # unitPriceMeasurement = "unitPriceMeasurement"
    updatedAt = "updatedAt"

columns = [
    {
        "TABLE_NAME": "product_variants",
        "COLUMN_NAME": "availableForSale",
        "DATA_TYPE": "BOOL",
        "COLUMN_DESCRIPTION": "Whether the product variant is available for sale.",
        "IS_NULLABLE": False
    },
    {
        "TABLE_NAME": "product_variants",
        "COLUMN_NAME": "barcode",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The value of the barcode associated with the product.",
        "IS_NULLABLE": None
    },
    {
        "TABLE_NAME": "product_variants",
        "COLUMN_NAME": "compareAtPrice",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The compare-at price of the variant in the default shop currency.",
        "IS_NULLABLE": None
    },
    # {
    #     "TABLE_NAME": "product_variants",
    #     "COLUMN_NAME": "contextualPricing",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The pricing that applies for a customer in a given context. As of API version 2025-04, only active markets are considered in the price resolution.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "product_variants",
        "COLUMN_NAME": "createdAt",
        "DATA_TYPE": "TIMESTAMP",
        "COLUMN_DESCRIPTION": "The date and time when the variant was created.",
        "IS_NULLABLE": False
    },
    # {
    #     "TABLE_NAME": "product_variants",
    #     "COLUMN_NAME": "defaultCursor",
    #     "DATA_TYPE": "TEXT",
    #     "COLUMN_DESCRIPTION": "A default cursor that returns the single next record, sorted ascending by ID.",
    #     "IS_NULLABLE": False
    # },
    # {
    #     "TABLE_NAME": "product_variants",
    #     "COLUMN_NAME": "deliveryProfile",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The delivery profile for the variant.",
    #     "IS_NULLABLE": None
    # },
    {
        "TABLE_NAME": "product_variants",
        "COLUMN_NAME": "displayName",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "Display name of the variant, based on product's title + variant's title.",
        "IS_NULLABLE": False
    },
    # {
    #     "TABLE_NAME": "product_variants",
    #     "COLUMN_NAME": "events",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The paginated list of events associated with the host subject.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "product_variants",
        "COLUMN_NAME": "id",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A globally-unique ID.",
        "IS_NULLABLE": False
    },
    # {
    #     "TABLE_NAME": "product_variants",
    #     "COLUMN_NAME": "inventoryItem",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The inventory item, which is used to query for inventory information.",
    #     "IS_NULLABLE": False
    # },
    # {
    #     "TABLE_NAME": "product_variants",
    #     "COLUMN_NAME": "inventoryPolicy",
    #     "DATA_TYPE": "TEXT",
    #     "COLUMN_DESCRIPTION": "Whether customers are allowed to place an order for the product variant when it's out of stock.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "product_variants",
        "COLUMN_NAME": "inventoryQuantity",
        "DATA_TYPE": "INT",
        "COLUMN_DESCRIPTION": "The total sellable quantity of the variant.",
        "IS_NULLABLE": None
    },
    # {
    #     "TABLE_NAME": "product_variants",
    #     "COLUMN_NAME": "legacyResourceId",
    #     "DATA_TYPE": "INT",
    #     "COLUMN_DESCRIPTION": "The ID of the corresponding resource in the REST Admin API.",
    #     "IS_NULLABLE": False
    # },
    # {
    #     "TABLE_NAME": "product_variants",
    #     "COLUMN_NAME": "media",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The media associated with the product variant.",
    #     "IS_NULLABLE": False
    # },
    # {
    #     "TABLE_NAME": "product_variants",
    #     "COLUMN_NAME": "metafield",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A custom field, including its namespace and key, that's associated with a Shopify resource for the purposes of adding and storing additional information.",
    #     "IS_NULLABLE": None
    # },
    # {
    #     "TABLE_NAME": "product_variants",
    #     "COLUMN_NAME": "metafields",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A list of custom fields that a merchant associates with a Shopify resource.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "product_variants",
        "COLUMN_NAME": "position",
        "DATA_TYPE": "INT",
        "COLUMN_DESCRIPTION": "The order of the product variant in the list of product variants. The first position in the list is 1.",
        "IS_NULLABLE": False
    },
    {
        "TABLE_NAME": "product_variants",
        "COLUMN_NAME": "price",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The price of the product variant in the default shop currency.",
        "IS_NULLABLE": False
    },
    # {
    #     "TABLE_NAME": "product_variants",
    #     "COLUMN_NAME": "product",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The product that this variant belongs to.",
    #     "IS_NULLABLE": False
    # },
    {
        # Custom, extracted from "product"
        "TABLE_NAME": "product_variants",
        "COLUMN_NAME": "productId",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "ID of the product that this variant belongs to.",
        "IS_NULLABLE": None
    },
    # {
    #     "TABLE_NAME": "product_variants",
    #     "COLUMN_NAME": "productParents",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A list of products that have product variants that contain this variant as a product component.",
    #     "IS_NULLABLE": False
    # },
    # {
    #     "TABLE_NAME": "product_variants",
    #     "COLUMN_NAME": "productVariantComponents",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A list of the product variant components.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "product_variants",
        "COLUMN_NAME": "requiresComponents",
        "DATA_TYPE": "BOOL",
        "COLUMN_DESCRIPTION": "Whether a product variant requires components. The default value is false. If true, then the product variant can only be purchased as a parent bundle with components and it will be omitted from channels that don't support bundles.",
        "IS_NULLABLE": False
    },
    # {
    #     "TABLE_NAME": "product_variants",
    #     "COLUMN_NAME": "selectedOptions",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "List of product options applied to the variant.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "product_variants",
        "COLUMN_NAME": "sellableOnlineQuantity",
        "DATA_TYPE": "INT",
        "COLUMN_DESCRIPTION": "The total sellable quantity of the variant for online channels. This doesn't represent the total available inventory or capture limitations based on customer location.",
        "IS_NULLABLE": False
    },
    # {
    #     "TABLE_NAME": "product_variants",
    #     "COLUMN_NAME": "sellingPlanGroups",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A list of all selling plan groups defined in the current shop associated with the product variant.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "product_variants",
        "COLUMN_NAME": "sellingPlanGroupsCount",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "Count of selling plan groups associated with the product variant.",
        "IS_NULLABLE": None
    },
    {
        "TABLE_NAME": "product_variants",
        "COLUMN_NAME": "showUnitPrice",
        "DATA_TYPE": "BOOL",
        "COLUMN_DESCRIPTION": "Whether to show the unit price for this product variant.",
        "IS_NULLABLE": False
    },
    {
        "TABLE_NAME": "product_variants",
        "COLUMN_NAME": "sku",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A case-sensitive identifier for the product variant in the shop. Required in order to connect to a fulfillment service.",
        "IS_NULLABLE": None
    },
    {
        "TABLE_NAME": "product_variants",
        "COLUMN_NAME": "taxable",
        "DATA_TYPE": "BOOL",
        "COLUMN_DESCRIPTION": "Whether a tax is charged when the product variant is sold.",
        "IS_NULLABLE": False
    },
    {
        "TABLE_NAME": "product_variants",
        "COLUMN_NAME": "title",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The title of the product variant.",
        "IS_NULLABLE": False
    },
    # {
    #     "TABLE_NAME": "product_variants",
    #     "COLUMN_NAME": "translations",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The published translations associated with the resource.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "product_variants",
        "COLUMN_NAME": "unitPrice",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The unit price value for the variant based on the variant measurement.",
        "IS_NULLABLE": None
    },
    # {
    #     "TABLE_NAME": "product_variants",
    #     "COLUMN_NAME": "unitPriceMeasurement",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The unit price measurement for the variant.",
    #     "IS_NULLABLE": None
    # },
    {
        "TABLE_NAME": "product_variants",
        "COLUMN_NAME": "updatedAt",
        "DATA_TYPE": "TIMESTAMP",
        "COLUMN_DESCRIPTION": "The date and time (ISO 8601 format) when the product variant was last modified.",
        "IS_NULLABLE": False
    }
]