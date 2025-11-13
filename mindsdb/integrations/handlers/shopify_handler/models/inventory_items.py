from .common import AliasesEnum, MoneyV2


class InventoryItems(AliasesEnum):
    """A class to represent a Shopify GraphQL inventory item.
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/InventoryItem
    Require `read_inventory` or `read_products` permission.
    """

    countryCodeOfOrigin = "countryCodeOfOrigin"
    # countryHarmonizedSystemCodes = "countryHarmonizedSystemCodes"
    createdAt = "createdAt"
    duplicateSkuCount = "duplicateSkuCount"
    harmonizedSystemCode = "harmonizedSystemCode"
    id = "id"
    inventoryHistoryUrl = "inventoryHistoryUrl"
    # inventoryLevel = "inventoryLevel"
    # inventoryLevels = "inventoryLevels"
    legacyResourceId = "legacyResourceId"
    # locationsCount = "locationsCount"
    # measurement = "measurement"
    provinceCodeOfOrigin = "provinceCodeOfOrigin"
    requiresShipping = "requiresShipping"
    sku = "sku"
    tracked = "tracked"
    # trackedEditable = "trackedEditable"
    unitCost = MoneyV2
    updatedAt = "updatedAt"
    # variant = "variant"


columns = [
    {
        "TABLE_NAME": "inventory_items",
        "COLUMN_NAME": "countryCodeOfOrigin",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The ISO 3166-1 alpha-2 country code of where the item originated from.",
        "IS_NULLABLE": None,
    },
    # {
    #     "TABLE_NAME": "inventory_items",
    #     "COLUMN_NAME": "countryHarmonizedSystemCodes",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A list of country specific harmonized system codes.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "inventory_items",
        "COLUMN_NAME": "createdAt",
        "DATA_TYPE": "TIMESTAMP",
        "COLUMN_DESCRIPTION": "The date and time when the inventory item was created.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "inventory_items",
        "COLUMN_NAME": "duplicateSkuCount",
        "DATA_TYPE": "INT",
        "COLUMN_DESCRIPTION": "The number of inventory items that share the same SKU with this item.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "inventory_items",
        "COLUMN_NAME": "harmonizedSystemCode",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The harmonized system code of the item. This must be a number between 6 and 13 digits.",
        "IS_NULLABLE": None,
    },
    {
        "TABLE_NAME": "inventory_items",
        "COLUMN_NAME": "id",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A globally-unique ID.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "inventory_items",
        "COLUMN_NAME": "inventoryHistoryUrl",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The URL that points to the inventory history for the item.",
        "IS_NULLABLE": None,
    },
    # {
    #     "TABLE_NAME": "inventory_items",
    #     "COLUMN_NAME": "inventoryLevel",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The inventory item's quantities at the specified location.",
    #     "IS_NULLABLE": None
    # },
    # {
    #     "TABLE_NAME": "inventory_items",
    #     "COLUMN_NAME": "inventoryLevels",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A list of the inventory item's quantities for each location that the inventory item can be stocked at.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "inventory_items",
        "COLUMN_NAME": "legacyResourceId",
        "DATA_TYPE": "INT",
        "COLUMN_DESCRIPTION": "The ID of the corresponding resource in the REST Admin API.",
        "IS_NULLABLE": False,
    },
    # {
    #     "TABLE_NAME": "inventory_items",
    #     "COLUMN_NAME": "locationsCount",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The number of locations where this inventory item is stocked.",
    #     "IS_NULLABLE": None
    # },
    # {
    #     "TABLE_NAME": "inventory_items",
    #     "COLUMN_NAME": "measurement",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The packaging dimensions of the inventory item.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "inventory_items",
        "COLUMN_NAME": "provinceCodeOfOrigin",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The ISO 3166-2 alpha-2 province code of where the item originated from.",
        "IS_NULLABLE": None,
    },
    {
        "TABLE_NAME": "inventory_items",
        "COLUMN_NAME": "requiresShipping",
        "DATA_TYPE": "BOOL",
        "COLUMN_DESCRIPTION": "Whether the inventory item requires shipping.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "inventory_items",
        "COLUMN_NAME": "sku",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "Inventory item SKU. Case-sensitive string.",
        "IS_NULLABLE": None,
    },
    {
        "TABLE_NAME": "inventory_items",
        "COLUMN_NAME": "tracked",
        "DATA_TYPE": "BOOL",
        "COLUMN_DESCRIPTION": "Whether inventory levels are tracked for the item.",
        "IS_NULLABLE": False,
    },
    # {
    #     "TABLE_NAME": "inventory_items",
    #     "COLUMN_NAME": "trackedEditable",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "Whether the value of the tracked field for the inventory item can be changed.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "inventory_items",
        "COLUMN_NAME": "unitCost",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "Unit cost associated with the inventory item. Note: the user must have View product costs permission granted in order to access this field once product granular permissions are enabled.",
        "IS_NULLABLE": None,
    },
    {
        "TABLE_NAME": "inventory_items",
        "COLUMN_NAME": "updatedAt",
        "DATA_TYPE": "TIMESTAMP",
        "COLUMN_DESCRIPTION": "The date and time when the inventory item was updated.",
        "IS_NULLABLE": False,
    },
    # {
    #     "TABLE_NAME": "inventory_items",
    #     "COLUMN_NAME": "variant",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The variant that owns this inventory item.",
    #     "IS_NULLABLE": False
    # }
]
