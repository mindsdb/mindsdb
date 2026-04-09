from .common import AliasesEnum


class InventoryLevels(AliasesEnum):
    """Minimal AliasesEnum used as root_class for query_graphql_nodes.
    The actual GraphQL column string is built manually in InventoryLevelsTable.list()
    because the quantities field requires a `names` argument.
    The table's list() method flattens item/location/quantities into flat columns.
    """

    id = "id"


# Inventory levels use a custom list() implementation in shopify_tables.py
# because the quantities field requires a `names` argument.

INVENTORY_QUANTITY_NAMES = [
    "available",
    "committed",
    "damaged",
    "incoming",
    "on_hand",
    "quality_control",
    "reserved",
    "safety_stock",
]

columns = [
    {
        "TABLE_NAME": "inventory_levels",
        "COLUMN_NAME": "id",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A globally-unique ID.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "inventory_levels",
        "COLUMN_NAME": "inventoryItemId",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The ID of the inventory item.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "inventory_levels",
        "COLUMN_NAME": "sku",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The SKU of the inventory item.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "inventory_levels",
        "COLUMN_NAME": "locationId",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The ID of the location.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "inventory_levels",
        "COLUMN_NAME": "locationName",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The name of the location.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "inventory_levels",
        "COLUMN_NAME": "available",
        "DATA_TYPE": "INT",
        "COLUMN_DESCRIPTION": "Quantity available for sale.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "inventory_levels",
        "COLUMN_NAME": "committed",
        "DATA_TYPE": "INT",
        "COLUMN_DESCRIPTION": "Quantity committed to open orders.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "inventory_levels",
        "COLUMN_NAME": "damaged",
        "DATA_TYPE": "INT",
        "COLUMN_DESCRIPTION": "Quantity in damaged stock.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "inventory_levels",
        "COLUMN_NAME": "incoming",
        "DATA_TYPE": "INT",
        "COLUMN_DESCRIPTION": "Quantity incoming from purchase orders or transfers.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "inventory_levels",
        "COLUMN_NAME": "on_hand",
        "DATA_TYPE": "INT",
        "COLUMN_DESCRIPTION": "Total quantity on hand (available + committed + damaged + quality_control + reserved + safety_stock).",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "inventory_levels",
        "COLUMN_NAME": "quality_control",
        "DATA_TYPE": "INT",
        "COLUMN_DESCRIPTION": "Quantity in quality control.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "inventory_levels",
        "COLUMN_NAME": "reserved",
        "DATA_TYPE": "INT",
        "COLUMN_DESCRIPTION": "Quantity reserved.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "inventory_levels",
        "COLUMN_NAME": "safety_stock",
        "DATA_TYPE": "INT",
        "COLUMN_DESCRIPTION": "Quantity held as safety stock.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "inventory_levels",
        "COLUMN_NAME": "canDeactivate",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether the inventory level can be deactivated.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "inventory_levels",
        "COLUMN_NAME": "createdAt",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The date and time when the inventory level was created.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "inventory_levels",
        "COLUMN_NAME": "updatedAt",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The date and time when the inventory level was last updated.",
        "IS_NULLABLE": False,
    },
]
