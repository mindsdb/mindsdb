from .common import AliasesEnum


class Markets(AliasesEnum):
    """A class to represent a Shopify GraphQL market.
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/Market
    Require `read_markets` permission.
    """

    enabled = "enabled"
    handle = "handle"
    id = "id"
    name = "name"
    primary = "primary"


columns = [
    {
        "TABLE_NAME": "markets",
        "COLUMN_NAME": "enabled",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether the market is enabled to receive visitors and sales.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "markets",
        "COLUMN_NAME": "handle",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A unique, human-readable identifier for the market.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "markets",
        "COLUMN_NAME": "id",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A globally-unique ID.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "markets",
        "COLUMN_NAME": "name",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The name of the market.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "markets",
        "COLUMN_NAME": "primary",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether the market is the primary market of the shop.",
        "IS_NULLABLE": False,
    },
]
