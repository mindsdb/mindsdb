from .common import AliasesEnum, Count, MoneyV2


class Companies(AliasesEnum):
    """A class to represent a Shopify GraphQL company (B2B).
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/Company
    Require `read_companies` permission (Shopify Plus only).
    """

    contactsCount = Count
    createdAt = "createdAt"
    externalId = "externalId"
    id = "id"
    locationsCount = Count
    name = "name"
    note = "note"
    ordersCount = Count
    totalSpent = MoneyV2
    updatedAt = "updatedAt"


columns = [
    {
        "TABLE_NAME": "companies",
        "COLUMN_NAME": "contactsCount",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The number of contacts for the company.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "companies",
        "COLUMN_NAME": "createdAt",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The date and time when the company was created.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "companies",
        "COLUMN_NAME": "externalId",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A unique externally-supplied ID for the company.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "companies",
        "COLUMN_NAME": "id",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A globally-unique ID.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "companies",
        "COLUMN_NAME": "locationsCount",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The number of locations for the company.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "companies",
        "COLUMN_NAME": "name",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The name of the company.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "companies",
        "COLUMN_NAME": "note",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A note about the company.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "companies",
        "COLUMN_NAME": "ordersCount",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The number of orders placed for this company across all its locations.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "companies",
        "COLUMN_NAME": "totalSpent",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The total amount spent by this company across all its locations (amount and currencyCode).",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "companies",
        "COLUMN_NAME": "updatedAt",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The date and time when the company was last updated.",
        "IS_NULLABLE": False,
    },
]
