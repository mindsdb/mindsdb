from .common import AliasesEnum, Count


class DeliveryProfiles(AliasesEnum):
    """A class to represent a Shopify GraphQL delivery profile.
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/DeliveryProfile
    Require `read_shipping` permission.
    """

    activeMethodDefinitionsCount = "activeMethodDefinitionsCount"
    default = "default"
    id = "id"
    name = "name"
    productVariantsCount = Count
    zoneCountryCount = "zoneCountryCount"


columns = [
    {
        "TABLE_NAME": "delivery_profiles",
        "COLUMN_NAME": "activeMethodDefinitionsCount",
        "DATA_TYPE": "INT",
        "COLUMN_DESCRIPTION": "The number of active method definitions for the delivery profile.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "delivery_profiles",
        "COLUMN_NAME": "default",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether this is the default delivery profile.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "delivery_profiles",
        "COLUMN_NAME": "id",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A globally-unique ID.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "delivery_profiles",
        "COLUMN_NAME": "name",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The name of the delivery profile.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "delivery_profiles",
        "COLUMN_NAME": "productVariantsCount",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The number of product variants for the delivery profile.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "delivery_profiles",
        "COLUMN_NAME": "zoneCountryCount",
        "DATA_TYPE": "INT",
        "COLUMN_DESCRIPTION": "The number of countries within all zones for the delivery profile.",
        "IS_NULLABLE": False,
    },
]
