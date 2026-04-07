from .common import AliasesEnum


class LocationAddress(AliasesEnum):
    """A class to represent a Shopify location address.
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/LocationAddress
    """

    address1 = "address1"
    address2 = "address2"
    city = "city"
    country = "country"
    countryCode = "countryCode"
    phone = "phone"
    province = "province"
    provinceCode = "provinceCode"
    zip = "zip"


class Locations(AliasesEnum):
    """A class to represent a Shopify GraphQL location.
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/Location
    Require `read_inventory` permission.
    """

    activationDate = "activationDate"
    address = LocationAddress
    addressVerified = "addressVerified"
    deactivatedAt = "deactivatedAt"
    fulfillsOnlineOrders = "fulfillsOnlineOrders"
    id = "id"
    isActive = "isActive"
    isPrimary = "isPrimary"
    legacyResourceId = "legacyResourceId"
    name = "name"
    shipsInventory = "shipsInventory"


columns = [
    {
        "TABLE_NAME": "locations",
        "COLUMN_NAME": "activationDate",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The date and time the location was activated.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "locations",
        "COLUMN_NAME": "address",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The address of the location.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "locations",
        "COLUMN_NAME": "addressVerified",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether the location address is verified.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "locations",
        "COLUMN_NAME": "deactivatedAt",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The date and time the location was deactivated.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "locations",
        "COLUMN_NAME": "fulfillsOnlineOrders",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether this location can be reordered.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "locations",
        "COLUMN_NAME": "id",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A globally-unique ID.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "locations",
        "COLUMN_NAME": "isActive",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether the location is active.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "locations",
        "COLUMN_NAME": "isPrimary",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether the location is your primary location for shipping inventory.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "locations",
        "COLUMN_NAME": "legacyResourceId",
        "DATA_TYPE": "INT",
        "COLUMN_DESCRIPTION": "The ID of the corresponding resource in the REST Admin API.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "locations",
        "COLUMN_NAME": "name",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The name of the location.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "locations",
        "COLUMN_NAME": "shipsInventory",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether the location is used to calculate shipping rates.",
        "IS_NULLABLE": False,
    },
]
