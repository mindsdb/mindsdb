from .common import AliasesEnum


class CarrierServices(AliasesEnum):
    """A class to represent a Shopify GraphQL carrier service.
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/DeliveryCarrierService
    Require `read_shipping` permission.
    """

    active = "active"
    callbackUrl = "callbackUrl"
    id = "id"
    name = "name"
    supportsServiceDiscovery = "supportsServiceDiscovery"


columns = [
    {
        "TABLE_NAME": "carrier_services",
        "COLUMN_NAME": "active",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether the carrier service is active.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "carrier_services",
        "COLUMN_NAME": "callbackUrl",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The URL endpoint that Shopify sends POST requests to when retrieving shipping rates.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "carrier_services",
        "COLUMN_NAME": "id",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A globally-unique ID.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "carrier_services",
        "COLUMN_NAME": "name",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The name of the carrier service as seen by merchants and customers.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "carrier_services",
        "COLUMN_NAME": "supportsServiceDiscovery",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether merchants can send dummy labels to the carrier service.",
        "IS_NULLABLE": False,
    },
]
