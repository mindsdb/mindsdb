from .common import AliasesEnum
from .utils import Extract


class FulfillmentOrderAssignedLocation(AliasesEnum):
    """A class to represent the assigned location of a fulfillment order.
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/FulfillmentOrderAssignedLocation
    """

    address1 = "address1"
    address2 = "address2"
    city = "city"
    countryCode = "countryCode"
    name = "name"
    phone = "phone"
    province = "province"
    zip = "zip"


class FulfillmentOrders(AliasesEnum):
    """A class to represent a Shopify GraphQL fulfillment order.
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/FulfillmentOrder
    Require `read_fulfillments` permission.
    """

    assignedLocation = FulfillmentOrderAssignedLocation
    assignedLocationId = Extract("assignedLocation", "locationId")
    createdAt = "createdAt"
    displayStatus = "displayStatus"
    id = "id"
    orderId = Extract("order", "id")
    requestStatus = "requestStatus"
    status = "status"
    updatedAt = "updatedAt"


columns = [
    {
        "TABLE_NAME": "fulfillment_orders",
        "COLUMN_NAME": "assignedLocation",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The location assigned to fulfill the fulfillment order (address and name).",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "fulfillment_orders",
        "COLUMN_NAME": "assignedLocationId",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The ID of the assigned location.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "fulfillment_orders",
        "COLUMN_NAME": "createdAt",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The date and time when the fulfillment order was created.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "fulfillment_orders",
        "COLUMN_NAME": "displayStatus",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The display status of the fulfillment order.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "fulfillment_orders",
        "COLUMN_NAME": "id",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A globally-unique ID.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "fulfillment_orders",
        "COLUMN_NAME": "orderId",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The ID of the order associated with the fulfillment order.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "fulfillment_orders",
        "COLUMN_NAME": "requestStatus",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The request status of the fulfillment order.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "fulfillment_orders",
        "COLUMN_NAME": "status",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The status of the fulfillment order (OPEN, IN_PROGRESS, CANCELLED, etc.).",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "fulfillment_orders",
        "COLUMN_NAME": "updatedAt",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The date and time when the fulfillment order was last updated.",
        "IS_NULLABLE": False,
    },
]
