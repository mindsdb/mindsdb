from .common import AliasesEnum, MoneyBag
from .utils import Extract


class DraftOrders(AliasesEnum):
    """A class to represent a Shopify GraphQL draft order.
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/DraftOrder
    Require `read_draft_orders` permission.
    """

    completedAt = "completedAt"
    createdAt = "createdAt"
    currencyCode = "currencyCode"
    customerId = Extract("customer", "id")
    email = "email"
    id = "id"
    invoiceSentAt = "invoiceSentAt"
    invoiceUrl = "invoiceUrl"
    legacyResourceId = "legacyResourceId"
    name = "name"
    note = "note"
    phone = "phone"
    poNumber = "poNumber"
    ready = "ready"
    status = "status"
    subtotalPriceSet = MoneyBag
    tags = "tags"
    taxesIncluded = "taxesIncluded"
    taxExempt = "taxExempt"
    totalPriceSet = MoneyBag
    updatedAt = "updatedAt"


columns = [
    {
        "TABLE_NAME": "draft_orders",
        "COLUMN_NAME": "completedAt",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The date and time when the draft order converted to a new order, and the draft order's status changed to Completed.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "draft_orders",
        "COLUMN_NAME": "createdAt",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The date and time when the draft order was created.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "draft_orders",
        "COLUMN_NAME": "currencyCode",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The three letter code for the currency of the store at the time of the most recent update to the draft order.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "draft_orders",
        "COLUMN_NAME": "customerId",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The ID of the customer associated with the draft order.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "draft_orders",
        "COLUMN_NAME": "email",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The email address of the customer, which is used to send invoice and confirmation emails.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "draft_orders",
        "COLUMN_NAME": "id",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A globally-unique ID.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "draft_orders",
        "COLUMN_NAME": "invoiceSentAt",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The date and time when the invoice was last emailed to the customer.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "draft_orders",
        "COLUMN_NAME": "invoiceUrl",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The link to the checkout, which is sent to the customer in the invoice email.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "draft_orders",
        "COLUMN_NAME": "legacyResourceId",
        "DATA_TYPE": "INT",
        "COLUMN_DESCRIPTION": "The ID of the corresponding resource in the REST Admin API.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "draft_orders",
        "COLUMN_NAME": "name",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The identifier for the draft order, which is unique to a store.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "draft_orders",
        "COLUMN_NAME": "note",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The text of an optional note that a shop owner can attach to the draft order.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "draft_orders",
        "COLUMN_NAME": "phone",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The customer's phone number.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "draft_orders",
        "COLUMN_NAME": "poNumber",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The purchase order number.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "draft_orders",
        "COLUMN_NAME": "ready",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether the draft order is ready and can be completed. Draft orders might have asynchronous operations that can take time to finish.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "draft_orders",
        "COLUMN_NAME": "status",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The status of the draft order (OPEN, INVOICE_SENT, COMPLETED).",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "draft_orders",
        "COLUMN_NAME": "subtotalPriceSet",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The subtotal of the line items and their discounts. Doesn't include shipping charges, shipping discounts, or taxes.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "draft_orders",
        "COLUMN_NAME": "tags",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "A comma-separated list of searchable keywords associated with the draft order.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "draft_orders",
        "COLUMN_NAME": "taxesIncluded",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether the line item prices include taxes.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "draft_orders",
        "COLUMN_NAME": "taxExempt",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether taxes are exempt for the draft order.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "draft_orders",
        "COLUMN_NAME": "totalPriceSet",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The total price of the draft order, including taxes, shipping charges, and discounts.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "draft_orders",
        "COLUMN_NAME": "updatedAt",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The date and time when the draft order was last changed.",
        "IS_NULLABLE": False,
    },
]
