from .common import AliasesEnum, MailingAddress, MoneyV2
from .utils import Extract, Nodes


class AbandonedCheckoutLineItem(AliasesEnum):
    """Minimal representation of an abandoned checkout line item."""

    id = "id"
    title = "title"
    quantity = "quantity"
    variantTitle = "variantTitle"


class AbandonedCheckouts(AliasesEnum):
    """A class to represent a Shopify GraphQL abandoned checkout.
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/AbandonedCheckout
    Require `read_checkouts` permission.
    """

    abandonedCheckoutUrl = "abandonedCheckoutUrl"
    completedAt = "completedAt"
    createdAt = "createdAt"
    customerId = Extract("customer", "id")
    email = "email"
    id = "id"
    lineItems = Nodes(AbandonedCheckoutLineItem)
    shippingAddress = MailingAddress
    totalPriceV2 = MoneyV2
    updatedAt = "updatedAt"


columns = [
    {
        "TABLE_NAME": "abandoned_checkouts",
        "COLUMN_NAME": "abandonedCheckoutUrl",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The URL for the abandoned checkout.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "abandoned_checkouts",
        "COLUMN_NAME": "completedAt",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The date and time when the checkout was completed.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "abandoned_checkouts",
        "COLUMN_NAME": "createdAt",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The date and time when the checkout was created.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "abandoned_checkouts",
        "COLUMN_NAME": "customerId",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The ID of the customer who started the checkout.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "abandoned_checkouts",
        "COLUMN_NAME": "email",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The email address of the customer.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "abandoned_checkouts",
        "COLUMN_NAME": "id",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A globally-unique ID.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "abandoned_checkouts",
        "COLUMN_NAME": "lineItems",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The line items in the checkout.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "abandoned_checkouts",
        "COLUMN_NAME": "shippingAddress",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The shipping address of the checkout.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "abandoned_checkouts",
        "COLUMN_NAME": "totalPriceV2",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The total price of the checkout (amount and currencyCode).",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "abandoned_checkouts",
        "COLUMN_NAME": "updatedAt",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The date and time when the checkout was last updated.",
        "IS_NULLABLE": False,
    },
]
