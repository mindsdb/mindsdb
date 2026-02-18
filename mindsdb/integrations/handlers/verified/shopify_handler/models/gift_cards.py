from .common import AliasesEnum, MoneyV2
from .utils import Extract


class GiftCards(AliasesEnum):
    """A class to represent a Shopify GraphQL gift card.
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/GiftCard
    Require `read_gift_cards` permission.
    """

    balance = MoneyV2
    createdAt = "createdAt"
    customerId = Extract("customer", "id")  # Custom
    # customer = "customer"
    deactivatedAt = "deactivatedAt"
    enabled = "enabled"
    expiresOn = "expiresOn"
    id = "id"
    initialValue = MoneyV2
    lastCharacters = "lastCharacters"
    maskedCode = "maskedCode"
    note = "note"
    orderId = Extract("order", "id")  # Custom
    # order = "order"
    # recipientAttributes = "recipientAttributes"
    templateSuffix = "templateSuffix"
    # transactions = "transactions"
    updatedAt = "updatedAt"


columns = [
    {
        "TABLE_NAME": "gift_cards",
        "COLUMN_NAME": "balance",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The gift card's remaining balance.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "gift_cards",
        "COLUMN_NAME": "createdAt",
        "DATA_TYPE": "TIMESTAMP",
        "COLUMN_DESCRIPTION": "The date and time at which the gift card was created.",
        "IS_NULLABLE": False,
    },
    # {
    #     "TABLE_NAME": "gift_cards",
    #     "COLUMN_NAME": "customer",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The customer who will receive the gift card.",
    #     "IS_NULLABLE": None
    # },
    {
        "TABLE_NAME": "gift_cards",
        "COLUMN_NAME": "deactivatedAt",
        "DATA_TYPE": "TIMESTAMP",
        "COLUMN_DESCRIPTION": "The date and time at which the gift card was deactivated.",
        "IS_NULLABLE": None,
    },
    {
        "TABLE_NAME": "gift_cards",
        "COLUMN_NAME": "enabled",
        "DATA_TYPE": "BOOL",
        "COLUMN_DESCRIPTION": "Whether the gift card is enabled.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "gift_cards",
        "COLUMN_NAME": "expiresOn",
        "DATA_TYPE": "DATE",
        "COLUMN_DESCRIPTION": "The date at which the gift card will expire.",
        "IS_NULLABLE": None,
    },
    {
        "TABLE_NAME": "gift_cards",
        "COLUMN_NAME": "id",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A globally-unique ID.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "gift_cards",
        "COLUMN_NAME": "initialValue",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The initial value of the gift card.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "gift_cards",
        "COLUMN_NAME": "lastCharacters",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The final four characters of the gift card code.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "gift_cards",
        "COLUMN_NAME": "maskedCode",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The gift card code. Everything but the final four characters is masked.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "gift_cards",
        "COLUMN_NAME": "note",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The note associated with the gift card, which isn't visible to the customer.",
        "IS_NULLABLE": None,
    },
    # {
    #     "TABLE_NAME": "gift_cards",
    #     "COLUMN_NAME": "order",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The order associated with the gift card. This value is null if the gift card was issued manually.",
    #     "IS_NULLABLE": None
    # },
    # {
    #     "TABLE_NAME": "gift_cards",
    #     "COLUMN_NAME": "recipientAttributes",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The recipient who will receive the gift card.",
    #     "IS_NULLABLE": None
    # },
    {
        "TABLE_NAME": "gift_cards",
        "COLUMN_NAME": "templateSuffix",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The theme template used to render the gift card online.",
        "IS_NULLABLE": None,
    },
    # {
    #     "TABLE_NAME": "gift_cards",
    #     "COLUMN_NAME": "transactions",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The transaction history of the gift card.",
    #     "IS_NULLABLE": None
    # },
    {
        "TABLE_NAME": "gift_cards",
        "COLUMN_NAME": "updatedAt",
        "DATA_TYPE": "TIMESTAMP",
        "COLUMN_DESCRIPTION": "The date and time at which the gift card was updated.",
        "IS_NULLABLE": False,
    },
]
