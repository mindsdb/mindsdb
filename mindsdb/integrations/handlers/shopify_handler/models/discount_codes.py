from .common import AliasesEnum
from .utils import Extract


class DiscountCodes(AliasesEnum):
    """A class to represent a Shopify GraphQL discount code (DiscountRedeemCode).
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/DiscountRedeemCode
    Require `read_discounts` permission.
    Note: discountCodes root query removed in API 2025-10.
    Fetched via codeDiscountNodes → codes connection.
    """

    asyncUsageCount = "asyncUsageCount"
    code = "code"
    id = "id"


columns = [
    {
        "TABLE_NAME": "discount_codes",
        "COLUMN_NAME": "id",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A globally-unique ID.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "discount_codes",
        "COLUMN_NAME": "code",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The code that customers enter in the checkout's Discount field.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "discount_codes",
        "COLUMN_NAME": "asyncUsageCount",
        "DATA_TYPE": "INT",
        "COLUMN_DESCRIPTION": "The number of times that the discount code has been used (updated asynchronously).",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "discount_codes",
        "COLUMN_NAME": "discountId",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The ID of the parent discount this code belongs to.",
        "IS_NULLABLE": False,
    },
]
