from .common import AliasesEnum
from .utils import Extract


class DiscountCodes(AliasesEnum):
    """A class to represent a Shopify GraphQL discount code (DiscountRedeemCode).
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/DiscountRedeemCode
    Require `read_discounts` permission.
    """

    asyncUsageCount = "asyncUsageCount"
    code = "code"
    createdAt = "createdAt"
    discountId = Extract("discount", "id")
    id = "id"
    updatedAt = "updatedAt"
    usageCount = "usageCount"


columns = [
    {
        "TABLE_NAME": "discount_codes",
        "COLUMN_NAME": "asyncUsageCount",
        "DATA_TYPE": "INT",
        "COLUMN_DESCRIPTION": "The number of times that the discount code has been used. This value is updated asynchronously and can be different from usageCount.",
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
        "COLUMN_NAME": "createdAt",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The date and time when the discount code was created.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "discount_codes",
        "COLUMN_NAME": "discountId",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The ID of the discount that this code belongs to.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "discount_codes",
        "COLUMN_NAME": "id",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A globally-unique ID.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "discount_codes",
        "COLUMN_NAME": "updatedAt",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The date and time when the discount code was last updated.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "discount_codes",
        "COLUMN_NAME": "usageCount",
        "DATA_TYPE": "INT",
        "COLUMN_DESCRIPTION": "The number of times that the discount code has been used.",
        "IS_NULLABLE": False,
    },
]
