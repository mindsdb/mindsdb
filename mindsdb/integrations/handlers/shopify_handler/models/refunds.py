# Refunds are nested under orders (no root-level GraphQL connection).
# This file only defines the columns metadata used by the custom list() implementation.

columns = [
    {
        "TABLE_NAME": "refunds",
        "COLUMN_NAME": "id",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A globally-unique ID.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "refunds",
        "COLUMN_NAME": "orderId",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The ID of the order this refund belongs to.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "refunds",
        "COLUMN_NAME": "note",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The optional note attached to a refund.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "refunds",
        "COLUMN_NAME": "createdAt",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The date and time when the refund was created.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "refunds",
        "COLUMN_NAME": "updatedAt",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The date and time when the refund was last updated.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "refunds",
        "COLUMN_NAME": "totalRefundedAmount",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "Total refunded amount in shop currency.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "refunds",
        "COLUMN_NAME": "totalRefundedCurrencyCode",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "Currency code of the refunded amount.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "refunds",
        "COLUMN_NAME": "refundLineItems",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The list of line items that were refunded.",
        "IS_NULLABLE": False,
    },
]
