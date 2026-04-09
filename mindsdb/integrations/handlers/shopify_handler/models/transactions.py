# Transactions are nested under orders (no root-level GraphQL connection).
# This file only defines the columns metadata used by the custom list() implementation.

columns = [
    {
        "TABLE_NAME": "transactions",
        "COLUMN_NAME": "id",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A globally-unique ID.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "transactions",
        "COLUMN_NAME": "orderId",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The ID of the order this transaction belongs to.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "transactions",
        "COLUMN_NAME": "kind",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The kind of the transaction (SALE, REFUND, VOID, CAPTURE, AUTHORIZATION, EMV_AUTHORIZATION, CHANGE).",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "transactions",
        "COLUMN_NAME": "status",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The status of the transaction (SUCCESS, FAILURE, PENDING, ERROR, AWAITING_RESPONSE, UNKNOWN).",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "transactions",
        "COLUMN_NAME": "amount",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The transaction amount in shop currency.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "transactions",
        "COLUMN_NAME": "currencyCode",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The currency code of the transaction amount.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "transactions",
        "COLUMN_NAME": "gateway",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The payment gateway used to process the transaction.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "transactions",
        "COLUMN_NAME": "errorCode",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The error code from the gateway (if any).",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "transactions",
        "COLUMN_NAME": "formattedGateway",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The human-readable gateway name.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "transactions",
        "COLUMN_NAME": "test",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether the transaction is a test.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "transactions",
        "COLUMN_NAME": "processedAt",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The date and time when the transaction was processed.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "transactions",
        "COLUMN_NAME": "createdAt",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The date and time when the transaction was created.",
        "IS_NULLABLE": False,
    },
]
