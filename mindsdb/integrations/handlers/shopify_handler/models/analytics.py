# Analytics uses a custom list() in shopify_tables.py that passes a raw ShopifyQL
# string to the shopifyqlQuery GraphQL root field. The returned columns are dynamic
# (determined by the query at runtime), so no AliasesEnum is defined here.
#
# Usage:
#   SELECT * FROM shopify.analytics
#   WHERE query = 'FROM sales SHOW total_sales, orders_count GROUP BY day SINCE -30d'

columns = [
    {
        "TABLE_NAME": "analytics",
        "COLUMN_NAME": "query",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The ShopifyQL query string to execute (used as a WHERE condition). Result columns are dynamic.",
        "IS_NULLABLE": False,
    },
]
