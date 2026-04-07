from .common import AliasesEnum, Count, SEO


class Collections(AliasesEnum):
    """A class to represent a Shopify GraphQL collection.
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/Collection
    Require `read_products` permission.
    """

    description = "description"
    descriptionHtml = "descriptionHtml"
    handle = "handle"
    id = "id"
    onlineStorePreviewUrl = "onlineStorePreviewUrl"
    onlineStoreUrl = "onlineStoreUrl"
    productsCount = Count
    publishedAt = "publishedAt"
    seo = SEO
    sortOrder = "sortOrder"
    templateSuffix = "templateSuffix"
    title = "title"
    updatedAt = "updatedAt"


columns = [
    {
        "TABLE_NAME": "collections",
        "COLUMN_NAME": "description",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A single-line description of the collection, stripped of any HTML tags and formatting.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "collections",
        "COLUMN_NAME": "descriptionHtml",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The description of the collection, complete with HTML formatting.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "collections",
        "COLUMN_NAME": "handle",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A unique string that identifies the collection.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "collections",
        "COLUMN_NAME": "id",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A globally-unique ID.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "collections",
        "COLUMN_NAME": "onlineStorePreviewUrl",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The URL used for viewing the resource on the shop's Online Store.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "collections",
        "COLUMN_NAME": "onlineStoreUrl",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The online store URL for the collection. Returns null if the collection isn't published to the online store sales channel.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "collections",
        "COLUMN_NAME": "productsCount",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The number of products in the collection.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "collections",
        "COLUMN_NAME": "publishedAt",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The date and time when the collection was published to the online store.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "collections",
        "COLUMN_NAME": "seo",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The SEO information for the collection.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "collections",
        "COLUMN_NAME": "sortOrder",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The sort order for the products in the collection.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "collections",
        "COLUMN_NAME": "templateSuffix",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The suffix of the Liquid template being used for the collection.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "collections",
        "COLUMN_NAME": "title",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The name of the collection.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "collections",
        "COLUMN_NAME": "updatedAt",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The date and time when the collection was last modified.",
        "IS_NULLABLE": False,
    },
]
