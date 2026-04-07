from .common import AliasesEnum, SEO


class Pages(AliasesEnum):
    """A class to represent a Shopify GraphQL page.
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/OnlineStorePage
    Require `read_content` permission.
    """

    author = "author"
    body = "body"
    bodySummary = "bodySummary"
    createdAt = "createdAt"
    handle = "handle"
    id = "id"
    isPublished = "isPublished"
    onlineStorePreviewUrl = "onlineStorePreviewUrl"
    onlineStoreUrl = "onlineStoreUrl"
    publishedAt = "publishedAt"
    seo = SEO
    templateSuffix = "templateSuffix"
    title = "title"
    updatedAt = "updatedAt"


columns = [
    {
        "TABLE_NAME": "pages",
        "COLUMN_NAME": "author",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The author of the page.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "pages",
        "COLUMN_NAME": "body",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The content of the page in HTML format.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "pages",
        "COLUMN_NAME": "bodySummary",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A summary of the page body content, stripped of HTML tags and formatted.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "pages",
        "COLUMN_NAME": "createdAt",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The date and time when the page was created.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "pages",
        "COLUMN_NAME": "handle",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A unique, human-readable string for the page.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "pages",
        "COLUMN_NAME": "id",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A globally-unique ID.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "pages",
        "COLUMN_NAME": "isPublished",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether the page is published.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "pages",
        "COLUMN_NAME": "onlineStorePreviewUrl",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The URL used for viewing the page on the shop's Online Store.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "pages",
        "COLUMN_NAME": "onlineStoreUrl",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The public URL for the page on the shop's Online Store. Returns null if unpublished.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "pages",
        "COLUMN_NAME": "publishedAt",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The date and time when the page was published.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "pages",
        "COLUMN_NAME": "seo",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The SEO title and description for the page.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "pages",
        "COLUMN_NAME": "templateSuffix",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The suffix of the Liquid template being used to show the page in the online store.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "pages",
        "COLUMN_NAME": "title",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The title of the page.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "pages",
        "COLUMN_NAME": "updatedAt",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The date and time when the page was last updated.",
        "IS_NULLABLE": False,
    },
]
