from .common import AliasesEnum, Count


class Blogs(AliasesEnum):
    """A class to represent a Shopify GraphQL blog.
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/Blog
    Require `read_content` permission.
    """

    articlesCount = Count
    createdAt = "createdAt"
    handle = "handle"
    id = "id"
    templateSuffix = "templateSuffix"
    title = "title"
    updatedAt = "updatedAt"


columns = [
    {
        "TABLE_NAME": "blogs",
        "COLUMN_NAME": "articlesCount",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The number of articles in the blog.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "blogs",
        "COLUMN_NAME": "createdAt",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The date and time when the blog was created.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "blogs",
        "COLUMN_NAME": "handle",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A unique, human-readable string for the blog.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "blogs",
        "COLUMN_NAME": "id",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A globally-unique ID.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "blogs",
        "COLUMN_NAME": "templateSuffix",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The suffix of the Liquid template being used for the blog.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "blogs",
        "COLUMN_NAME": "title",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The title of the blog.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "blogs",
        "COLUMN_NAME": "updatedAt",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The date and time when the blog was last updated.",
        "IS_NULLABLE": False,
    },
]
