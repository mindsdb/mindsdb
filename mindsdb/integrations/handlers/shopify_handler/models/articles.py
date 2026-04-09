from .common import AliasesEnum
from .utils import Extract


class ArticleAuthor(AliasesEnum):
    """A class to represent a Shopify GraphQL article author.
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/ArticleAuthor
    """

    name = "name"


class Articles(AliasesEnum):
    """A class to represent a Shopify GraphQL article.
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/Article
    Require `read_content` permission.
    """

    author = ArticleAuthor
    blogId = Extract("blog", "id")
    blogTitle = Extract("blog", "title")
    body = "body"
    createdAt = "createdAt"
    handle = "handle"
    id = "id"
    isPublished = "isPublished"
    publishedAt = "publishedAt"
    tags = "tags"
    templateSuffix = "templateSuffix"
    title = "title"
    updatedAt = "updatedAt"


columns = [
    {
        "TABLE_NAME": "articles",
        "COLUMN_NAME": "author",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The author of the article.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "articles",
        "COLUMN_NAME": "blogId",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The ID of the blog this article belongs to.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "articles",
        "COLUMN_NAME": "blogTitle",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The title of the blog this article belongs to.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "articles",
        "COLUMN_NAME": "body",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The content of the article in HTML format.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "articles",
        "COLUMN_NAME": "createdAt",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The date and time when the article was created.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "articles",
        "COLUMN_NAME": "handle",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A unique, human-readable string for the article.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "articles",
        "COLUMN_NAME": "id",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A globally-unique ID.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "articles",
        "COLUMN_NAME": "isPublished",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether the article is published.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "articles",
        "COLUMN_NAME": "publishedAt",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The date and time when the article was published.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "articles",
        "COLUMN_NAME": "tags",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "A list of tags associated with the article.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "articles",
        "COLUMN_NAME": "templateSuffix",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The suffix of the Liquid template for the article.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "articles",
        "COLUMN_NAME": "title",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The title of the article.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "articles",
        "COLUMN_NAME": "updatedAt",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The date and time when the article was last updated.",
        "IS_NULLABLE": False,
    },
]
