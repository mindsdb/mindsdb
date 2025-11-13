from .common import AliasesEnum


class StaffMembers(AliasesEnum):
    """A class to represent a Shopify GraphQL staff member.
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/StaffMember
    Require `read_users` permission. Also the app must be a finance embedded app or installed on a Shopify Plus or Advanced store.
    """
    accountType = "accountType"
    active = "active"
    # avatar = "avatar"
    email = "email"
    exists = "exists"
    firstName = "firstName"
    id = "id"
    initials = "initials"
    isShopOwner = "isShopOwner"
    lastName = "lastName"
    locale = "locale"
    name = "name"
    phone = "phone"
    # privateData = "privateData"

columns = [
    {
        "TABLE_NAME": "staff_members",
        "COLUMN_NAME": "accountType",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The type of account the staff member has.",
        "IS_NULLABLE": None
    },
    {
        "TABLE_NAME": "staff_members",
        "COLUMN_NAME": "active",
        "DATA_TYPE": "BOOL",
        "COLUMN_DESCRIPTION": "Whether the staff member is active.",
        "IS_NULLABLE": False
    },
    # {
    #     "TABLE_NAME": "staff_members",
    #     "COLUMN_NAME": "avatar",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The image used as the staff member's avatar in the Shopify admin.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "staff_members",
        "COLUMN_NAME": "email",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The staff member's email address.",
        "IS_NULLABLE": False
    },
    {
        "TABLE_NAME": "staff_members",
        "COLUMN_NAME": "exists",
        "DATA_TYPE": "BOOL",
        "COLUMN_DESCRIPTION": "Whether the staff member's account exists.",
        "IS_NULLABLE": False
    },
    {
        "TABLE_NAME": "staff_members",
        "COLUMN_NAME": "firstName",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The staff member's first name.",
        "IS_NULLABLE": None
    },
    {
        "TABLE_NAME": "staff_members",
        "COLUMN_NAME": "id",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A globally-unique ID.",
        "IS_NULLABLE": False
    },
    {
        "TABLE_NAME": "staff_members",
        "COLUMN_NAME": "initials",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The staff member's initials, if available.",
        "IS_NULLABLE": None
    },
    {
        "TABLE_NAME": "staff_members",
        "COLUMN_NAME": "isShopOwner",
        "DATA_TYPE": "BOOL",
        "COLUMN_DESCRIPTION": "Whether the staff member is the shop owner.",
        "IS_NULLABLE": False
    },
    {
        "TABLE_NAME": "staff_members",
        "COLUMN_NAME": "lastName",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The staff member's last name.",
        "IS_NULLABLE": None
    },
    {
        "TABLE_NAME": "staff_members",
        "COLUMN_NAME": "locale",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The staff member's preferred locale. Locale values use the format language or language-COUNTRY, where language is a two-letter language code, and COUNTRY is a two-letter country code. For example: en or en-US",
        "IS_NULLABLE": False
    },
    {
        "TABLE_NAME": "staff_members",
        "COLUMN_NAME": "name",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The staff member's full name.",
        "IS_NULLABLE": False
    },
    {
        "TABLE_NAME": "staff_members",
        "COLUMN_NAME": "phone",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The staff member's phone number.",
        "IS_NULLABLE": None
    },
    # {
    #     "TABLE_NAME": "staff_members",
    #     "COLUMN_NAME": "privateData",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The data used to customize the Shopify admin experience for the staff member.",
    #     "IS_NULLABLE": False
    # }
]
