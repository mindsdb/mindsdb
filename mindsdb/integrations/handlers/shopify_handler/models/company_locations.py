from .common import AliasesEnum
from .utils import Extract


class CompanyLocationAddress(AliasesEnum):
    """Minimal address representation for a company location."""

    address1 = "address1"
    address2 = "address2"
    city = "city"
    country = "country"
    countryCode = "countryCode"
    province = "province"
    zip = "zip"
    phone = "phone"


class CompanyLocations(AliasesEnum):
    """A class to represent a Shopify GraphQL company location (B2B).
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/CompanyLocation
    Require `read_companies` permission (Shopify Plus only).
    """

    billingAddress = CompanyLocationAddress
    companyId = Extract("company", "id")
    createdAt = "createdAt"
    externalId = "externalId"
    id = "id"
    locale = "locale"
    name = "name"
    phone = "phone"
    shippingAddress = CompanyLocationAddress
    taxRegistrationId = "taxRegistrationId"
    updatedAt = "updatedAt"


columns = [
    {
        "TABLE_NAME": "company_locations",
        "COLUMN_NAME": "billingAddress",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The billing address of the company location.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "company_locations",
        "COLUMN_NAME": "companyId",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The ID of the company this location belongs to.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "company_locations",
        "COLUMN_NAME": "createdAt",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The date and time when the company location was created.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "company_locations",
        "COLUMN_NAME": "externalId",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A unique externally-supplied ID for the company location.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "company_locations",
        "COLUMN_NAME": "id",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A globally-unique ID.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "company_locations",
        "COLUMN_NAME": "locale",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The preferred locale of the company location.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "company_locations",
        "COLUMN_NAME": "name",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The name of the company location.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "company_locations",
        "COLUMN_NAME": "phone",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The phone number of the company location.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "company_locations",
        "COLUMN_NAME": "shippingAddress",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The shipping address of the company location.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "company_locations",
        "COLUMN_NAME": "taxRegistrationId",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The tax registration ID of the company location.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "company_locations",
        "COLUMN_NAME": "updatedAt",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The date and time when the company location was last updated.",
        "IS_NULLABLE": False,
    },
]
