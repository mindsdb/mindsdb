# Company contacts have no root-level GraphQL query.
# They are accessible only nested under company.contacts(first: N).
# The custom list() in shopify_tables.py queries companies with embedded contacts
# and flattens each contact into a row with a companyId field.

columns = [
    {
        "TABLE_NAME": "company_contacts",
        "COLUMN_NAME": "id",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A globally-unique ID.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "company_contacts",
        "COLUMN_NAME": "companyId",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The ID of the company this contact belongs to.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "company_contacts",
        "COLUMN_NAME": "customerId",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The ID of the customer associated with this contact.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "company_contacts",
        "COLUMN_NAME": "customerEmail",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The email address of the customer associated with this contact.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "company_contacts",
        "COLUMN_NAME": "customerFirstName",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The first name of the customer associated with this contact.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "company_contacts",
        "COLUMN_NAME": "customerLastName",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The last name of the customer associated with this contact.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "company_contacts",
        "COLUMN_NAME": "isMainContact",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether this is the main contact for the company.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "company_contacts",
        "COLUMN_NAME": "locale",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The preferred locale of the contact.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "company_contacts",
        "COLUMN_NAME": "title",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The job title of the contact.",
        "IS_NULLABLE": True,
    },
    {
        "TABLE_NAME": "company_contacts",
        "COLUMN_NAME": "createdAt",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The date and time when the contact was created.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "company_contacts",
        "COLUMN_NAME": "updatedAt",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The date and time when the contact was last updated.",
        "IS_NULLABLE": False,
    },
]
