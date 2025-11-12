from enum import Enum

from .common import (
    MailingAddress,
    MoneyV2
)
from .utils import Nodes, Extract


class Customers(Enum):
    addresses = MailingAddress
    addressesV2 = Nodes(MailingAddress)
    amountSpent = MoneyV2
    canDelete = "canDelete"
    # companyContactProfiles = CompanyContact
    createdAt = "createdAt"
    dataSaleOptOut = "dataSaleOptOut"
    defaultAddress = MailingAddress
    country = Extract("defaultAddress", "country")  # Custom
    # defaultEmailAddress = defaultEmailAddress
    emailAddress = Extract("defaultEmailAddress", "emailAddress")  # Custom
    # defaultPhoneNumber = "defaultPhoneNumber"
    phoneNumber = Extract("defaultPhoneNumber", "phoneNumber")  # Custom
    displayName = "displayName"
    # events = "events"
    firstName = "firstName"
    id = "id"
    # image = "image"
    lastName = "lastName"
    # lastOrder = "lastOrder"
    # legacyResourceId = "legacyResourceId"
    lifetimeDuration = "lifetimeDuration"
    locale = "locale"
    # mergeable = "mergeable"
    # metafield = "metafield"
    # metafields = "metafields"
    multipassIdentifier = "multipassIdentifier"
    note = "note"
    numberOfOrders = "numberOfOrders"
    # orders = "orders"
    # paymentMethods = "paymentMethods"
    productSubscriberStatus = "productSubscriberStatus"
    state = "state"
    # statistics = "statistics"
    # storeCreditAccounts = "storeCreditAccounts"
    # subscriptionContracts = "subscriptionContracts"
    tags = "tags"
    taxExempt = "taxExempt"
    # taxExemptions = "taxExemptions"
    updatedAt = "updatedAt"
    verifiedEmail = "verifiedEmail"


columns = [
    {
        "TABLE_NAME": "customers",
        "COLUMN_NAME": "addresses",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "A list of addresses associated with the customer.",
        "IS_NULLABLE": False
    },
    {
        "TABLE_NAME": "customers",
        "COLUMN_NAME": "addressesV2",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The addresses associated with the customer.",
        "IS_NULLABLE": False
    },
    {
        "TABLE_NAME": "customers",
        "COLUMN_NAME": "amountSpent",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The total amount that the customer has spent on orders in their lifetime.",
        "IS_NULLABLE": False
    },
    {
        "TABLE_NAME": "customers",
        "COLUMN_NAME": "canDelete",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether the merchant can delete the customer from their store. A customer can be deleted from a store only if they haven't yet made an order. After a customer makes an order, they can't be deleted from a store.",
        "IS_NULLABLE": False
    },
    # {
    #     "TABLE_NAME": "customers",
    #     "COLUMN_NAME": "companyContactProfiles",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A list of the customer's company contact profiles.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "customers",
        "COLUMN_NAME": "createdAt",
        "DATA_TYPE": "TIMESTAMP",
        "COLUMN_DESCRIPTION": "The date and time when the customer was added to the store.",
        "IS_NULLABLE": False
    },
    {
        "TABLE_NAME": "customers",
        "COLUMN_NAME": "dataSaleOptOut",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether the customer has opted out of having their data sold.",
        "IS_NULLABLE": False
    },
    {
        "TABLE_NAME": "customers",
        "COLUMN_NAME": "defaultAddress",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The default address associated with the customer.",
        "IS_NULLABLE": None
    },
    {
        # Custom field, extracted from defaultAddress
        "TABLE_NAME": "customers",
        "COLUMN_NAME": "country",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The coutry associated with the customer.",
        "IS_NULLABLE": None
    },
    # {
    #     "TABLE_NAME": "customers",
    #     "COLUMN_NAME": "defaultEmailAddress",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The customer's default email address.",
    #     "IS_NULLABLE": None
    # },
    {
        # Custom field, extracted from defaultEmailAddress
        "TABLE_NAME": "customers",
        "COLUMN_NAME": "emailAddress",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The customer's default email address.",
        "IS_NULLABLE": None
    },
    # {
    #     "TABLE_NAME": "customers",
    #     "COLUMN_NAME": "defaultPhoneNumber",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The customer's default phone number.",
    #     "IS_NULLABLE": None
    # },
    {
        # Custom field, extracted from defaultPhoneNumber
        "TABLE_NAME": "customers",
        "COLUMN_NAME": "phoneNumber",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The customer's default phone number.",
        "IS_NULLABLE": None
    },
    {
        "TABLE_NAME": "customers",
        "COLUMN_NAME": "displayName",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The full name of the customer, based on the values for first_name and last_name. If the first_name and last_name are not available, then this falls back to the customer's email address, and if that is not available, the customer's phone number.",
        "IS_NULLABLE": False
    },
    # {
    #     "TABLE_NAME": "customers",
    #     "COLUMN_NAME": "events",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A list of events associated with the customer.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "customers",
        "COLUMN_NAME": "firstName",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The customer's first name.",
        "IS_NULLABLE": None
    },
    {
        "TABLE_NAME": "customers",
        "COLUMN_NAME": "id",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A globally-unique ID.",
        "IS_NULLABLE": False
    },
    # {
    #     "TABLE_NAME": "customers",
    #     "COLUMN_NAME": "image",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The image associated with the customer.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "customers",
        "COLUMN_NAME": "lastName",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The customer's last name.",
        "IS_NULLABLE": None
    },
    # {
    #     "TABLE_NAME": "customers",
    #     "COLUMN_NAME": "lastOrder",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The customer's last order.",
    #     "IS_NULLABLE": None
    # },
    # {
    #     "TABLE_NAME": "customers",
    #     "COLUMN_NAME": "legacyResourceId",
    #     "DATA_TYPE": "INT",
    #     "COLUMN_DESCRIPTION": "The ID of the corresponding resource in the REST Admin API.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "customers",
        "COLUMN_NAME": "lifetimeDuration",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The amount of time since the customer was first added to the store. Example: 'about 12 years'.",
        "IS_NULLABLE": False
    },
    {
        "TABLE_NAME": "customers",
        "COLUMN_NAME": "locale",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The customer's locale.",
        "IS_NULLABLE": False
    },
    # {
    #     "TABLE_NAME": "customers",
    #     "COLUMN_NAME": "mergeable",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "Whether the customer can be merged with another customer.",
    #     "IS_NULLABLE": False
    # },
    # {
    #     "TABLE_NAME": "customers",
    #     "COLUMN_NAME": "metafield",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A custom field, including its namespace and key, that's associated with a Shopify resource for the purposes of adding and storing additional information.",
    #     "IS_NULLABLE": None
    # },
    # {
    #     "TABLE_NAME": "customers",
    #     "COLUMN_NAME": "metafields",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A list of custom fields that a merchant associates with a Shopify resource.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "customers",
        "COLUMN_NAME": "multipassIdentifier",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A unique identifier for the customer that's used with Multipass login.",
        "IS_NULLABLE": None
    },
    {
        "TABLE_NAME": "customers",
        "COLUMN_NAME": "note",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A note about the customer.",
        "IS_NULLABLE": None
    },
    {
        "TABLE_NAME": "customers",
        "COLUMN_NAME": "numberOfOrders",
        "DATA_TYPE": "INT",
        "COLUMN_DESCRIPTION": "The number of orders that the customer has made at the store in their lifetime.",
        "IS_NULLABLE": False
    },
    # {
    #     "TABLE_NAME": "customers",
    #     "COLUMN_NAME": "orders",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A list of the customer's orders.",
    #     "IS_NULLABLE": False
    # },
    # {
    #     "TABLE_NAME": "customers",
    #     "COLUMN_NAME": "paymentMethods",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A list of the customer's payment methods.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "customers",
        "COLUMN_NAME": "productSubscriberStatus",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "Possible subscriber states of a customer defined by their subscription contracts.",
        "IS_NULLABLE": False
    },
    {
        "TABLE_NAME": "customers",
        "COLUMN_NAME": "state",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The state of the customer's account with the shop. Please note that this only meaningful when Classic Customer Accounts is active.",
        "IS_NULLABLE": False
    },
    # {
    #     "TABLE_NAME": "customers",
    #     "COLUMN_NAME": "statistics",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The statistics for a given customer.",
    #     "IS_NULLABLE": False
    # },
    # {
    #     "TABLE_NAME": "customers",
    #     "COLUMN_NAME": "storeCreditAccounts",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "Returns a list of store credit accounts that belong to the owner resource. A store credit account owner can hold multiple accounts each with a different currency.",
    #     "IS_NULLABLE": False
    # },
    # {
    #     "TABLE_NAME": "customers",
    #     "COLUMN_NAME": "subscriptionContracts",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A list of the customer's subscription contracts.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "customers",
        "COLUMN_NAME": "tags",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "A comma separated list of tags that have been added to the customer.",
        "IS_NULLABLE": False
    },
    {
        "TABLE_NAME": "customers",
        "COLUMN_NAME": "taxExempt",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether the customer is exempt from being charged taxes on their orders.",
        "IS_NULLABLE": False
    },
    # {
    #     "TABLE_NAME": "customers",
    #     "COLUMN_NAME": "taxExemptions",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The list of tax exemptions applied to the customer.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "customers",
        "COLUMN_NAME": "updatedAt",
        "DATA_TYPE": "TIMESTAMP",
        "COLUMN_DESCRIPTION": "The date and time when the customer was last updated.",
        "IS_NULLABLE": False
    },
    {
        "TABLE_NAME": "customers",
        "COLUMN_NAME": "verifiedEmail",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether the customer has verified their email address. Defaults to true if the customer is created through the Shopify admin or API.",
        "IS_NULLABLE": False
    }
]