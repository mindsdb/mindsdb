from .common import AliasesEnum, Count, MailingAddress, OrderCancellation, MoneyBag
from .utils import Extract, DeepExtract


class Orders(AliasesEnum):
    """A class to represent a Shopify GraphQL order.
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/Order
    Require `read_orders`, `read_marketplace_orders` or `read_quick_sale` permission.
    """

    # additionalFees = "additionalFees"
    # agreements = "agreements"
    # alerts = "alerts"
    # app = "app"
    # billingAddress = "billingAddress"
    billingAddressMatchesShippingAddress = "billingAddressMatchesShippingAddress"
    cancellation = OrderCancellation
    cancelledAt = "cancelledAt"
    cancelReason = "cancelReason"
    canMarkAsPaid = "canMarkAsPaid"
    canNotifyCustomer = "canNotifyCustomer"
    capturable = "capturable"
    # cartDiscountAmountSet = "cartDiscountAmountSet"
    # channelInformation = "channelInformation"
    clientIp = "clientIp"
    closed = "closed"
    closedAt = "closedAt"
    confirmationNumber = "confirmationNumber"
    confirmed = "confirmed"
    createdAt = "createdAt"
    currencyCode = "currencyCode"
    # currentCartDiscountAmountSet = "currentCartDiscountAmountSet"
    currentShippingPriceSet = MoneyBag
    currentSubtotalLineItemsQuantity = "currentSubtotalLineItemsQuantity"
    currentSubtotalPriceSet = MoneyBag
    # currentTaxLines = "currentTaxLines"
    currentTotalAdditionalFeesSet = MoneyBag
    currentTotalDiscountsSet = MoneyBag
    currentTotalDutiesSet = MoneyBag
    currentTotalPriceSet = MoneyBag
    currentTotalTaxSet = MoneyBag
    currentTotalWeight = "currentTotalWeight"
    # customAttributes = "customAttributes"
    # customer = "customer"
    customerId = Extract("customer", "id")  # custom
    customerAcceptsMarketing = "customerAcceptsMarketing"
    # customerJourneySummary = "customerJourneySummary"
    customerLocale = "customerLocale"
    # discountApplications = "discountApplications"
    discountCode = "discountCode"
    discountCodes = "discountCodes"
    # displayAddress = "displayAddress"
    # displayFinancialStatus = "displayFinancialStatus"
    # displayFulfillmentStatus = "displayFulfillmentStatus"
    # disputes = "disputes"
    dutiesIncluded = "dutiesIncluded"
    edited = "edited"
    email = "email"
    estimatedTaxes = "estimatedTaxes"
    # events = "events"
    fulfillable = "fulfillable"
    # fulfillmentOrders = "fulfillmentOrders"
    # fulfillments = "fulfillments"
    fulfillmentsCount = Count
    fullyPaid = "fullyPaid"
    hasTimelineComment = "hasTimelineComment"
    id = "id"
    # legacyResourceId = "legacyResourceId"
    # lineItems = "lineItems"
    # localizedFields = "localizedFields"
    # merchantBusinessEntity = "merchantBusinessEntity"
    merchantEditable = "merchantEditable"
    merchantEditableErrors = "merchantEditableErrors"
    # merchantOfRecordApp = "merchantOfRecordApp"
    # metafield = "metafield"
    # metafields = "metafields"
    name = "name"
    netPaymentSet = MoneyBag
    # nonFulfillableLineItems = "nonFulfillableLineItems"
    note = "note"
    number = "number"
    originalTotalAdditionalFeesSet = MoneyBag
    originalTotalDutiesSet = MoneyBag
    originalTotalPriceSet = MoneyBag
    # paymentCollectionDetails = "paymentCollectionDetails"
    paymentGatewayNames = "paymentGatewayNames"
    # paymentTerms = "paymentTerms"
    phone = "phone"
    poNumber = "poNumber"
    presentmentCurrencyCode = "presentmentCurrencyCode"
    processedAt = "processedAt"
    productNetwork = "productNetwork"
    # publication = "publication"
    # purchasingEntity = "purchasingEntity"
    refundable = "refundable"
    refundDiscrepancySet = MoneyBag
    # refunds = "refunds"
    registeredSourceUrl = "registeredSourceUrl"
    requiresShipping = "requiresShipping"
    restockable = "restockable"
    # retailLocation = "retailLocation"
    # returns = "returns"
    returnStatus = "returnStatus"
    # risk = "risk"
    shippingAddress = MailingAddress
    # shippingLine = "shippingLine"
    # shippingLines = "shippingLines"
    # shopifyProtect = "shopifyProtect"
    sourceIdentifier = "sourceIdentifier"
    sourceName = "sourceName"
    # staffMember = "staffMember"
    statusPageUrl = "statusPageUrl"
    subtotalLineItemsQuantity = "subtotalLineItemsQuantity"
    subtotalPriceSet = MoneyBag
    # suggestedRefund = "suggestedRefund"
    tags = "tags"
    taxesIncluded = "taxesIncluded"
    taxExempt = "taxExempt"
    # taxLines = "taxLines"
    test = "test"
    totalCapturableSet = MoneyBag
    # totalCashRoundingAdjustment = "totalCashRoundingAdjustment"
    totalDiscountsSet = MoneyBag
    totalOutstandingSet = MoneyBag
    totalPriceSet = MoneyBag
    totalPriceSet_presentmentMoney_amount = DeepExtract(["totalPriceSet", "presentmentMoney", "amount"], 'DECIMAL')
    totalPriceSet_presentmentMoney_currencyCode = DeepExtract(["totalPriceSet", "presentmentMoney", "currencyCode"], 'TEXT')
    totalPriceSet_shopMoney_amount = DeepExtract(["totalPriceSet", "shopMoney", "amount"], 'DECIMAL')
    totalPriceSet_shopMoney_currencyCode = DeepExtract(["totalPriceSet", "shopMoney", "currencyCode"], 'TEXT')
    totalReceivedSet = MoneyBag
    totalRefundedSet = MoneyBag
    totalRefundedShippingSet = MoneyBag
    totalShippingPriceSet = MoneyBag
    totalTaxSet = MoneyBag
    totalTipReceivedSet = MoneyBag
    totalWeight = "totalWeight"
    # transactions = "transactions"
    transactionsCount = Count
    unpaid = "unpaid"
    updatedAt = "updatedAt"


columns = [
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "additionalFees",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A list of additional fees applied to an order, such as duties, import fees, or tax lines.",
    #     "IS_NULLABLE": False
    # },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "agreements",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A list of sales agreements associated with the order, such as contracts defining payment terms, or delivery schedules between merchants and customers.",
    #     "IS_NULLABLE": False
    # },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "alerts",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A list of messages that appear on the Orders page in the Shopify admin. These alerts provide merchants with important information about an order's status or required actions.",
    #     "IS_NULLABLE": False
    # },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "app",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The application that created the order. For example, Online Store, Point of Sale, or a custom app name. Use this to identify the order source for attribution and fulfillment workflows.",
    #     "IS_NULLABLE": None
    # },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "billingAddress",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The billing address associated with the payment method selected by the customer for an order. Returns null if no billing address was provided during checkout.",
    #     "IS_NULLABLE": None
    # },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "billingAddressMatchesShippingAddress",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether the billing address matches the shipping address. Returns true if both addresses are the same, and false if they're different or if an address is missing.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "cancellation",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "Details of an order's cancellation, if it has been canceled. This includes the reason, date, and any staff notes.",
        "IS_NULLABLE": None,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "cancelledAt",
        "DATA_TYPE": "TIMESTAMP",
        "COLUMN_DESCRIPTION": "The date and time in ISO 8601 format when an order was canceled. Returns null if the order hasn't been canceled.",
        "IS_NULLABLE": None,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "cancelReason",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The reason provided for an order cancellation. For example, a merchant might cancel an order if there's insufficient inventory. Returns null if the order hasn't been canceled.",
        "IS_NULLABLE": None,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "canMarkAsPaid",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether an order can be manually marked as paid. Returns false if the order is already paid, is canceled, has pending Shopify Payments transactions, or has a negative payment amount.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "canNotifyCustomer",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether order notifications can be sent to the customer. Returns true if the customer has a valid email address.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "capturable",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether an authorized payment for an order can be captured. Returns true if an authorized payment exists that hasn't been fully captured yet.",
        "IS_NULLABLE": False,
    },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "cartDiscountAmountSet",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The total discount amount applied at the time the order was created, displayed in both shop and presentment currencies, before returns, refunds, order edits, and cancellations. This field only includes discounts applied to the entire order.",
    #     "IS_NULLABLE": None
    # },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "channelInformation",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "Details about the sales channel that created the order, such as the channel app type and channel name, which helps to track order sources.",
    #     "IS_NULLABLE": None
    # },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "clientIp",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The IP address of the customer who placed the order. Useful for fraud detection and geographic analysis.",
        "IS_NULLABLE": None,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "closed",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether an order is closed. An order is considered closed if all its line items have been fulfilled or canceled, and all financial transactions are complete.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "closedAt",
        "DATA_TYPE": "TIMESTAMP",
        "COLUMN_DESCRIPTION": "The date and time ISO 8601 format when an order was closed. Shopify automatically records this timestamp when all items have been fulfilled or canceled, and all financial transactions are complete. Returns null if the order isn't closed.",
        "IS_NULLABLE": None,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "confirmationNumber",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A customer-facing order identifier, often shown instead of the sequential order name. It uses a random alphanumeric format (for example, XPAV284CT) and isn't guaranteed to be unique across orders.",
        "IS_NULLABLE": None,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "confirmed",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether inventory has been reserved for an order. Returns true if inventory quantities for an order's line items have been reserved.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "createdAt",
        "DATA_TYPE": "TIMESTAMP",
        "COLUMN_DESCRIPTION": "The date and time in ISO 8601 format when an order was created. This timestamp is set when the customer completes checkout and remains unchanged throughout an order's lifecycle.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "currencyCode",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The shop currency when the order was placed. For example, USD or CAD.",
        "IS_NULLABLE": False,
    },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "currentCartDiscountAmountSet",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The current total of all discounts applied to the entire order, after returns, refunds, order edits, and cancellations. This includes discount codes, automatic discounts, and other promotions that affect the whole order rather than individual line items.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "currentShippingPriceSet",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The current shipping price after applying refunds and discounts. If the parent order.taxesIncluded field is true, then this price includes taxes. Otherwise, this field is the pre-tax price.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "currentSubtotalLineItemsQuantity",
        "DATA_TYPE": "INT",
        "COLUMN_DESCRIPTION": "The current sum of the quantities for all line items that contribute to the order's subtotal price, after returns, refunds, order edits, and cancellations.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "currentSubtotalPriceSet",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The total price of the order, after returns and refunds, in shop and presentment currencies. This includes taxes and discounts.",
        "IS_NULLABLE": False,
    },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "currentTaxLines",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A list of all tax lines applied to line items on the order, after returns. Tax line prices represent the total price for all tax lines with the same rate and title.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "currentTotalAdditionalFeesSet",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The current total of all additional fees for an order, after any returns or modifications. Modifications include returns, refunds, order edits, and cancellations. Additional fees can include charges such as duties, import fees, and special handling.",
        "IS_NULLABLE": None,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "currentTotalDiscountsSet",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The total amount discounted on the order after returns and refunds, in shop and presentment currencies. This includes both order and line level discounts.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "currentTotalDutiesSet",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The current total duties amount for an order, after any returns or modifications. Modifications include returns, refunds, order edits, and cancellations.",
        "IS_NULLABLE": None,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "currentTotalPriceSet",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The total price of the order, after returns, in shop and presentment currencies. This includes taxes and discounts.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "currentTotalTaxSet",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The sum of the prices of all tax lines applied to line items on the order, after returns and refunds, in shop and presentment currencies.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "currentTotalWeight",
        "DATA_TYPE": "INT",
        "COLUMN_DESCRIPTION": "The total weight of the order after returns and refunds, in grams.",
        "IS_NULLABLE": False,
    },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "customAttributes",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A list of additional information that has been attached to the order. For example, gift message, delivery instructions, or internal notes.",
    #     "IS_NULLABLE": False
    # },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "customer",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The customer who placed an order. Returns null if an order was created through a checkout without customer authentication, such as a guest checkout.",
    #     "IS_NULLABLE": None
    # },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "customerId",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "ID of the customer.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "customerAcceptsMarketing",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether the customer agreed to receive marketing emails at the time of purchase. Use this to ensure compliance with marketing consent laws and to segment customers for email campaigns.",
        "IS_NULLABLE": False,
    },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "customerJourneySummary",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The customer's visits and interactions with the online store before placing the order. Use this to understand customer behavior, attribution sources, and marketing effectiveness to optimize your sales funnel.",
    #     "IS_NULLABLE": None
    # },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "customerLocale",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The customer's language and region preference at the time of purchase. For example, en for English, fr-CA for French (Canada), or es-MX for Spanish (Mexico). Use this to provide localized customer service and targeted marketing in the customer's preferred language.",
        "IS_NULLABLE": None,
    },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "discountApplications",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A list of discounts that are applied to the order, excluding order edits and refunds. Includes discount codes, automatic discounts, and other promotions that reduce the order total.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "discountCode",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The discount code used for an order. Returns null if no discount code was applied.",
        "IS_NULLABLE": None,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "discountCodes",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The discount codes used for the order. Multiple codes can be applied to a single order.",
        "IS_NULLABLE": False,
    },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "displayAddress",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The primary address of the customer, prioritizing shipping address over billing address when both are available. Returns null if neither shipping address nor billing address was provided.",
    #     "IS_NULLABLE": None
    # },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "displayFinancialStatus",
    #     "DATA_TYPE": "TEXT",
    #     "COLUMN_DESCRIPTION": "An order's financial status for display in the Shopify admin.",
    #     "IS_NULLABLE": None
    # },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "displayFulfillmentStatus",
    #     "DATA_TYPE": "TEXT",
    #     "COLUMN_DESCRIPTION": "The order's fulfillment status that displays in the Shopify admin to merchants. For example, an order might be unfulfilled or scheduled.",
    #     "IS_NULLABLE": False
    # },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "disputes",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A list of payment disputes associated with the order, such as chargebacks or payment inquiries. Disputes occur when customers challenge transactions with their bank or payment provider.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "dutiesIncluded",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether duties are included in the subtotal price of the order. Duties are import taxes charged by customs authorities when goods cross international borders.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "edited",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether the order has had any edits applied. For example, adding or removing line items, updating quantities, or changing prices.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "email",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The email address associated with the customer for this order. Used for sending order confirmations, shipping notifications, and other order-related communications. Returns null if no email address was provided during checkout.",
        "IS_NULLABLE": None,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "estimatedTaxes",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether taxes on the order are estimated. This field returns false when taxes on the order are finalized and aren't subject to any changes.",
        "IS_NULLABLE": False,
    },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "events",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A list of events associated with the order. Events track significant changes and activities related to the order, such as creation, payment, fulfillment, and cancellation.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "fulfillable",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether there are line items that can be fulfilled. This field returns false when the order has no fulfillable line items.",
        "IS_NULLABLE": False,
    },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "fulfillmentOrders",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A list of fulfillment orders for an order. Each fulfillment order groups line items that are fulfilled together, allowing an order to be processed in parts if needed.",
    #     "IS_NULLABLE": False
    # },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "fulfillments",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A list of shipments for the order. Fulfillments represent the physical shipment of products to customers.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "fulfillmentsCount",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The total number of fulfillments for the order, including canceled ones.",
        "IS_NULLABLE": None,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "fullyPaid",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether the order has been paid in full. This field returns true when the total amount received equals or exceeds the order total.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "hasTimelineComment",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether the merchant has added a timeline comment to the order.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "id",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A globally-unique ID.",
        "IS_NULLABLE": False,
    },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "legacyResourceId",
    #     "DATA_TYPE": "INT",
    #     "COLUMN_DESCRIPTION": "The ID of the corresponding resource in the REST Admin API.",
    #     "IS_NULLABLE": False
    # },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "lineItems",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A list of the order's line items. Line items represent the individual products and quantities that make up the order.",
    #     "IS_NULLABLE": False
    # },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "localizedFields",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "List of localized fields for the resource.",
    #     "IS_NULLABLE": False
    # },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "merchantBusinessEntity",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The legal business structure that the merchant operates under for this order, such as an LLC, corporation, or partnership. Used for tax reporting, legal compliance, and determining which business entity is responsible for the order.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "merchantEditable",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether the order can be edited by the merchant. Returns false for orders that can't be modified, such as canceled orders or orders with specific payment statuses.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "merchantEditableErrors",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "A list of reasons why the order can't be edited. For example, canceled orders can't be edited.",
        "IS_NULLABLE": False,
    },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "merchantOfRecordApp",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The application acting as the Merchant of Record for the order. The Merchant of Record is responsible for tax collection and remittance.",
    #     "IS_NULLABLE": None
    # },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "metafield",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A custom field, including its namespace and key, that's associated with a Shopify resource for the purposes of adding and storing additional information.",
    #     "IS_NULLABLE": None
    # },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "metafields",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A list of custom fields that a merchant associates with a Shopify resource.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "name",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The unique identifier for the order that appears on the order page in the Shopify admin and the Order status page. For example, #1001, EN1001, or 1001-A. This value isn't unique across multiple stores.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "netPaymentSet",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The net payment for the order, based on the total amount received minus the total amount refunded, in shop and presentment currencies.",
        "IS_NULLABLE": False,
    },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "nonFulfillableLineItems",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A list of line items that can't be fulfilled. For example, tips and fully refunded line items can't be fulfilled.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "note",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The note associated with the order. Contains additional information or instructions added by merchants or customers during the order process. Commonly used for special delivery instructions, gift messages, or internal processing notes.",
        "IS_NULLABLE": None,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "number",
        "DATA_TYPE": "INT",
        "COLUMN_DESCRIPTION": "The order number used to generate the name using the store's configured order number prefix/suffix. This number isn't guaranteed to follow a consecutive integer sequence, nor is it guaranteed to be unique across multiple stores, or even for a single store.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "originalTotalAdditionalFeesSet",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The total amount of all additional fees, such as import fees or taxes, that were applied when an order was created. Returns null if additional fees aren't applicable.",
        "IS_NULLABLE": None,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "originalTotalDutiesSet",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The total amount of duties calculated when an order was created, before any modifications. Modifications include returns, refunds, order edits, and cancellations.",
        "IS_NULLABLE": None,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "originalTotalPriceSet",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The total price of the order at the time of order creation, in shop and presentment currencies. Use this to compare the original order value against the current total after edits, returns, or refunds.",
        "IS_NULLABLE": False,
    },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "paymentCollectionDetails",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The payment collection details for the order, including payment status, outstanding amounts, and collection information. Use this to understand when and how payments should be collected, especially for orders with deferred or installment payment terms.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "paymentGatewayNames",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "A list of the names of all payment gateways used for the order. For example, Shopify Payments and Cash on Delivery (COD).",
        "IS_NULLABLE": False,
    },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "paymentTerms",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The payment terms associated with the order, such as net payment due dates or early payment discounts. Payment terms define when and how an order should be paid. Returns null if no specific payment terms were set for the order.",
    #     "IS_NULLABLE": None
    # },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "phone",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The phone number associated with the customer for this order. Useful for contacting customers about shipping updates, delivery notifications, or order issues. Returns null if no phone number was provided during checkout.",
        "IS_NULLABLE": None,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "poNumber",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The purchase order (PO) number that's associated with an order. This is typically provided by business customers who require a PO number for their procurement.",
        "IS_NULLABLE": None,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "presentmentCurrencyCode",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The currency used by the customer when placing the order. For example, USD, EUR, or CAD. This may differ from the shop's base currency when serving international customers or using multi-currency pricing.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "processedAt",
        "DATA_TYPE": "TIMESTAMP",
        "COLUMN_DESCRIPTION": "The date and time in ISO 8601 format when the order was processed. This date and time might not match the date and time when the order was created.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "productNetwork",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether the customer also purchased items from other stores in the network.",
        "IS_NULLABLE": False,
    },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "publication",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The sales channel that the order was created from, such as the Online Store or Shopify POS.",
    #     "IS_NULLABLE": None
    # },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "purchasingEntity",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The business entity that placed the order, including company details and purchasing relationships. Used for B2B transactions to track which company or organization is responsible for the purchase and payment terms.",
    #     "IS_NULLABLE": None
    # },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "refundable",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether the order can be refunded based on its payment transactions. Returns false for orders with no eligible payment transactions, such as fully refunded orders or orders with non-refundable payment methods.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "refundDiscrepancySet",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The difference between the suggested and actual refund amount of all refunds that have been applied to the order. A positive value indicates a difference in the merchant's favor, and a negative value indicates a difference in the customer's favor.",
        "IS_NULLABLE": False,
    },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "refunds",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A list of refunds that have been applied to the order. Refunds represent money returned to customers for returned items, cancellations, or adjustments.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "registeredSourceUrl",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The URL of the source that the order originated from, if found in the domain registry. Returns null if the source URL isn't in the domain registry.",
        "IS_NULLABLE": None,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "requiresShipping",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether the order requires physical shipping to the customer. Returns false for digital-only orders (such as gift cards or downloadable products) and true for orders with physical products that need delivery.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "restockable",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether any line items on the order can be restocked into inventory. Returns false for digital products, custom items, or items that can't be resold.",
        "IS_NULLABLE": False,
    },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "retailLocation",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The physical location where a retail order is created or completed, except for draft POS orders completed using the mark as paid flow in the Shopify admin, which return null. Transactions associated with the order might have been processed at a different location.",
    #     "IS_NULLABLE": None
    # },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "returns",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The returns associated with the order. Contains information about items that customers have requested to return, including return reasons, status, and refund details.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "returnStatus",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The order's aggregated return status for display purposes. Indicates the overall state of returns for the order, helping merchants track and manage the return process.",
        "IS_NULLABLE": False,
    },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "risk",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The risk assessment summary for the order. Provides fraud analysis and risk scoring to help you identify potentially fraudulent orders.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "shippingAddress",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The shipping address where the order will be delivered. Contains the customer's delivery location for fulfillment and shipping label generation. Returns null for digital orders or orders that don't require shipping.",
        "IS_NULLABLE": None,
    },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "shippingLine",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A summary of all shipping costs on the order. Aggregates shipping charges, discounts, and taxes to provide a single view of delivery costs.",
    #     "IS_NULLABLE": None
    # },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "shippingLines",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The shipping methods applied to the order. Each shipping line represents a shipping option chosen during checkout, including the carrier, service level, and cost.",
    #     "IS_NULLABLE": False
    # },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "shopifyProtect",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The Shopify Protect details for the order, including fraud protection status and coverage information. Shopify Protect helps protect eligible orders against fraudulent chargebacks. Returns null if Shopify Protect is disabled for the shop or the order isn't eligible for protection.",
    #     "IS_NULLABLE": None
    # },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "sourceIdentifier",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A unique POS or third party order identifier. For example, 1234-12-1000 or 111-98567-54.",
        "IS_NULLABLE": None,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "sourceName",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The name of the source associated with the order, such as web, mobile_app, or pos. Use this field to identify the platform where the order was placed.",
        "IS_NULLABLE": None,
    },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "staffMember",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The staff member who created or is responsible for the order. Useful for tracking which team member handled phone orders, manual orders, or order modifications. Returns null for orders created directly by customers through the online store.",
    #     "IS_NULLABLE": None
    # },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "statusPageUrl",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The URL where customers can check their order's current status, including tracking information and delivery updates.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "subtotalLineItemsQuantity",
        "DATA_TYPE": "INT",
        "COLUMN_DESCRIPTION": "The sum of quantities for all line items that contribute to the order's subtotal price. This excludes quantities for items like tips, shipping costs, or gift cards that don't affect the subtotal.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "subtotalPriceSet",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The sum of the prices for all line items after discounts and before returns, in shop and presentment currencies. If taxesIncluded is true, then the subtotal also includes tax.",
        "IS_NULLABLE": None,
    },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "suggestedRefund",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A calculated refund suggestion for the order based on specified line items, shipping, and duties. Use this to preview refund amounts, taxes, and processing fees before creating an actual refund.",
    #     "IS_NULLABLE": None
    # },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "tags",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "A comma separated list of tags associated with the order. Updating tags overwrites any existing tags that were previously added to the order.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "taxesIncluded",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether taxes are included in the subtotal price of the order. When true, the subtotal and line item prices include tax amounts. When false, taxes are calculated and displayed separately.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "taxExempt",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether taxes are exempt on the order. Returns true for orders where the customer or business has a valid tax exemption, such as non-profit organizations or tax-free purchases.",
        "IS_NULLABLE": False,
    },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "taxLines",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A list of all tax lines applied to line items on the order, before returns. Tax line prices represent the total price for all tax lines with the same rate and title.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "test",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether the order is a test. Test orders are made using the Shopify Bogus Gateway or a payment provider with test mode enabled. A test order can't be converted into a real order and vice versa.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "totalCapturableSet",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The authorized amount that's uncaptured or undercaptured, in shop and presentment currencies. This amount isn't adjusted for returns.",
        "IS_NULLABLE": False,
    },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "totalCashRoundingAdjustment",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The total rounding adjustment applied to payments or refunds for an order involving cash payments. Applies to some countries where cash transactions are rounded to the nearest currency denomination.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "totalDiscountsSet",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The total amount discounted on the order before returns, in shop and presentment currencies. This includes both order and line level discounts.",
        "IS_NULLABLE": None,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "totalOutstandingSet",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The total amount not yet transacted for the order, in shop and presentment currencies. A positive value indicates a difference in the merchant's favor (payment from customer to merchant) and a negative value indicates a difference in the customer's favor (refund from merchant to customer).",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "totalPriceSet",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The total price of the order, before returns, in shop and presentment currencies. This includes taxes and discounts.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "totalReceivedSet",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The total amount received from the customer before returns, in shop and presentment currencies.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "totalRefundedSet",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The total amount that was refunded, in shop and presentment currencies.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "totalRefundedShippingSet",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The total amount of shipping that was refunded, in shop and presentment currencies.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "totalShippingPriceSet",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The total shipping costs returned to the customer, in shop and presentment currencies. This includes fees and any related discounts that were refunded.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "totalTaxSet",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The total tax amount before returns, in shop and presentment currencies.",
        "IS_NULLABLE": None,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "totalTipReceivedSet",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The sum of all tip amounts for the order, in shop and presentment currencies.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "totalWeight",
        "DATA_TYPE": "INT",
        "COLUMN_DESCRIPTION": "The total weight of the order before returns, in grams.",
        "IS_NULLABLE": None,
    },
    # {
    #     "TABLE_NAME": "orders",
    #     "COLUMN_NAME": "transactions",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A list of transactions associated with the order.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "transactionsCount",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The number of transactions associated with the order.",
        "IS_NULLABLE": None,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "unpaid",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether no payments have been made for the order.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "orders",
        "COLUMN_NAME": "updatedAt",
        "DATA_TYPE": "TIMESTAMP",
        "COLUMN_DESCRIPTION": "The date and time in ISO 8601 format when the order was last modified.",
        "IS_NULLABLE": False,
    },
]

# region add fields flattened from JSON
for field_name, field in Orders.aliases():
    if isinstance(field, DeepExtract):
        columns.append({
            "TABLE_NAME": "orders",
            "COLUMN_NAME": field_name,
            "DATA_TYPE": field.mysql_data_type,
            "COLUMN_DESCRIPTION": field.description,
            "IS_NULLABLE": None,
        })
# endregion
