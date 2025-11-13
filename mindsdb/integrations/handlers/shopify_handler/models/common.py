from enum import Enum


class AliasesEnum(Enum):
    """A class to represent a Shopify GraphQL resource.
    It adds easy method to get the aliases of the Enum class.
    """
    @classmethod
    def aliases(cls):
        return ((name, field.value) for name, field in cls.__members__.items())


class Count(AliasesEnum):
    """A class to represent a Shopify GraphQL count.
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/Count
    """
    count = "count"
    precision = "precision"

class TaxonomyAttribute(AliasesEnum):
    """A class to represent a Shopify GraphQL taxonomy attribute.
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/TaxonomyAttribute
    """
    id = "id"

class TaxonomyChoiceListAttribute(AliasesEnum):
    """A class to represent a Shopify GraphQL taxonomy choice list attribute.
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/TaxonomyChoiceListAttribute
    """
    id = "id"
    name = "name"
    # values = Nodes()

class TaxonomyMeasurementAttribute(AliasesEnum):
    """A class to represent a Shopify GraphQL taxonomy measurement attribute.
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/TaxonomyMeasurementAttribute
    """
    id = "id"
    name = "name"
    # options = Attribute

class SEO(AliasesEnum):
    """A class to represent a Shopify GraphQL SEO.
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/SEO
    """
    description = "description"
    title = "title"

class TaxonomyCategory(AliasesEnum):
    """A class to represent a Shopify GraphQL taxonomy category.
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/TaxonomyCategory
    """
    ancestorIds = "ancestorIds"
    # attributes = Nodes(TaxonomyCategoryAttributeConnection)
    childrenIds = "childrenIds"
    fullName = "fullName"
    id = "id"
    name = "name"

class MoneyV2(AliasesEnum):
    """A class to represent a Shopify GraphQL money v2.
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/MoneyV2
    """
    amount = "amount"
    currencyCode = "currencyCode"


class ProductPriceRangeV2(AliasesEnum):
    """A class to represent a Shopify GraphQL product price range v2.
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/ProductPriceRangeV2
    Require `read_products` permission.
    """
    maxVariantPrice = MoneyV2
    minVariantPrice = MoneyV2

class ProductCompareAtPriceRange(AliasesEnum):
    """A class to represent a Shopify GraphQL product compare at price range.
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/ProductCompareAtPriceRange
    Require `read_products` permission.
    """
    maxVariantCompareAtPrice = MoneyV2
    minVariantCompareAtPrice = MoneyV2

class CollectionConnection(AliasesEnum):
    """A class to represent a Shopify GraphQL collection connection short.
    Reference: https://shopify.dev/docs/api/storefront/latest/connections/collectionconnection
    Just a subset of fields.
    """
    id = "id"
    title = "title"

class MailingAddress(AliasesEnum):
    """A class to represent a Shopify GraphQL mailing address.
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/MailingAddress
    """
    address1 = "address1"
    address2 = "address2"
    city = "city"
    company = "company"
    coordinatesValidated = "coordinatesValidated"
    country = "country"
    countryCodeV2 = "countryCodeV2"
    firstName = "firstName"
    formatted = "formatted"
    formattedArea = "formattedArea"
    id = "id"
    lastName = "lastName"
    latitude = "latitude"
    longitude = "longitude"
    name = "name"
    phone = "phone"
    province = "province"
    provinceCode = "provinceCode"
    timeZone = "timeZone"
    validationResultSummary = "validationResultSummary"
    zip = "zip"

class OrderCancellation(AliasesEnum):
    """A class to represent a Shopify GraphQL order cancellation.
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/OrderCancellation
    Require `read_orders` permission.
    """
    staffNote = "staffNote"

class MoneyBag(AliasesEnum):
    """A class to represent a Shopify GraphQL money bag.
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/MoneyBag
    """
    presentmentMoney = MoneyV2
    shopMoney = MoneyV2
