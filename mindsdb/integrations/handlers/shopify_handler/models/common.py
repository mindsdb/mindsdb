from enum import Enum

class Count(Enum):
    count = "count"
    precision = "precision"

class TaxonomyAttribute(Enum):
    id = "id"

class TaxonomyChoiceListAttribute(Enum):
    id = "id"
    name = "name"
    # values = Nodes()

class TaxonomyMeasurementAttribute(Enum):
    id = "id"
    name = "name"
    # options = Attribute

class SEO(Enum):
    description = "description"
    title = "title"

class TaxonomyCategory(Enum):
    ancestorIds = "ancestorIds"
    # attributes = Nodes(TaxonomyCategoryAttributeConnection)
    childrenIds = "childrenIds"
    fullName = "fullName"

class MoneyV2(Enum):
    amount = "amount"
    currencyCode = "currencyCode"


class ProductPriceRangeV2(Enum):
    maxVariantPrice = MoneyV2
    minVariantPrice = MoneyV2

class ProductCompareAtPriceRange(Enum):
    maxVariantCompareAtPrice = MoneyV2
    minVariantCompareAtPrice = MoneyV2

class CollectionConnectionShort(Enum):
    # just subset of fields
    id = "id"
    title = "title"

class MailingAddress(Enum):
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

class MoneyV2(Enum):
    amount = "amount"
    currencyCode = "currencyCode"