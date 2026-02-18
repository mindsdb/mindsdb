from .common import (
    AliasesEnum,
    Count,
    TaxonomyCategory,
    CollectionConnection,
    ProductCompareAtPriceRange,
    ProductPriceRangeV2,
    SEO,
)
from .utils import Nodes


class Products(AliasesEnum):
    """A class to represent a Shopify GraphQL product.
    Reference: https://shopify.dev/docs/api/admin-graphql/latest/objects/Product
    Require `read_products` permission.
    """

    availablePublicationsCount = Count
    # bundleComponents
    category = TaxonomyCategory
    collections = Nodes(CollectionConnection)
    # combinedListing = CombinedListing
    combinedListingRole = "combinedListingRole"
    compareAtPriceRange = ProductCompareAtPriceRange
    # contextualPricing = ProductContextualPricing  # require context
    createdAt = "createdAt"
    # defaultCursor = "defaultCursor"
    description = "description"
    descriptionHtml = "descriptionHtml"
    # events = Nodes(EventConnection)
    # featuredMedia = Media
    # feedback = ResourceFeedback
    giftCardTemplateSuffix = "giftCardTemplateSuffix"
    handle = "handle"
    hasOnlyDefaultVariant = "hasOnlyDefaultVariant"
    hasOutOfStockVariants = "hasOutOfStockVariants"
    hasVariantsThatRequiresComponents = "hasVariantsThatRequiresComponents"
    id = "id"
    # inCollection = "inCollection" # require id
    isGiftCard = "isGiftCard"
    legacyResourceId = "legacyResourceId"
    # media = Nodes(MediaConnection)
    mediaCount = Count
    # metafield = Metafield  # require key
    # metafields = Nodes(MetafieldConnection)
    onlineStorePreviewUrl = "onlineStorePreviewUrl"
    onlineStoreUrl = "onlineStoreUrl"
    # options = List(ProductOption)
    priceRangeV2 = ProductPriceRangeV2
    # productComponents = Nodes(ProductComponentType)
    productComponentsCount = Count
    # productParents = Nodes(Product)
    productType = "productType"
    publishedAt = "publishedAt"
    # # publishedInContext = "publishedInContext"  # require context
    # publishedOnCurrentPublication = "publishedOnCurrentPublication"   # raise error if publication not exists
    # publishedOnPublication = "publishedOnPublication"  # require id
    requiresSellingPlan = "requiresSellingPlan"
    # resourcePublicationOnCurrentPublication = ResourcePublicationV2
    # resourcePublications = Nodes(ResourcePublication)
    resourcePublicationsCount = Count
    # resourcePublicationsV2 = Nodes(ResourcePublicationV2)
    # restrictedForResource = RestrictedForResource(calculatedOrderIdcalculatedOrderId=!required)
    # sellingPlanGroups = Nodes(SellingPlanGroupConnection)
    sellingPlanGroupsCount = Count
    seo = SEO
    status = "status"
    tags = "tags"
    templateSuffix = "templateSuffix"
    title = "title"
    totalInventory = "totalInventory"
    tracksInventory = "tracksInventory"
    # translations = Translation(locale=!required)
    # unpublishedPublications = Nodes(Publication)
    updatedAt = "updatedAt"
    # variants = Nodes(ProductVariant)
    variantsCount = Count
    vendor = "vendor"


columns = [
    {
        "TABLE_NAME": "products",
        "COLUMN_NAME": "availablePublicationsCount",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The number of publications that a resource is published to, without feedback errors.",
        "IS_NULLABLE": None,
    },
    # {
    #     "TABLE_NAME": "products",
    #     "COLUMN_NAME": "bundleComponents",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A list of components that are associated with a product in a bundle.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "products",
        "COLUMN_NAME": "category",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The category of a product from Shopify's Standard Product Taxonomy.",
        "IS_NULLABLE": None,
    },
    {
        "TABLE_NAME": "products",
        "COLUMN_NAME": "collections",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "A list of collections that include the product.",
        "IS_NULLABLE": False,
    },
    # {
    #     "TABLE_NAME": "products",
    #     "COLUMN_NAME": "combinedListing",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A special product type that combines separate products from a store into a single product listing. Combined listings are connected by a shared option, such as color, model, or dimension.",
    #     "IS_NULLABLE": None
    # },
    {
        "TABLE_NAME": "products",
        "COLUMN_NAME": "combinedListingRole",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The role of the product in a combined listing. If null, then the product isn't part of any combined listing.",
        "IS_NULLABLE": None,
    },
    {
        "TABLE_NAME": "products",
        "COLUMN_NAME": "compareAtPriceRange",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The compare-at price range of the product in the shop's default currency.",
        "IS_NULLABLE": None,
    },
    # {
    #     "TABLE_NAME": "products",
    #     "COLUMN_NAME": "contextualPricing",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The pricing that applies to a customer in a specific context. For example, a price might vary depending on the customer's location. Only active markets are considered in the price resolution.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "products",
        "COLUMN_NAME": "createdAt",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The date and time when the product was created.",
        "IS_NULLABLE": False,
    },
    # {
    #     "TABLE_NAME": "products",
    #     "COLUMN_NAME": "defaultCursor",
    #     "DATA_TYPE": "TEXT",
    #     "COLUMN_DESCRIPTION": "A default cursor that returns the single next record, sorted ascending by ID.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "products",
        "COLUMN_NAME": "description",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A single-line description of the product, with HTML tags removed.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "products",
        "COLUMN_NAME": "descriptionHtml",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The description of the product, with HTML tags. For example, the description might include bold and italic text.",
        "IS_NULLABLE": False,
    },
    # {
    #     "TABLE_NAME": "products",
    #     "COLUMN_NAME": "events",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The paginated list of events associated with the host subject.",
    #     "IS_NULLABLE": False
    # },
    # {
    #     "TABLE_NAME": "products",
    #     "COLUMN_NAME": "featuredMedia",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The featured media associated with the product.",
    #     "IS_NULLABLE": None
    # },
    # {
    #     "TABLE_NAME": "products",
    #     "COLUMN_NAME": "feedback",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The information that lets merchants know what steps they need to take to make sure that the app is set up correctly. For example, if a merchant hasn't set up a product correctly in the app, then the feedback might include a message that says You need to add a price to this product.",
    #     "IS_NULLABLE": None
    # },
    {
        "TABLE_NAME": "products",
        "COLUMN_NAME": "giftCardTemplateSuffix",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The theme template that's used when customers view the gift card in a store.",
        "IS_NULLABLE": None,
    },
    {
        "TABLE_NAME": "products",
        "COLUMN_NAME": "handle",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A unique, human-readable string of the product's title. A handle can contain letters, hyphens, and numbers, but no spaces. The handle is used in the online store URL for the product.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "products",
        "COLUMN_NAME": "hasOnlyDefaultVariant",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether the product has only a single variant with the default option and value.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "products",
        "COLUMN_NAME": "hasOutOfStockVariants",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether the product has variants that are out of stock.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "products",
        "COLUMN_NAME": "hasVariantsThatRequiresComponents",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether at least one of the product variants requires bundle components. Learn more about store eligibility for bundles.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "products",
        "COLUMN_NAME": "id",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A globally-unique ID.",
        "IS_NULLABLE": False,
    },
    # {
    #     "TABLE_NAME": "products",
    #     "COLUMN_NAME": "inCollection",
    #     "DATA_TYPE": "BOOLEAN",
    #     "COLUMN_DESCRIPTION": "Whether the product is in a specified collection.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "products",
        "COLUMN_NAME": "isGiftCard",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether the product is a gift card.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "products",
        "COLUMN_NAME": "legacyResourceId",
        "DATA_TYPE": "INT",
        "COLUMN_DESCRIPTION": "The ID of the corresponding resource in the REST Admin API.",
        "IS_NULLABLE": False,
    },
    # {
    #     "TABLE_NAME": "products",
    #     "COLUMN_NAME": "media",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The media associated with the product. Valid media are images, 3D models, videos.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "products",
        "COLUMN_NAME": "mediaCount",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The total count of media that's associated with a product.",
        "IS_NULLABLE": None,
    },
    # {
    #     "TABLE_NAME": "products",
    #     "COLUMN_NAME": "metafield",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A custom field, including its namespace and key, that's associated with a Shopify resource for the purposes of adding and storing additional information.",
    #     "IS_NULLABLE": None
    # },
    # {
    #     "TABLE_NAME": "products",
    #     "COLUMN_NAME": "metafields",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A list of custom fields that a merchant associates with a Shopify resource.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "products",
        "COLUMN_NAME": "onlineStorePreviewUrl",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The preview URL for the online store.",
        "IS_NULLABLE": None,
    },
    {
        "TABLE_NAME": "products",
        "COLUMN_NAME": "onlineStoreUrl",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The product's URL on the online store. If null, then the product isn't published to the online store sales channel.",
        "IS_NULLABLE": None,
    },
    # {
    #     "TABLE_NAME": "products",
    #     "COLUMN_NAME": "options",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A list of product options. The limit is defined by the shop's resource limits for product options.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "products",
        "COLUMN_NAME": "priceRangeV2",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The minimum and maximum prices of a product, expressed in decimal numbers. For example, if the product is priced between $10.00 and $50.00, then the price range is $10.00 - $50.00.",
        "IS_NULLABLE": False,
    },
    # {
    #     "TABLE_NAME": "products",
    #     "COLUMN_NAME": "productComponents",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A list of products that contain at least one variant associated with at least one of the current products' variants via group relationship.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "products",
        "COLUMN_NAME": "productComponentsCount",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "A count of unique products that contain at least one variant associated with at least one of the current products' variants via group relationship.",
        "IS_NULLABLE": None,
    },
    # {
    #     "TABLE_NAME": "products",
    #     "COLUMN_NAME": "productParents",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A list of products that has a variant that contains any of this product's variants as a component.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "products",
        "COLUMN_NAME": "productType",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The product type that merchants define.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "products",
        "COLUMN_NAME": "publishedAt",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The date and time when the product was published to the online store.",
        "IS_NULLABLE": None,
    },
    # {
    #     "TABLE_NAME": "products",
    #     "COLUMN_NAME": "publishedInContext",
    #     "DATA_TYPE": "BOOLEAN",
    #     "COLUMN_DESCRIPTION": "Whether the product is published for a customer only in a specified context. For example, a product might be published for a customer only in a specific location.",
    #     "IS_NULLABLE": False
    # },
    # {
    #     "TABLE_NAME": "products",
    #     "COLUMN_NAME": "publishedOnCurrentPublication",
    #     "DATA_TYPE": "BOOLEAN",
    #     "COLUMN_DESCRIPTION": "Whether the resource is published to the app's publication. For example, the resource might be published to the app's online store channel.",
    #     "IS_NULLABLE": False
    # },
    # {
    #     "TABLE_NAME": "products",
    #     "COLUMN_NAME": "publishedOnPublication",
    #     "DATA_TYPE": "BOOLEAN",
    #     "COLUMN_DESCRIPTION": "Whether the resource is published to a specified publication.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "products",
        "COLUMN_NAME": "requiresSellingPlan",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether the product can only be purchased with a selling plan. Products that are sold on subscription can be updated only for online stores. If you update a product to be subscription-only, then the product is unpublished from all channels, except the online store.",
        "IS_NULLABLE": False,
    },
    # {
    #     "TABLE_NAME": "products",
    #     "COLUMN_NAME": "resourcePublicationOnCurrentPublication",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The resource that's either published or staged to be published to the publication.",
    #     "IS_NULLABLE": None
    # },
    # {
    #     "TABLE_NAME": "products",
    #     "COLUMN_NAME": "resourcePublications",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The list of resources that are published to a publication.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "products",
        "COLUMN_NAME": "resourcePublicationsCount",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The number of publications that a resource is published to, without feedback errors.",
        "IS_NULLABLE": None,
    },
    # {
    #     "TABLE_NAME": "products",
    #     "COLUMN_NAME": "resourcePublicationsV2",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The list of resources that are either published or staged to be published to a publication.",
    #     "IS_NULLABLE": False
    # },
    # {
    #     "TABLE_NAME": "products",
    #     "COLUMN_NAME": "restrictedForResource",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "Whether the merchant can make changes to the product when they edit the order associated with the product. For example, a merchant might be restricted from changing product details when they edit an order.",
    #     "IS_NULLABLE": None
    # },
    # {
    #     "TABLE_NAME": "products",
    #     "COLUMN_NAME": "sellingPlanGroups",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A list of all selling plan groups that are associated with the product either directly, or through the product's variants.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "products",
        "COLUMN_NAME": "sellingPlanGroupsCount",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "A count of selling plan groups that are associated with the product.",
        "IS_NULLABLE": None,
    },
    {
        "TABLE_NAME": "products",
        "COLUMN_NAME": "seo",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The SEO title and description that are associated with a product.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "products",
        "COLUMN_NAME": "status",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The product status, which controls visibility across all sales channels.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "products",
        "COLUMN_NAME": "tags",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "A comma-separated list of searchable keywords that are associated with the product. For example, a merchant might apply the sports and summer tags to products that are associated with sportwear for summer. Updating tags overwrites any existing tags that were previously added to the product. To add new tags without overwriting existing tags, use the tagsAdd mutation.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "products",
        "COLUMN_NAME": "templateSuffix",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The theme template that's used when customers view the product in a store.",
        "IS_NULLABLE": None,
    },
    {
        "TABLE_NAME": "products",
        "COLUMN_NAME": "title",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The name for the product that displays to customers. The title is used to construct the product's handle. For example, if a product is titled Black Sunglasses, then the handle is black-sunglasses.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "products",
        "COLUMN_NAME": "totalInventory",
        "DATA_TYPE": "INT",
        "COLUMN_DESCRIPTION": "The quantity of inventory that's in stock.",
        "IS_NULLABLE": False,
    },
    {
        "TABLE_NAME": "products",
        "COLUMN_NAME": "tracksInventory",
        "DATA_TYPE": "BOOLEAN",
        "COLUMN_DESCRIPTION": "Whether inventory tracking has been enabled for the product.",
        "IS_NULLABLE": False,
    },
    # {
    #     "TABLE_NAME": "products",
    #     "COLUMN_NAME": "translations",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The published translations associated with the resource.",
    #     "IS_NULLABLE": False
    # },
    # {
    #     "TABLE_NAME": "products",
    #     "COLUMN_NAME": "unpublishedPublications",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The list of publications that the resource isn't published to.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "products",
        "COLUMN_NAME": "updatedAt",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The date and time when the product was last modified. A product's updatedAt value can change for different reasons. For example, if an order is placed for a product that has inventory tracking set up, then the inventory adjustment is counted as an update.",
        "IS_NULLABLE": False,
    },
    # {
    #     "TABLE_NAME": "products",
    #     "COLUMN_NAME": "variants",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "A list of variants associated with the product. If querying a single product at the root, you can fetch up to 2048 variants.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "products",
        "COLUMN_NAME": "variantsCount",
        "DATA_TYPE": "JSON",
        "COLUMN_DESCRIPTION": "The number of variants that are associated with the product.",
        "IS_NULLABLE": None,
    },
    {
        "TABLE_NAME": "products",
        "COLUMN_NAME": "vendor",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The name of the product's vendor.",
        "IS_NULLABLE": False,
    },
]
