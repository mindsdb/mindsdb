from enum import Enum


class MarketingEvents(Enum):
    # app = "app"
    channelHandle = "channelHandle"
    description = "description"
    endedAt = "endedAt"
    id = "id"
    # legacyResourceId = "legacyResourceId"
    manageUrl = "manageUrl"
    marketingChannelType = "marketingChannelType"
    previewUrl = "previewUrl"
    remoteId = "remoteId"
    scheduledToEndAt = "scheduledToEndAt"
    sourceAndMedium = "sourceAndMedium"
    startedAt = "startedAt"
    type = "type"
    utmCampaign = "utmCampaign"
    utmMedium = "utmMedium"
    utmSource = "utmSource"


columns = [
    # {
    #     "TABLE_NAME": "marketing_events",
    #     "COLUMN_NAME": "app",
    #     "DATA_TYPE": "JSON",
    #     "COLUMN_DESCRIPTION": "The app that the marketing event is attributed to.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "marketing_events",
        "COLUMN_NAME": "channelHandle",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The unique string identifier of the channel to which this activity belongs. For the correct handle for your channel, contact your partner manager.",
        "IS_NULLABLE": None
    },
    {
        "TABLE_NAME": "marketing_events",
        "COLUMN_NAME": "description",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A human-readable description of the marketing event.",
        "IS_NULLABLE": None
    },
    {
        "TABLE_NAME": "marketing_events",
        "COLUMN_NAME": "endedAt",
        "DATA_TYPE": "TIMESTAMP",
        "COLUMN_DESCRIPTION": "The date and time when the marketing event ended.",
        "IS_NULLABLE": None
    },
    {
        "TABLE_NAME": "marketing_events",
        "COLUMN_NAME": "id",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "A globally-unique ID.",
        "IS_NULLABLE": False
    },
    # {
    #     "TABLE_NAME": "marketing_events",
    #     "COLUMN_NAME": "legacyResourceId",
    #     "DATA_TYPE": "INT",
    #     "COLUMN_DESCRIPTION": "The ID of the corresponding resource in the REST Admin API.",
    #     "IS_NULLABLE": False
    # },
    {
        "TABLE_NAME": "marketing_events",
        "COLUMN_NAME": "manageUrl",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The URL where the marketing event can be managed.",
        "IS_NULLABLE": None
    },
    {
        "TABLE_NAME": "marketing_events",
        "COLUMN_NAME": "marketingChannelType",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The medium through which the marketing activity and event reached consumers. This is used for reporting aggregation.",
        "IS_NULLABLE": None
    },
    {
        "TABLE_NAME": "marketing_events",
        "COLUMN_NAME": "previewUrl",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The URL where the marketing event can be previewed.",
        "IS_NULLABLE": None
    },
    {
        "TABLE_NAME": "marketing_events",
        "COLUMN_NAME": "remoteId",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "An optional ID that helps Shopify validate engagement data.",
        "IS_NULLABLE": None
    },
    {
        "TABLE_NAME": "marketing_events",
        "COLUMN_NAME": "scheduledToEndAt",
        "DATA_TYPE": "TIMESTAMP",
        "COLUMN_DESCRIPTION": "The date and time when the marketing event is scheduled to end.",
        "IS_NULLABLE": None
    },
    {
        "TABLE_NAME": "marketing_events",
        "COLUMN_NAME": "sourceAndMedium",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "Where the MarketingEvent occurred and what kind of content was used. Because utmSource and utmMedium are often used interchangeably, this is based on a combination of marketingChannel, referringDomain, and type to provide a consistent representation for any given piece of marketing regardless of the app that created it.",
        "IS_NULLABLE": False
    },
    {
        "TABLE_NAME": "marketing_events",
        "COLUMN_NAME": "startedAt",
        "DATA_TYPE": "TIMESTAMP",
        "COLUMN_DESCRIPTION": "The date and time when the marketing event started.",
        "IS_NULLABLE": False
    },
    {
        "TABLE_NAME": "marketing_events",
        "COLUMN_NAME": "type",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The marketing event type.",
        "IS_NULLABLE": False
    },
    {
        "TABLE_NAME": "marketing_events",
        "COLUMN_NAME": "utmCampaign",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The name of the marketing campaign.",
        "IS_NULLABLE": None
    },
    {
        "TABLE_NAME": "marketing_events",
        "COLUMN_NAME": "utmMedium",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The medium that the marketing campaign is using. Example values: cpc, banner.",
        "IS_NULLABLE": None
    },
    {
        "TABLE_NAME": "marketing_events",
        "COLUMN_NAME": "utmSource",
        "DATA_TYPE": "TEXT",
        "COLUMN_DESCRIPTION": "The referrer of the marketing event. Example values: google, newsletter.",
        "IS_NULLABLE": None
    }
]