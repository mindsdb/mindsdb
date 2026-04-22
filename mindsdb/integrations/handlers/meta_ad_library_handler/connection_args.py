from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    access_token={
        "type": ARG_TYPE.STR,
        "description": "Meta Ad Library access token.",
        "label": "Access Token",
        "required": True,
        "secret": True,
    },
    search_page_ids={
        "type": ARG_TYPE.STR,
        "description": "Optional JSON array of Meta page IDs used to scope the archive query.",
        "label": "Search Page IDs",
        "required": False,
    },
    search_terms={
        "type": ARG_TYPE.STR,
        "description": "Optional search term used when page IDs are not provided.",
        "label": "Search Terms",
        "required": False,
    },
    ad_reached_countries={
        "type": ARG_TYPE.STR,
        "description": "Optional JSON array of reached countries. Defaults to [\"ALL\"].",
        "label": "Reached Countries",
        "required": False,
    },
    ad_type={
        "type": ARG_TYPE.STR,
        "description": "Meta ad type filter. Defaults to ALL.",
        "label": "Ad Type",
        "required": False,
    },
    ad_active_status={
        "type": ARG_TYPE.STR,
        "description": "Meta active status filter. Defaults to ALL.",
        "label": "Ad Active Status",
        "required": False,
    },
    search_type={
        "type": ARG_TYPE.STR,
        "description": "Meta search type. Defaults to KEYWORD_UNORDERED.",
        "label": "Search Type",
        "required": False,
    },
    languages={
        "type": ARG_TYPE.STR,
        "description": "Optional JSON array of language codes.",
        "label": "Languages",
        "required": False,
    },
    publisher_platforms={
        "type": ARG_TYPE.STR,
        "description": "Optional JSON array of publisher platforms.",
        "label": "Publisher Platforms",
        "required": False,
    },
    unmask_removed_content={
        "type": ARG_TYPE.BOOL,
        "description": "Whether removed content should be unmasked. Defaults to false.",
        "label": "Unmask Removed Content",
        "required": False,
    },
    api_version={
        "type": ARG_TYPE.STR,
        "description": "Meta Graph API version used for requests. Defaults to v24.0.",
        "label": "API Version",
        "required": False,
    },
)

connection_args_example = OrderedDict(
    access_token="your_access_token_here",
    search_page_ids='["257702164651631"]',
    ad_reached_countries='["ALL"]',
    ad_type="ALL",
    ad_active_status="ALL",
    search_type="KEYWORD_UNORDERED",
    api_version="v24.0",
)
