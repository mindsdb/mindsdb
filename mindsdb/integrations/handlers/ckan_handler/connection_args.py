from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    url={
        "type": ARG_TYPE.STR,
        "description": "The URI-Like connection string to the CKAN server. If provided, it will override the other connection arguments.",
        "required": True,
        "label": "URL",
    },
    api_key={
        "type": ARG_TYPE.STR,
        "description": "The API key used to authenticate with the CKAN server. For CKAN 2.10+ API tokens are supported.",
        "required": False,
        "label": "API Key/Token",
    },
)


connection_args_example = OrderedDict(url="https://data.gov.au/data/", api_key="my_api_key")
