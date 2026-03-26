from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    api_base={
        "type": ARG_TYPE.URL,
        "description": "The base URL of the BigCommerce instance/server.",
        "label": "Base URL",
        "required": True,
    },
    access_token={
        "type": ARG_TYPE.STR,
        "description": "The API token for the BigCommerce account.",
        "label": "Access Token",
        "required": True,
        "secret": True,
    },
)

connection_args_example = OrderedDict(
    api_base="https://api.bigcommerce.com/stores/0fh0fh0fh0/v3/",
    access_token="h0fhag1nyqag1ezme1nyqa",
)
