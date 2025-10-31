from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

connection_args = OrderedDict(
    access_token={
        "type": ARG_TYPE.STR,
        "description": "The access token for the HubSpot API.",
        "required": True,
        "label": "Access Token",
    }
)

connection_args_example = OrderedDict(access_token="your_access_token")
