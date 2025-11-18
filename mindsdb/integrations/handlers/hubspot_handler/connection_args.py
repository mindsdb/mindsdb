from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

connection_args = OrderedDict(
    access_token={
        "type": ARG_TYPE.STR,
        "description": "The access token for the HubSpot API. Required for direct access token authentication.",
        "required": False,
        "label": "Access Token",
    },
    client_id={
        "type": ARG_TYPE.STR,
        "description": "The client ID (consumer key) from your HubSpot app for OAuth authentication.",
        "required": False,
        "label": "Client ID",
    },
    client_secret={
        "type": ARG_TYPE.PWD,
        "description": "The client secret (consumer secret) from your HubSpot app for OAuth authentication.",
        "secret": True,
        "required": False,
        "label": "Client Secret",
    },
)

connection_args_example = OrderedDict(
    access_token="your_access_token", client_id="your_client_id", client_secret="your_client_secret"
)
