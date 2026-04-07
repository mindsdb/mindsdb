from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    shop_url={
        "type": ARG_TYPE.STR,
        "description": "Shop url (e.g. shop-123456.myshopify.com)",
        "required": True,
        "label": "Shop url",
    },
    access_token={
        "type": ARG_TYPE.PWD,
        "description": "Permanent access token for the Shopify Admin API. Use this instead of client_id/client_secret when you already have a token from a custom app.",
        "required": False,
        "label": "Access token",
        "secret": True,
    },
    client_id={
        "type": ARG_TYPE.STR,
        "description": "Client ID of the Shopify app (used to obtain an access token via OAuth client credentials flow).",
        "required": False,
        "label": "Client ID",
    },
    client_secret={
        "type": ARG_TYPE.PWD,
        "description": "Client secret of the Shopify app (used with client_id to obtain an access token).",
        "required": False,
        "label": "Client secret",
        "secret": True,
    },
)

connection_args_example = OrderedDict(
    shop_url="shop-123456.myshopify.com",
    access_token="shpat_xxxxxxxxxxxxxxxxxxxx",
)
