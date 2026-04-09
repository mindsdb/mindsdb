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
    refresh_token={
        "type": ARG_TYPE.PWD,
        "description": "Offline refresh token from Shopify OAuth. Used with client_id and client_secret to rotate expiring access tokens.",
        "required": False,
        "label": "Refresh token",
        "secret": True,
    },
    expires_at={
        "type": ARG_TYPE.STR,
        "description": "ISO 8601 timestamp of when the current access token expires (e.g. 2026-04-09T12:00:00+00:00).",
        "required": False,
        "label": "Token expiry",
    },
)

connection_args_example = OrderedDict(
    shop_url="shop-123456.myshopify.com",
    access_token="shpat_xxxxxxxxxxxxxxxxxxxx",
)
