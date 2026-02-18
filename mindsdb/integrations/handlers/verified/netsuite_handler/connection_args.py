from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    account_id={
        "type": ARG_TYPE.STR,
        "description": "NetSuite account/realm ID (e.g. 123456_SB1)",
        "required": True,
        "label": "Account ID",
    },
    consumer_key={
        "type": ARG_TYPE.PWD,
        "description": "OAuth consumer key for the NetSuite integration",
        "required": True,
        "label": "Consumer Key",
        "secret": True,
    },
    consumer_secret={
        "type": ARG_TYPE.PWD,
        "description": "OAuth consumer secret for the NetSuite integration",
        "required": True,
        "label": "Consumer Secret",
        "secret": True,
    },
    token_id={
        "type": ARG_TYPE.PWD,
        "description": "Token ID generated for the integration role",
        "required": True,
        "label": "Token ID",
        "secret": True,
    },
    token_secret={
        "type": ARG_TYPE.PWD,
        "description": "Token secret generated for the integration role",
        "required": True,
        "label": "Token Secret",
        "secret": True,
    },
    rest_domain={
        "type": ARG_TYPE.URL,
        "description": "Optional REST domain override (defaults to https://<account_id>.suitetalk.api.netsuite.com)",
        "required": False,
        "label": "REST Domain",
    },
    record_types={
        "type": ARG_TYPE.STR,
        "description": "Comma separated NetSuite record types to expose (e.g. customer,item,salesOrder)",
        "required": False,
        "label": "Record Types",
    },
)

connection_args_example = OrderedDict(
    account_id="123456_SB1",
    consumer_key="ck_...",
    consumer_secret="cs_...",
    token_id="token_...",
    token_secret="token_secret_...",
    rest_domain="https://123456-sb1.suitetalk.api.netsuite.com",
    record_types="customer,item,salesorder",
)
