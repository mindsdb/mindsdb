from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    fauna_secret={
        "type": ARG_TYPE.STR,
        "description": "faunadb secret",
        "required": True,
        "secret": True
    },
    fauna_scheme={
        "type": ARG_TYPE.STR,
        "description": "faunadb scheme(http/https)",
        "required": False,
    },
    fauna_domain={
        "type": ARG_TYPE.STR,
        "description": "faunadb instance domain",
        "required": False,
    },
    fauna_port={
        "type": ARG_TYPE.INT,
        "description": "faunadb instance port",
        "required": False,
    },
    fauna_endpoint={
        "type": ARG_TYPE.STR,
        "description": "faunadb instance endpoint",
        "required": False,
    },
)

connection_args_example = OrderedDict(
    fauna_scheme="https",
    fauna_domain="db.fauna.com",
    fauna_port=443,
)
