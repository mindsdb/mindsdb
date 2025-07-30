from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    user={
        "type": ARG_TYPE.STR,
        "description": "The user name used to authenticate with the Denodo server.",
        "required": True,
        "label": "User",
    },
    password={
        "type": ARG_TYPE.PWD,
        "description": "The password to authenticate the user with the Denodo server.",
        "required": True,
        "label": "Password",
        "secret": True,
    },
    database={
        "type": ARG_TYPE.STR,
        "description": "The database name to use when connecting with the Denodo server.",
        "required": True,
        "label": "Database",
    },
    host={
        "type": ARG_TYPE.STR,
        "description": "The host name or IP address of the Denodo server.",
        "required": True,
        "label": "Host",
    },
    port={
        "type": ARG_TYPE.INT,
        "description": "The TCP/IP port of the Denodo server. Must be an integer.",
        "required": True,
        "label": "Port",
    },
)

connection_args_example = {
    "host": "localhost",
    "port": 9996,
    "user": "admin",
    "password": "password",
    "database": "database",
}
