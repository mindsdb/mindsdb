from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    host={
        "type": ARG_TYPE.STR,
        "description": "The host name or IP address of the Oracle DB.",
    },
    port={
        "type": ARG_TYPE.INT,
        "description": "The TCP/IP port of the Oracle DB. Must be an integer. Default 1521.",
    },
    sid={
        "type": ARG_TYPE.STR,
        "description": "The site identifier of the Oracle DB. Either sid or service_name should be provided.",
    },
    service_name={
        "type": ARG_TYPE.STR,
        "description": "The name of the Oracle DB service. Either sid or service_name should be provided.",
    },
    user={
        "type": ARG_TYPE.STR,
        "description": "The user name used to authenticate against the Oracle DB.",
    },
    password={
        "type": ARG_TYPE.PWD,
        "description": "The password to authenticate the user against Oracle DB.",
        "secret": True
    },
    disable_oob={
        "type": ARG_TYPE.BOOL,
        "description": "Disable out-of-band breaks",
    },
    auth_mode={
        "type": ARG_TYPE.STR,
        "description": "Database privilege for connection",
    },
)

connection_args_example = OrderedDict(
    host="127.0.0.1",
    port=1521,
    user="admin",
    password="password",
    sid="ORCL",
)
