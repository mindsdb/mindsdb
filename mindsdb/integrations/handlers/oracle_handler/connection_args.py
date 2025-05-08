from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    dsn={
        "type": ARG_TYPE.STR,
        "description": "The data source name (DSN) for the Oracle database.",
        "required": False,
        "label": "Data Source Name (DSN)",
    },
    host={
        "type": ARG_TYPE.STR,
        "description": "The hostname, IP address, or URL of the Oracle server.",
        "required": False,
        "label": "Host",
    },
    port={
        "type": ARG_TYPE.INT,
        "description": "The port number for connecting to the Oracle database. Default is 1521.",
        "required": False,
        "label": "Port",
    },
    sid={
        "type": ARG_TYPE.STR,
        "description": "The system identifier (SID) of the Oracle database.",
        "required": False,
        "label": "SID",
    },
    service_name={
        "type": ARG_TYPE.STR,
        "description": "The service name of the Oracle database.",
        "required": False,
        "label": "Service Name",
    },
    user={
        "type": ARG_TYPE.STR,
        "description": "The username for the Oracle database.",
        "required": True,
        "label": "User",
    },
    password={
        "type": ARG_TYPE.PWD,
        "description": "The password for the Oracle database.",
        "secret": True,
        "required": True,
        "label": "Password",
    },
    disable_oob={
        "type": ARG_TYPE.BOOL,
        "description": "The boolean parameter to disable out-of-band breaks. Default is `false`.",
        "required": False,
        "label": "Disable OOB",
    },
    auth_mode={
        "type": ARG_TYPE.STR,
        "description": "The authorization mode to use.",
        "required": False,
        "label": "Auth Mode",
    },
    thick_mode={
        "type": ARG_TYPE.BOOL,
        "description": "Set to `true` to use thick mode for the connection. Thin mode is used by default.",
        "required": False,
        "label": "Connection mode",
    }
)

connection_args_example = OrderedDict(
    host="127.0.0.1",
    port=1521,
    user="admin",
    password="password",
    sid="ORCL",
)
