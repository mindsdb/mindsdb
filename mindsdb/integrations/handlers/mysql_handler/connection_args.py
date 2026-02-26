from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    url={
        "type": ARG_TYPE.STR,
        "description": "The URI-Like connection string to the MySQL server. If provided, it will override the other connection arguments.",
        "required": False,
        "label": "URL",
    },
    user={
        "type": ARG_TYPE.STR,
        "description": "The user name used to authenticate with the MySQL server.",
        "required": True,
        "label": "User",
    },
    password={
        "type": ARG_TYPE.PWD,
        "description": "The password to authenticate the user with the MySQL server.",
        "required": True,
        "label": "Password",
        "secret": True,
    },
    database={
        "type": ARG_TYPE.STR,
        "description": "The database name to use when connecting with the MySQL server.",
        "required": True,
        "label": "Database",
    },
    host={
        "type": ARG_TYPE.STR,
        "description": "The host name or IP address of the MySQL server. NOTE: use '127.0.0.1' instead of 'localhost' to connect to local server.",
        "required": True,
        "label": "Host",
    },
    port={
        "type": ARG_TYPE.INT,
        "description": "The TCP/IP port of the MySQL server. Must be an integer.",
        "required": True,
        "label": "Port",
    },
    ssl={
        "type": ARG_TYPE.BOOL,
        "description": "Set it to True to enable ssl.",
        "required": False,
        "label": "ssl",
    },
    ssl_ca={
        "type": ARG_TYPE.PATH,
        "description": "Path or URL of the Certificate Authority (CA) certificate file",
        "required": False,
        "label": "ssl_ca",
    },
    ssl_cert={
        "type": ARG_TYPE.PATH,
        "description": "Path name or URL of the server public key certificate file",
        "required": False,
        "label": "ssl_cert",
    },
    ssl_key={
        "type": ARG_TYPE.PATH,
        "description": "The path name or URL of the server private key file",
        "required": False,
        "label": "ssl_key",
    },
)

connection_args_example = OrderedDict(
    host="127.0.0.1", port=3306, user="root", password="password", database="database"
)
