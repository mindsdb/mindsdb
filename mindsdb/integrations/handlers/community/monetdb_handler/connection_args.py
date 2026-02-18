from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    host={
        "type": ARG_TYPE.STR,
        "description": "The host name or IP address of the MonetDB server/database.",
    },
    database={
        "type": ARG_TYPE.STR,
        "description": """
            The database name to use when connecting with the MonetDB server.
        """,
    },
    user={
        "type": ARG_TYPE.STR,
        "description": "The user name used to authenticate with the MonetDB server.",
    },
    password={
        "type": ARG_TYPE.PWD,
        "description": "The password to authenticate the user with the MonetDB server.",
        "secret": True,
    },
    port={
        "type": ARG_TYPE.INT,
        "description": "Specify port to connect MonetDB through TCP/IP",
    },
    schema_name={
        "type": ARG_TYPE.STR,
        "description": "Specify the schema name for Listing Table ",
    },
)

connection_args_example = OrderedDict(
    host="127.0.0.1",
    port=50000,
    password="monetdb",
    user="monetdb",
    schemaName="sys",
    database="demo",
)
