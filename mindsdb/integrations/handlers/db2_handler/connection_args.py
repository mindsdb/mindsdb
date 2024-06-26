from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    host={
        "type": ARG_TYPE.STR,
        "description": "The host name or IP address of the DB2 server/database.",
    },
    database={
        "type": ARG_TYPE.STR,
        "description": """
            The database name to use when connecting with the DB2 server.
        """,
    },
    user={
        "type": ARG_TYPE.STR,
        "description": "The user name used to authenticate with the DB2 server.",
    },
    password={
        "type": ARG_TYPE.PWD,
        "description": "The password to authenticate the user with the DB2 server.",
        'secret': True
    },
    port={
        "type": ARG_TYPE.INT,
        "description": "Specify port to connect DB2 through TCP/IP",
    },
    schemaName={"type": ARG_TYPE.STR, "description": "Specify the schema name "},
)

connection_args_example = OrderedDict(
    host="127.0.0.1",
    port="25000",
    password="1234",
    user="db2admin",
    schemaName="db2admin",
    database="BOOKS",
)
