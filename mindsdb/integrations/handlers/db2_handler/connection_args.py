from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    host={
        "type": ARG_TYPE.STR,
        "description": "The hostname, IP address, or URL of the IBM Db2 database.",
        "required": True,
        "label": "Host"
    },
    database={
        "type": ARG_TYPE.STR,
        "description": "The name of the IBM Db2 database to connect to.",
        "required": True,
        "label": "Database"
    },
    user={
        "type": ARG_TYPE.STR,
        "description": "The username for the IBM Db2 database.",
        "required": True,
        "label": "User"
    },
    password={
        "type": ARG_TYPE.PWD,
        "description": "The password for the IBM Db2 database.",
        'secret': True,
        "required": True,
        "label": "Password"
    },
    port={
        "type": ARG_TYPE.INT,
        "description": "The port number for connecting to the IBM Db2 database. Default is `50000`",
        "required": False,
        "label": "Port"
    },
    schema={
        "type": ARG_TYPE.STR,
        "description": "The database schema to use within the IBM Db2 database.",
        "required": False,
        "label": "Schema"
    },
)

connection_args_example = OrderedDict(
    host="127.0.0.1",
    port="25000",
    password="1234",
    user="db2admin",
    schema="db2admin",
    database="BOOKS",
)
