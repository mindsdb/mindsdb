from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    user={
        "type": ARG_TYPE.STR,
        "description": "The user name used to authenticate with the Couchbase server.",
    },
    password={
        "type": ARG_TYPE.PWD,
        "description": "The password to authenticate the user with the Couchbase server.",
        'secret': True
    },
    bucket={
        "type": ARG_TYPE.STR,
        "description": "The database/bucket name to use when connecting with the Couchbase server.",
    },
    connection_string={
        "type": ARG_TYPE.STR,
        "description": "the Connection string to specify the cluster endpoint.",
    },
    scope={
        "type": ARG_TYPE.STR,
        "description": 'The scope use in the query context in Couchbase server. If blank, scope will be "_default".',
    },
)
connection_args_example = OrderedDict(
    connection_string="couchbase://localhost", user="root", password="password", bucket="bucket"
)
