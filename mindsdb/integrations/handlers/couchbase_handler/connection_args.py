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
    host={
        "type": ARG_TYPE.STR,
        "description": "--your-instance--.dp.cloud.couchbase.com or IP address of the Couchbase server.",
    },
    scope={
        "type": ARG_TYPE.STR,
        "description": 'The scope use in the query context in Couchbase server. If blank, scope will be "_default".',
    },
)
connection_args_example = OrderedDict(
    host="127.0.0.1", user="root", password="password", bucket="bucket"
)
