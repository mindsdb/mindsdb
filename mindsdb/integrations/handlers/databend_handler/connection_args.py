from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the Databend warehouse.'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password to authenticate the user with the Databend warehouse.',
        'secret': True
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'The database name to use when connecting with the Databend warehouse.'
    },
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the Databend warehouse. NOTE: use \'127.0.0.1\' instead of \'localhost\' to connect to local server.'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'The TCP/IP port of the ClickHouse server.'
    }
)

connection_args_example = OrderedDict(
    host='some-url.aws-us-east-2.default.databend.com',
    port=443,
    user='root',
    password='password',
    database='test_db'
)
