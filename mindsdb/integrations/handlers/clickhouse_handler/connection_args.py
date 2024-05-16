from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    protocol={
        'type': ARG_TYPE.STR,
        'description': 'The protocol to query clickhouse. Supported: native, http, https. Default: native',
        'required': False,
        'label': 'Protocol'
    },
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the ClickHouse server.',
        'required': True,
        'label': 'User'
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'The database name to use when connecting with the ClickHouse server.',
        'required': True,
        'label': 'Database name'
    },
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the ClickHouse server. NOTE: use \'127.0.0.1\' instead of \'localhost\' to connect to local server.',
        'required': True,
        'label': 'Host'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'The TCP/IP port of the ClickHouse server. Must be an integer.',
        'required': True,
        'label': 'Port'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password to authenticate the user with the ClickHouse server.',
        'required': True,
        'label': 'Password',
        'secret': True
    }
)

connection_args_example = OrderedDict(
    protocol='native',
    host='127.0.0.1',
    port=9000,
    user='root',
    password='password',
    database='database'
)
