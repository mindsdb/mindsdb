from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the Vertica server.'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password to authenticate the user with the VERTICA server.',
        'secret': True
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'The database name to use when connecting with the VERTICA server.'
    },
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the VERTICA server. NOTE: use \'127.0.0.1\' instead of \'localhost\' to connect to local server.'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'The TCP/IP port of the VERTICA server. Must be an integer.'
    },
    schema_name={
        'type': ARG_TYPE.STR,
        'description': 'Table are listed according to schema name (it is optional). Note: Default value is "public"'
    }
)

connection_args_example = OrderedDict(
    host='127.0.0.1',
    port=5433,
    user='root',
    password='password',
    database='database',
    schema_name='xyz'
)
