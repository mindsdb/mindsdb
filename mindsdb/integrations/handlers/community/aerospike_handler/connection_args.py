from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the Aerospike server.',
        'required': False,
        'label': 'User'
    },
    password={
        'type': ARG_TYPE.STR,
        'description': 'The password to authenticate the user with the Aerospike server.',
        'required': False,
        'label': 'Password',
        'secret': True
    },
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the Aerospike server. NOTE: use \'127.0.0.1\' instead of \'localhost\' to connect to local server.',
        'required': True,
        'label': 'Host'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'The TCP/IP port of the Aerospike server. Must be an integer.',
        'required': True,
        'label': 'Port'
    },
    namespace={
        'type': ARG_TYPE.STR,
        'description': 'The namespace name to use for the query in the Aerospike server.',
        'required': True,
        'label': 'namespace'
    }
)

connection_args_example = OrderedDict(
    user="demo_user",
    password="demo_password",
    host='127.0.0.1',
    port=3000,
    namespace="demo",
)
