from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    url={
        'type': ARG_TYPE.STR,
        'description': 'The URL to the Phoenix Query Server.'
    },
    max_retries={
        'type': ARG_TYPE.INT,
        'description': 'The maximum number of retries in case there is a connection error.'
    },
    autocommit={
        'type': ARG_TYPE.BOOL,
        'description': 'The flag for switching the connection to autocommit mode.'
    },
    auth={
        'type': ARG_TYPE.STR,
        'description': 'An authentication configuration object as expected by the underlying python_requests and python_requests_gssapi library.'
    },
    authentication={
        'type': ARG_TYPE.STR,
        'description': 'An alternative way to specify the authentication mechanism that mimics the semantics of the JDBC drirver.'
    },
    avatica_user={
        'type': ARG_TYPE.STR,
        'description': 'The username for BASIC or DIGEST authentication. Use in conjunction with the authentication option.'
    },
    avatica_password={
        'type': ARG_TYPE.PWD,
        'description': 'The password for BASIC or DIGEST authentication. Use in conjunction with the authentication option.',
        'secret': True
    },
    user={
        'type': ARG_TYPE.STR,
        'description': 'If authentication is BASIC or DIGEST then alias for avatica_user. If authentication is NONE or SPNEGO then alias for do_as'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'If authentication is BASIC or DIGEST then alias for avatica_password.',
        'secret': True
    }
)

connection_args_example = OrderedDict(
    url='http://127.0.0.1:8765',
    autocommit=True
)
