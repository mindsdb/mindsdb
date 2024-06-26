from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the Firebird server.'
    },
    database={
        'type': ARG_TYPE.STR,
        'description': """
            The database name to use when connecting with the Firebird server. NOTE: use double backslashes (\\) for the
            database path on a Windows machine.
        """
    },
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the Firebird server.'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password to authenticate the user with the Firebird server.',
        'secret': True
    }
)

connection_args_example = OrderedDict(
    host='localhost',
    database='/temp/test.db',
    user='sysdba',
    password='password'
)
