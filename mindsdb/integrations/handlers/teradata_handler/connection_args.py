from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    host={
        'type': ARG_TYPE.STR,
        'description': 'The hostname, IP address, or URL of the Teradata server.',
        'required': True,
        'label': 'Host'
    },
    user={
        'type': ARG_TYPE.STR,
        'description': 'The username for the Teradata database.',
        'required': True,
        'label': 'User'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password for the Teradata database.',
        'secret': True,
        'required': True,
        'label': 'Password'
    },
    database={
        'type': ARG_TYPE.STR,
        'description': "The name of the Teradata database to connect to. Defaults is the user's default database.",
        'required': False,
        'label': 'Database'
    }
)

connection_args_example = OrderedDict(
    host='192.168.0.41',
    user='dbc',
    password='dbc',
    database='HR'
)
