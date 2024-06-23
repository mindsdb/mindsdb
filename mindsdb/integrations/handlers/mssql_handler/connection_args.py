from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the Microsoft SQL Server.',
        'required': True,
        'label': 'User'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password to authenticate the user with the Microsoft SQL Server.',
        'required': True,
        'label': 'Password',
        'secret': True
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'The database name to use when connecting with the Microsoft SQL Server.',
        'required': True,
        'label': 'Database'
    },
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the Microsoft SQL Server.',
        'required': True,
        'label': 'Host'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'The TCP/IP port of the Microsoft SQL Server. Must be an integer.',
        'required': False,
        'label': 'Port'
    },
    server={
        'type': ARG_TYPE.STR,
        'description': 'The server name of the Microsoft SQL Server. Typically only used with named instances or Azure SQL Database.',
        'required': False,
        'label': 'Server'
    }
)

connection_args_example = OrderedDict(
    host='127.0.0.1',
    port=1433,
    user='sa',
    password='password',
    database='master'
)
