from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


# For complete list of parameters: https://infocenter.sybase.com/help/index.jsp?topic=/com.sybase.help.sqlanywhere.12.0.1/dbadmin/da-conparm.html
connection_args = OrderedDict(
    host={
        'type': ARG_TYPE.STR,
        'description': 'The IP address/host name of the SAP SQL Anywhere instance host.'
    },
    port={
        'type': ARG_TYPE.STR,
        'description': 'The port number of the SAP SQL Anywhere instance.'
    },
    user={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the user name.'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'Specifies the password for the user.',
        'secret': True
    },
    server={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the name of the server to connect to.'
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the name of the database to connect to.'
    },
    encrypt={
        'type': ARG_TYPE.BOOL,
        'description': 'Enables or disables TLS encryption.'
    },
)

connection_args_example = OrderedDict(
    host='localhost',
    port=55505,
    user='DBADMIN',
    password='password',
    serverName='TestMe',
    database='MINDSDB'
)
