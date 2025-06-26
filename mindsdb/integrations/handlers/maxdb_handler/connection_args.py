from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the SAP MaxDB server.'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'The TCP/IP port of the SAP MaxDB. Must be an integer.'
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'The database name to use when connecting with the SAP MaxDB server.'
    },
    jdbc_location={
        'type': ARG_TYPE.STR,
        'description': 'The location of the jar file which contains the JDBC class.'
    },
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the SAP MaxDB server.'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password to authenticate the user with the SAP MaxDB server.',
        'secret': True
    }
)


connection_args_example = OrderedDict(
    host='127.0.0.1',
    port=7210,
    user='DBADMIN',
    password='password',
    database="MAXDB",
    jdbc_location='/Users/marsid/Desktop/sapdbc.jar',
)
