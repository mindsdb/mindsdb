from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the Altibase server. NOTE: use \'127.0.0.1\' instead of \'localhost\' to connect to local server.'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'The TCP/IP port of the Altibase server. Must be an integer.'
    },
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the Altibase server.'
    },
    password={
        'type': ARG_TYPE.STR,
        'description': 'The password to authenticate the user with the Altibase server.',
        'secret': True
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'The database name to use when connecting with the Altibase server.'
    },
    jdbc_class={
        'type': ARG_TYPE.STR,
        'description': 'The driver class of the Altibase JDBC driver'
    },
    jar_location={
        'type': ARG_TYPE.PATH,
        'description': 'The location of the Altibase JDBC driver jar file'
    },
    dsn={
        'type': ARG_TYPE.STR,
        'description': 'Datasource name of the Altibase server. NOTE: use dsn if you want to use an ODBC connection.'
    }
)

connection_args_example = OrderedDict(
    host='127.0.0.1',
    port=20300,
    user='sys',
    password='manager',
    database='mydb',
    jdbc_class='Altibase.jdbc.driver.AltibaseDriver',
    jar_location='/data/altibase_home/lib/Altibase.jar'
)
