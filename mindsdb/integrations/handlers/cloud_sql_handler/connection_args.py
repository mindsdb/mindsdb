from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the Google Cloud SQL instance.'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password to authenticate the user with the Google Cloud SQL instance.',
        'secret': True
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'The database name to use when connecting with the Google Cloud SQL instance.'
    },
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the Google Cloud SQL instance.'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'The TCP/IP port of the Google Cloud SQL instance. Must be an integer.'
    },
    db_engine={
        'type': ARG_TYPE.STR,
        'description': "The database engine of the Google Cloud SQL instance. This can take one of three values: 'mysql', 'postgresql' or 'mssql'."
    }
)

connection_args_example = OrderedDict(
    db_engine='mysql',
    host='53.170.61.16',
    port=3306,
    user='root',
    password='password',
    database='database'
)
