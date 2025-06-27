from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the SQreamDB server/database.'
    },
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the SQreamDB server.'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password to authenticate the user with the SQreamDB server.',
        'secret': True
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'Specify port to connect SQreamDB server'
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'Specify database name  to connect SQreamDB server'
    },
    service={
        'type': ARG_TYPE.STR,
        'description': 'Optional: service queue (default: "sqream")'
    },
    use_ssl={
        'type': ARG_TYPE.BOOL,
        'description': 'use SSL connection (default: False)'
    },
    clustered={
        'type': ARG_TYPE.BOOL,
        'description': 'Optional:  Connect through load balancer, or direct to worker (Default: false - direct to worker)'
    },
)

connection_args_example = OrderedDict(
    host='127.0.0.1',
    port=5000,
    password='sqream',
    user='master',
    database='sqream'
)
