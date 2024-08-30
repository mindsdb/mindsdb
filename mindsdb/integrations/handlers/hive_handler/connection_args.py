from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    username={
        'type': ARG_TYPE.STR,
        'description': 'The username for the Apache Hive database.',
        'required': False,
        'label': 'Username'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password for the Apache Hive database.',
        'secret': True,
        'required': False,
        'label': 'Password'
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'The name of the Apache Hive database to connect to.',
        'required': True,
        'label': 'Database'
    },
    host={
        'type': ARG_TYPE.STR,
        'description': 'The hostname, IP address, or URL of the Apache Hive server.. NOTE: use \'127.0.0.1\' instead of \'localhost\' to connect to local server.',
        'required': True,
        'label': 'Host'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'The port number for connecting to the Apache Hive server. Default is `10000`.',
        'required': False,
        'label': 'Port'
    },
    auth={
        'type': ARG_TYPE.STR,
        'description': 'The authentication mechanism to use. Default is `CUSTOM`. Other options are `NONE`, `NOSASL`, `KERBEROS` and `LDAP`.',
        'required': False,
        'label': 'Authentication'
    }
)

connection_args_example = OrderedDict(
    host='127.0.0.1',
    port='10000',
    auth='CUSTOM',
    user='root',
    password='password',
    database='database'
)
