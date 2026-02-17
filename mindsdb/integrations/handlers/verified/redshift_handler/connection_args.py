from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the Redshift cluster.',
        'required': True,
        'label': 'Host'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'The port to use when connecting with the Redshift cluster.',
        'required': True,
        'label': 'Port'
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'The database name to use when connecting with the Redshift cluster.',
        'required': True,
        'label': 'Database'
    },
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the Redshift cluster.',
        'required': True,
        'label': 'User'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password to authenticate the user with the Redshift cluster.',
        'required': True,
        'label': 'Password',
        'secret': True
    },
    schema={
        'type': ARG_TYPE.STR,
        'description': 'The schema in which objects are searched first.',
        'required': False,
        'label': 'Schema'
    },
    sslmode={
        'type': ARG_TYPE.STR,
        'description': 'The SSL mode that will be used for connection.',
        'required': False,
        'label': 'sslmode'
    }
)

connection_args_example = OrderedDict(
    host='examplecluster.abc123xyz789.us-west-1.redshift.amazonaws.com',
    port='5439',
    database='dev',
    user='awsuser',
    password='my_password'
)
