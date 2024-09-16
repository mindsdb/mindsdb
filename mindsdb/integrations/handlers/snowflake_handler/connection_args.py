from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    host={
        'type': ARG_TYPE.STR,
        'description': 'The Snowflake host.',
        'required': True,
        'label': 'Host'
    },
    account={
        'type': ARG_TYPE.STR,
        'description': 'The Snowflake account name.',
        'required': False,
        'label': 'Server'
    },
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the Snowflake account.',
        'required': True,
        'label': 'User'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password to authenticate the user with the Snowflake account.',
        'required': True,
        'label': 'Password',
        'secret': True
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'The database to use when connecting to the Snowflake account.',
        'required': True,
        'label': 'Database'
    },
    schema={
        'type': ARG_TYPE.STR,
        'description': 'The schema to use when connecting to the Snowflake account.',
        'required': False,
        'label': 'Schema'
    },
    warehouse={
        'type': ARG_TYPE.STR,
        'description': 'The warehouse to use when executing queries on the Snowflake account.',
        'required': False,
        'label': 'Warehouse'
    },
    role={
        'type': ARG_TYPE.STR,
        'description': 'The role to use when executing queries on the Snowflake account.',
        'required': False,
        'label': 'Role'
    }
)

connection_args_example = OrderedDict(
    account='abcxyz-1234567',
    user='user',
    password='password',
    database='test'
)
