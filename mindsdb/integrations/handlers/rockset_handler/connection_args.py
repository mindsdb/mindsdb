from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    user={
        'type': ARG_TYPE.STR,
        'description': 'Rockset user name'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'Rockset password',
        'secret': True
    },
    api_key={
        'type': ARG_TYPE.STR,
        'description': 'Rockset API key'
    },
    api_server={
        'type': ARG_TYPE.STR,
        'description': 'Rockset API server'
    },
    host={
        'type': ARG_TYPE.STR,
        'description': 'Rockset host'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'Rockset port'
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'Rockset database'
    }
)
connection_args_example = OrderedDict(
    user='rockset',
    password='rockset',
    api_key="adkjf234rksjfa23waejf2",
    api_server='api-us-west-2.rockset.io',
    host='localhost',
    port='3306',
    database='test'
)
