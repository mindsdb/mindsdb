from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    driver={
        'type': ARG_TYPE.STR,
        'description': 'The driver to use for the Denodo connection.'
    },
    url={
        'type': ARG_TYPE.STR,
        'description': 'The URL for the Denodo connection.'
    },
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user for the Denodo connection.'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password for the Denodo connection.'
    }
)

connection_args_example = OrderedDict(
    driver='com.denodo.vdp.jdbc.Driver',
    url='jdbc:vdb://<hostname>:<port>/<database_name>',
    user='admin',
    password='password'
)
