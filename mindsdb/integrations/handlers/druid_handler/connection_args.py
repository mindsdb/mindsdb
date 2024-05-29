from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of Apache Druid.',
        'required': True,
        'label': 'Host'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'The port that Apache Druid is running on.',
        'required': True,
        'label': 'Port'
    },
    path={
        'type': ARG_TYPE.STR,
        'description': 'The query path.',
        'required': True,
        'label': 'path'
    },
    scheme={
        'type': ARG_TYPE.STR,
        'description': 'The URI schema. This parameter is optional and the default will be http.',
        'required': False,
        'label': 'Scheme'
    },
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with Apache Druid. This parameter is optional.',
        'required': False,
        'label': 'User'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password used to authenticate with Apache Druid. This parameter is optional.',
        'required': False,
        'label': 'password',
        'secret': True
    }
)

connection_args_example = OrderedDict(
    host='localhost',
    port=8888,
    path='/druid/v2/sql/',
    scheme='http'
)
