from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the Apache Pinot cluster.'
    },
    broker_port={
        'type': ARG_TYPE.INT,
        'description': 'The port that the Broker of the Apache Pinot cluster is running on.'
    },
    controller_port={
        'type': ARG_TYPE.INT,
        'description': 'The port that the Controller of the Apache Pinot cluster is running on.'
    },
    path={
        'type': ARG_TYPE.STR,
        'description': 'The query path.'
    },
    scheme={
        'type': ARG_TYPE.STR,
        'description': 'The URI schema. This parameter is optional and the default will be https.'
    },
    username={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the Apache Pinot cluster. This parameter is optional.'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password used to authenticate with the Apache Pinot cluster. This parameter is optional.',
        'secret': True
    },
    verify_ssl={
        'type': ARG_TYPE.STR,
        'description': 'The flag for whether SSL certificates should be verified or not. This parameter is optional and '
                       'if specified, it should be either True or False'
    },
)

connection_args_example = OrderedDict(
    host='localhost',
    broker_port=8000,
    controller_port=9000,
    path='/query/sql',
    scheme='http'
)
