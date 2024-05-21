from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    hosts={
        'type': ARG_TYPE.STR,
        'description': 'The host name(s) or IP address(es) of the Elasticsearch server(s). If multiple host name(s) or '
                       'IP address(es) exist, they should be separated by commas. This parameter is '
                       'optional, but it should be provided if cloud_id is not.'
    },
    cloud_id={
        'type': ARG_TYPE.STR,
        'description': 'The unique ID to your hosted Elasticsearch cluster on Elasticsearch Service. This parameter is '
                       'optional, but it should be provided if hosts is not.'
    },
    username={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the Elasticsearch server. This parameter is optional.'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password to authenticate the user with the Elasticsearch server. This parameter is '
                       'optional.',
        'secret': True
    }
)

connection_args_example = OrderedDict(
    hosts='localhost:9200',
)
