from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    hosts={
        "type": ARG_TYPE.STR,
        "description": "The host name(s) or IP address(es) of the Elasticsearch server(s). If multiple host name(s) or "
        "IP address(es) exist, they should be separated by commas, e.g., `host1:port1, host2:port2`. "
        "If this parameter is not provided, `cloud_id` should be.",
    },
    cloud_id={
        "type": ARG_TYPE.STR,
        "description": "The unique ID to your hosted Elasticsearch cluster on Elasticsearch Service. If this parameter is "
        "not provided, `hosts` should be.",
    },
    user={
        "type": ARG_TYPE.STR,
        "description": "The username to connect to the Elasticsearch server with.",
    },
    password={
        "type": ARG_TYPE.PWD,
        "description": "The password to authenticate the user with the Elasticsearch server.",
        "secret": True,
    },
    api_key={
        "type": ARG_TYPE.STR,
        "description": "The API key for authentication with the Elasticsearch server.",
        "secret": True,
    },
)

connection_args_example = OrderedDict(
    hosts="localhost:9200",
    user="admin",
    password="password",
)
