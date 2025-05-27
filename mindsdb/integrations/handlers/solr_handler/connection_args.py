from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    username={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the Solr server.'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password to authenticate the user with the Solr server.',
        'secret': True
    },
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the Solr server. NOTE: use \'127.0.0.1\' instead of \'localhost\' to connect to local server.'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'The TCP/IP port of the Solr server. Must be an integer.'
    },
    server_path={
        'type': ARG_TYPE.STR,
        'description': 'The server path connecting with the Solr server. Defaults to solr when not provided.'
    },
    collection={
        'type': ARG_TYPE.STR,
        'description': 'The collection name to use for the query in the Solr server.'
    },
    use_ssl={
        'type': ARG_TYPE.BOOL,
        'description': 'The flag to set ssl for the query in the Solr server.Defaults to false.'
    }
)

connection_args_example = OrderedDict(
    username="demo_user",
    password="demo_password",
    host="127.0.0.1",
    port=8981,
    server_path="solr",
    collection="gettingstarted",
    use_ssl=False,
)
