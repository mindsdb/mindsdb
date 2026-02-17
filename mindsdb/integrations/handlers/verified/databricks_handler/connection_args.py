from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    server_hostname={
        "type": ARG_TYPE.STR,
        "description": "The server hostname for the cluster or SQL warehouse.",
        "required": True,
        "label": "Server Hostname",
    },
    http_path={
        "type": ARG_TYPE.STR,
        "description": "The HTTP path of the cluster or SQL warehouse.",
        "required": True,
        "label": "HTTP Path",
    },
    access_token={
        "type": ARG_TYPE.STR,
        "description": "A Databricks personal access token to authenticate the connection.",
        "required": True,
        "label": "Access Token",
        'secret': True
    },
    session_configuration={
        "type": ARG_TYPE.STR,
        "description": "Additional (key, value) pairs to set as Spark session configuration parameters.",
        "required": False,
        "label": "Session Configuration",
    },
    http_headers={
        "type": ARG_TYPE.STR,
        "description": "Additional (key, value) pairs to set in HTTP headers on every RPC request the connection makes."
        " This parameter is optional.",
        "required": False,
        "label": "HTTP Headers",
    },
    catalog={
        "type": ARG_TYPE.STR,
        "description": "Catalog to use for the connection.",
        "required": False,
        "label": "Catalog",
    },
    schema={
        "type": ARG_TYPE.STR,
        "description": "Schema (database) to use for the connection.",
        "required": False,
        "label": "Schema",
    },
)

connection_args_example = OrderedDict(
    server_hostname="adb-1234567890123456.7.azuredatabricks.net",
    http_path="sql/protocolv1/o/1234567890123456/1234-567890-test123",
    access_token="dapi1234567890ab1cde2f3ab456c7d89efa",
    schema="sales",
)
