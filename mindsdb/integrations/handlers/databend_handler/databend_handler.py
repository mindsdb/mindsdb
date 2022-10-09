from ..clickhouse_handler.clickhouse_handler import ClickHouseHandler

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from collections import OrderedDict
from typing import Optional


class DatabendHandler(ClickHouseHandler):
    """
    This handler handles connection and execution of the Databend statements.
    """
    name = 'databend'

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        super().__init__(name, connection_data, **kwargs)


connection_args = OrderedDict(
    protocol={
        'type': ARG_TYPE.STR,
        'protocol': 'The protocol to query Databend. Supported: native, http, https. Default: native'
    },
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the Databend warehouse.'
    },
    password={
        'type': ARG_TYPE.STR,
        'description': 'The password to authenticate the user with the Databend warehouse.'
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'The database name to use when connecting with the Databend warehouse.'
    },
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the Databend warehouse. NOTE: use \'127.0.0.1\' instead of \'localhost\' to connect to local server.'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'The TCP/IP port of the ClickHouse server.'
    }
)

connection_args_example = OrderedDict(
    protocol='https',
    host='some-url.aws-us-east-2.default.databend.com',
    port=443,
    user='root',
    password='password',
    database='test_db'
)