from ..clickhouse_handler import Handler as ClickhouseHandler

from collections import OrderedDict
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


class DatabendHandler(ClickhouseHandler):
    """
    This handler handles connection and execution of the Databend statements.
    """
    name = 'databend'

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)


connection_args_example = OrderedDict(
    protocol='https',
    host='some-url.aws-us-east-2.default.databend.com',
    port=443,
    user='root',
    password='password',
    database='test_db'
)