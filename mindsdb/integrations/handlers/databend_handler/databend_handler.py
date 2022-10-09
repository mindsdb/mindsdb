from ..clickhouse_handler.clickhouse_handler import ClickHouseHandler

from collections import OrderedDict
from typing import Optional


class DatabendHandler(ClickHouseHandler):
    """
    This handler handles connection and execution of the Databend statements.
    """
    name = 'databend'

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        super().__init__(name, connection_data, **kwargs)


connection_args_example = OrderedDict(
    protocol='https',
    host='some-url.aws-us-east-2.default.databend.com',
    port=443,
    user='root',
    password='password',
    database='test_db'
)