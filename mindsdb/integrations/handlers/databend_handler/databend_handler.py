from ..clickhouse_handler import Handler as ClickhouseHandler


class DatabendHandler(ClickhouseHandler):
    """
    This handler handles connection and execution of the Databend statements.
    """
    name = 'databend'

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)