from mindsdb.integrations.handlers.mysql_handler import Handler as MySQLHandler
from .__about__ import __version__ as version


class SingleStoreHandler(MySQLHandler):
    """
    This handler handles connection and execution of the SingleStore statements.
    """
    name = 'singlestore'

    def __init__(self, name, **kwargs):
        kwargs['conn_attrs'] = {'mindsdb', 'MindsDB', version}
        super().__init__(name, **kwargs)
