from mindsdb.api.executor.controllers import SessionController
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import CHARSET_NUMBERS
from mindsdb.api.mysql.mysql_proxy.mysql_proxy import MysqlProxy
from mindsdb.utilities.config import config


def empty_fn():
    pass


class Dummy:
    pass


class FakeMysqlProxy(MysqlProxy):
    def __init__(self):
        request = Dummy()
        client_address = ['', '']
        server = Dummy()
        server.connection_id = 0
        server.hook_before_handle = empty_fn

        self.charset = 'utf8'
        self.charset_text_type = CHARSET_NUMBERS['utf8_general_ci']
        self.client_capabilities = None

        self.request = request
        self.client_address = client_address
        self.server = server
        self.connection_id = None

        self.session = SessionController()
        self.session.database = config.get('default_project')

    def is_cloud_connection(self):
        return {
            'is_cloud': False
        }
