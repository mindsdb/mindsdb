from mindsdb.api.mysql.mysql_proxy.controllers.session_controller import SessionController
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import CHARSET_NUMBERS
from mindsdb.interfaces.model.model_interface import ModelInterface
from mindsdb.interfaces.database.integrations import IntegrationController
from mindsdb.interfaces.database.views import ViewController
from mindsdb.api.mysql.mysql_proxy.mysql_proxy import MysqlProxy


def empty_fn():
    pass


class Dummy:
    pass


class FakeMysqlProxy(MysqlProxy):
    def __init__(self, company_id, user_class):
        request = Dummy()
        client_address = ['', '']
        server = Dummy()
        server.connection_id = 0
        server.hook_before_handle = empty_fn
        server.original_model_interface = ModelInterface()
        server.original_integration_controller = IntegrationController()
        server.original_view_controller = ViewController()

        self.charset = 'utf8'
        self.charset_text_type = CHARSET_NUMBERS['utf8_general_ci']
        self.client_capabilities = None

        self.request = request
        self.client_address = client_address
        self.server = server

        self.session = SessionController(
            server=self.server,
            company_id=company_id,
            user_class=user_class
        )
        self.session.database = 'mindsdb'

    def is_cloud_connection(self):
        return {
            'is_cloud': False
        }
