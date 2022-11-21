from mindsdb.api.mysql.mysql_proxy.controllers.session_controller import SessionController
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import CHARSET_NUMBERS
from mindsdb.interfaces.model.model_controller import ModelController
from mindsdb.interfaces.database.integrations import IntegrationController
from mindsdb.interfaces.database.projects import ProjectController
from mindsdb.interfaces.database.database import DatabaseController
from mindsdb.api.mysql.mysql_proxy.mysql_proxy import MysqlProxy
from mindsdb.utilities.context import context as ctx

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
        server.original_model_controller = ModelController()
        server.original_integration_controller = IntegrationController()
        server.original_project_controller = ProjectController()
        server.original_database_controller = DatabaseController()

        self.charset = 'utf8'
        self.charset_text_type = CHARSET_NUMBERS['utf8_general_ci']
        self.client_capabilities = None

        self.request = request
        self.client_address = client_address
        self.server = server

        self.session = SessionController(
            server=self.server
        )
        self.session.database = 'mindsdb'

    def is_cloud_connection(self):
        return {
            'is_cloud': False
        }
