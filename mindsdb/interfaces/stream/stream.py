from mindsdb.utilities import log
from mindsdb.utilities.config import Config
from mindsdb.interfaces.database.integrations import integration_controller
from mindsdb.utilities.context import context as ctx

logger = log.getLogger(__name__)

class StreamController():
    known_dbs = {}

    def __init__(self):
        self.config = Config()
        self.company_id = ctx.company_id
        self.integration_controller = integration_controller

    def setup(self, db_alias):
        try:
            integration_meta = self.integration_controller.get(db_alias)
            if integration_meta is None:
                raise Exception(f'Unkonw database integration: {db_alias}')
            if integration_meta.get('engine') not in self.known_dbs:
                raise Exception(f'Unkonw database integration type for: {db_alias}')
            self.known_dbs[integration_meta['engine']](self.config, db_alias, integration_meta).setup()
        except Exception as e:
            logger.warning('Failed to setup stream for ' + db_alias + f', error: {e}')
