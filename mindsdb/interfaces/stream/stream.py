from mindsdb.interfaces.stream.redis.redisdb import Redis
from mindsdb.interfaces.stream.kafka.kafkadb import Kafka
from mindsdb.utilities import log
from mindsdb.utilities.config import Config
from mindsdb.interfaces.database.integrations import IntegrationController
from mindsdb.utilities.context import context as ctx


class StreamController():
    known_dbs = {
        'redis': Redis,
        'kafka': Kafka
    }

    def __init__(self):
        self.config = Config()
        self.company_id = ctx.company_id
        self.integration_controller = IntegrationController()

    def setup(self, db_alias):
        try:
            integration_meta = self.integration_controller.get(db_alias)
            if integration_meta is None:
                raise Exception(f'Unkonw database integration: {db_alias}')
            if integration_meta.get('engine') not in self.known_dbs:
                raise Exception(f'Unkonw database integration type for: {db_alias}')
            self.known_dbs[integration_meta['engine']](self.config, db_alias, integration_meta).setup()
        except Exception as e:
            log.logger.warning('Failed to setup stream for ' + db_alias + f', error: {e}')
