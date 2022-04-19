from mindsdb.integrations.redis.redisdb import Redis
from mindsdb.integrations.kafka.kafkadb import Kafka
from mindsdb.utilities.log import log as logger
from mindsdb.utilities.config import Config
from mindsdb.interfaces.database.integrations import IntegrationController
from mindsdb.utilities.with_kwargs_wrapper import WithKWArgsWrapper


class StreamController():
    known_dbs = {
        'redis': Redis,
        'kafka': Kafka
    }

    def __init__(self, company_id):
        self.config = Config()
        self.company_id = company_id
        self.integration_controller = WithKWArgsWrapper(
            IntegrationController(), company_id=company_id
        )

    def setup(self, db_alias):
        try:
            integration = self.integration_controller.get(db_alias)
            if integration is None:
                raise Exception(f'Unkonw database integration: {db_alias}')
            if integration.get('type') not in self.known_dbs:
                raise Exception(f'Unkonw database integration type for: {db_alias}')
            integration.setup()
        except Exception as e:
            logger.warning('Failed to setup stream for ' + db_alias + f', error: {e}')
