from mindsdb.integrations.clickhouse.clickhouse import Clickhouse
from mindsdb.integrations.postgres.postgres import PostgreSQL
from mindsdb.integrations.mariadb.mariadb import Mariadb
from mindsdb.integrations.mysql.mysql import MySQL
from mindsdb.integrations.mssql.mssql import MSSQL
from mindsdb.integrations.mongodb.mongodb import MongoDB
from mindsdb.integrations.redis.redisdb import Redis

from mindsdb.utilities.log import log as logger
from mindsdb.utilities.config import Config


class DatabaseWrapper():
    known_dbs = {'clickhouse': Clickhouse,
                 'mariadb': Mariadb,
                 'mysql': MySQL,
                 'postgres': PostgreSQL,
                 'mssql': MSSQL,
                 'mongodb': MongoDB,
                 'redis': Redis}
    def __init__(self):
        self.config = Config()

    def setup_integration(self, db_alias):
        try:
            # If this is the name of an integration
            integration = self._get_integration(db_alias)
            if integration == False:
                raise Exception(f'Unkonw database integration type for: {db_alias}')
            if integration != True:
                integration.setup()
        except Exception as e:
            logger.warning('Failed to integrate with database ' + db_alias + f', error: {e}')

    def _get_integration(self, db_alias):
        if self.config['integrations'][db_alias]['publish']:
            db_type = self.config['integrations'][db_alias]['type']
            if db_type in self.known_dbs:
                return self.known_dbs[db_type](self.config, db_alias)
            logger.warning(f'Uknown integration type: {db_type} for database called: {db_alias}')
            return False
        return True

    def _get_integrations(self):
        integrations = [self._get_integration(x) for x in self.config['integrations']]
        integrations = [x for x in integrations if x != True and x != False]
        return integrations

    def register_predictors(self, model_data_arr, integration_name=None):
        if integration_name is None:
            integrations = self._get_integrations()
        else:
            integration = self._get_integration(integration_name)
            integrations = [] if isinstance(integration, bool) else [integration]

        for integration in integrations:
            if integration.check_connection():
                try:
                    integration.register_predictors(model_data_arr)
                except Exception as e:
                    logger.warning(f"Error {e} when trying to register predictor to {integration.name}. Predictor wouldn't be registred.")
            else:
                logger.warning(f"There is no connection to {integration.name}. Predictor wouldn't be registred.")

    def unregister_predictor(self, name):
        for integration in self._get_integrations():
            if integration.check_connection():
                integration.unregister_predictor(name)
            else:
                logger.warning(f"There is no connection to {integration.name}. predictor wouldn't be unregistred")

    def check_connections(self):
        connections = {}
        for integration in self._get_integrations():
            connections[integration.name] = integration.check_connection()

        return connections
