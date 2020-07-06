from mindsdb.integrations.clickhouse.clickhouse import Clickhouse
from mindsdb.integrations.mariadb.mariadb import Mariadb


class DatabaseWrapper():

    def __init__(self, config, setup=False):
        self.config = config
        self.integration_arr = []

        for db_alias in config['integrations']:
            if config['integrations'][db_alias]['enabled']:
                if config['integrations'][db_alias]['type'] == 'clickhouse':
                    self.integration_arr.append(Clickhouse(config,db_alias))
                if config['integrations'][db_alias]['type'] == 'mariadb':
                    self.integration_arr.append(Mariadb(config,db_alias))
        # Doesn't really matter if we call this multiple times, but it will waste time so ideally don't
        if setup:
            working_integrations = []
            try:
                for integration in self.integration_arr:
                    integration.setup()
                    working_integrations.append(integration)
            except Exception as e:
                print(f'Failed to integrate with a database, error: {e}')

            self.integration_arr = working_integrations

    def register_predictors(self, model_data_arr):
        for integration in self.integration_arr: integration.register_predictors(model_data_arr)

    def unregister_predictor(self, name):
        for integration in self.integration_arr: integration.unregister_predictor(name)

    def check_connections(self):
        connections = {}
        for integration in self.integration_arr:
            connections[integration.name] = integration.check_connection()

        return connections
