from mindsdb.integrations.clickhouse.clickhouse import Clickhouse
from mindsdb.integrations.mariadb.mariadb import Mariadb


class DatabaseWrapper():

    def __init__(self, config):
        self.config = config
        self._get_integrations()

    def _setup_integrations(self, integration_arr):
        # Doesn't really matter if we call this multiple times, but it will waste time so ideally don't
        working_integration_arr = []
        for integration in integration_arr:
            try:
                integration.setup()
                working_integration_arr.append(integration)
            except Exception as e:
                print('Failed to integrate with database' + integration.name + f', error: {e}')

        return working_integration_arr

    def _get_integrations(self):
        # @TODO Once we have a presistent state sorted out this should be simplified as to not refresh the existing integrations every single time
        integration_arr = []
        for db_alias in self.config['integrations']:
            if self.config['integrations'][db_alias]['enabled']:
                if self.config['integrations'][db_alias]['type'] == 'clickhouse':
                    integration_arr.append(Clickhouse(self.config,db_alias))
                elif self.config['integrations'][db_alias]['type'] == 'mariadb':
                    integration_arr.append(Mariadb(self.config,db_alias))
                else:
                    print('Uknown integration type: ' + self.config['integrations'][db_alias]['type'] + f' for database called: {db_alias}')

        return integration_arr

    def register_predictors(self, model_data_arr):
        it = self._get_integrations()
        it = self._setup_integrations(it)
        for integration in it: integration.register_predictors(model_data_arr)

    def unregister_predictor(self, name):
        for integration in self._get_integrations(): integration.unregister_predictor(name)

    def check_connections(self):
        connections = {}
        for integration in self._get_integrations():
            connections[integration.name] = integration.check_connection()

        return connections
