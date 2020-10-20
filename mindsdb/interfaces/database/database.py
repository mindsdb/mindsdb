from mindsdb.integrations.clickhouse.clickhouse import Clickhouse
from mindsdb.integrations.postgres.postgres import PostgreSQL
from mindsdb.integrations.mariadb.mariadb import Mariadb
from mindsdb.integrations.mysql.mysql import MySQL
from mindsdb.integrations.mssql.mssql import MSSQL


class DatabaseWrapper():

    def __init__(self, config):
        self.config = config
        self._get_integrations()

    def _setup_integration(self, integration):
        success = False
        try:
            integration.setup()
            success = True
        except Exception as e:
            print('Failed to integrate with database ' + integration.name + f', error: {e}')
        return success

    def _get_integrations(self):
        # @TODO Once we have a presistent state sorted out this should be simplified as to not refresh the existing integrations every single time
        integration_arr = []
        for db_alias in self.config['integrations']:
            if self.config['integrations'][db_alias]['enabled']:
                db_type = self.config['integrations'][db_alias]['type']
                if db_type == 'clickhouse':
                    integration_arr.append(Clickhouse(self.config, db_alias))
                elif db_type == 'mariadb':
                    integration_arr.append(Mariadb(self.config, db_alias))
                elif db_type == 'mysql':
                    integration_arr.append(MySQL(self.config, db_alias))
                elif db_type == 'postgres':
                    integration_arr.append(PostgreSQL(self.config, db_alias))
                elif db_type == 'mssql':
                    integration_arr.append(MSSQL(self.config, db_alias))
                elif db_type == 'mongodb':
                    pass
                else:
                    print(f'Uknown integration type: {db_type} for database called: {db_alias}')

        return integration_arr

    def register_predictors(self, model_data_arr, setup=True):
        it = self._get_integrations()
        for integration in it:
            register = True
            if setup:
                register = self._setup_integration(integration)
            if register:
                integration.register_predictors(model_data_arr)

            integration = [integration]

    def unregister_predictor(self, name):
        for integration in self._get_integrations():
            integration.unregister_predictor(name)

    def check_connections(self):
        connections = {}
        for integration in self._get_integrations():
            connections[integration.name] = integration.check_connection()

        return connections
