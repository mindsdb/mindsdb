import requests
from mindsdb.libs.constants.mindsdb import DATA_TYPES, DATA_SUBTYPES

class Clickhouse():
    def __init__(self, config):
        self.config = config
        self.host = config['integrations']['clickhouse']['host']
        self.port = config['integrations']['clickhouse']['port']
        self.user = config['integrations']['clickhouse']['user']
        self.password = config['integrations']['clickhouse']['password']
        self.setup_clickhouse()


    def _to_clickhouse_table(self, stats):
        subtype_map = {
            DATA_SUBTYPES.INT: 'Nullable(Int64)',
            DATA_SUBTYPES.FLOAT: 'Nullable(Float64)',
            DATA_SUBTYPES.BINARY: 'Nullable(UInt8)',
            DATA_SUBTYPES.DATE: 'Nullable(Date)',
            DATA_SUBTYPES.TIMESTAMP: 'Nullable(Datetime)',
            DATA_SUBTYPES.SINGLE: 'Nullable(String)',
            DATA_SUBTYPES.MULTIPLE: 'Nullable(String)',
            DATA_SUBTYPES.IMAGE: 'Nullable(String)',
            DATA_SUBTYPES.VIDEO: 'Nullable(String)',
            DATA_SUBTYPES.AUDIO: 'Nullable(String)',
            DATA_SUBTYPES.TEXT: 'Nullable(String)',
            DATA_SUBTYPES.ARRAY: 'Array(Float64)'
        }

        column_declaration = []
        for name, column in stats.items():
            try:
                col_subtype = stats[name]['typing']['data_subtype']
                new_type = subtype_map[col_subtype]
                column_declaration.append(f' `{name}` {new_type} ')
            except Exception as e:
                print(e)
                print(f'Error: cant convert type {col_subtype} of column {name} to clickhouse tpye')

        return column_declaration

    def _query(self, query):
        params = {'user': 'default'}
        try:
            params['user'] = self.config['integrations']['clickhouse']['user']
        except:
            pass

        try:
            params['password'] = self.config['integrations']['clickhouse']['password']
        except:
            pass

        host = self.config['integrations']['clickhouse']['host']
        port = self.config['integrations']['clickhouse']['port']

        response = requests.post(f'http://{host}:{port}', data=query, params=params)

        return response

    def setup_clickhouse(self):
        self._query('CREATE DATABASE IF NOT EXISTS mindsdb')

        msqyl_conn = self.config['api']['mysql']['host'] + ':' + str(self.config['api']['mysql']['port'])
        msqyl_user = self.config['api']['mysql']['user']
        msqyl_pass = self.config['api']['mysql']['password']

        q = f"""
                CREATE TABLE IF NOT EXISTS mindsdb.predictors
                (name String,
                status String,
                accuracy String,
                predict_cols String,
                select_data_query String,
                training_options String
                ) ENGINE=MySQL('{msqyl_conn}', 'mindsdb', 'predictors_clickhouse', '{msqyl_user}', '{msqyl_pass}')
        """
        print(f'Executing table creation query to create predictors list:\n{q}\n')
        self._query(q)

        q = f"""
            CREATE TABLE IF NOT EXISTS mindsdb.commands (
                command String
            ) ENGINE=MySQL('{msqyl_conn}', 'mindsdb', 'commands_clickhouse', '{msqyl_user}', '{msqyl_pass}')
        """
        print(f'Executing table creation query to create command table:\n{q}\n')
        self._query(q)

    def register_predictor(self, name, stats):
        columns_sql = ','.join(self._to_clickhouse_table(stats))
        columns_sql += ',`$select_data_query` Nullable(String)'

        msqyl_conn = self.config['api']['mysql']['host'] + ':' + str(self.config['api']['mysql']['port'])
        msqyl_user = self.config['api']['mysql']['user']
        msqyl_pass = self.config['api']['mysql']['password']

        q = f"""
                CREATE TABLE mindsdb.{name}
                ({columns_sql}
                ) ENGINE=MySQL('{msqyl_conn}', 'mindsdb', '{name}_clickhouse', '{msqyl_user}', '{msqyl_pass}')
        """
        print(f'Executing table creation query to sync predictor:\n{q}\n')
        self._query(q)

    def unregister_predictor(self, name):
        q = f"""
            drop table if exists mindsdb.{name};
        """
        print(f'Executing table creation query to sync predictor:\n{q}\n')
        self._query(q)
