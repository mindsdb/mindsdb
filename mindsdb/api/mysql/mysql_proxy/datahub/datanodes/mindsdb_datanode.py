import json

import pandas as pd
import numpy as np
from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.datanode import DataNode
from mindsdb.api.mysql.mysql_proxy.utilities.sql import query_df
from mindsdb.utilities.config import Config
from mindsdb.api.mysql.mysql_proxy.utilities.lightwood_dtype import dtype
from mindsdb.api.mysql.mysql_proxy.datahub.classes.tables_row import TablesRow


class NumpyJSONEncoder(json.JSONEncoder):
    """
    Use this encoder to avoid
    "TypeError: Object of type float32 is not JSON serializable"

    Example:
    x = np.float32(5)
    json.dumps(x, cls=NumpyJSONEncoder)
    """
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, (np.float, np.float32, np.float64)):
            return float(obj)
        else:
            return super().default(obj)


class MindsDBDataNode(DataNode):
    type = 'mindsdb'

    def __init__(self, model_controller, integration_controller, ml_handler='lightwood'):
        self.config = Config()
        self.model_controller = model_controller
        self.integration_controller = integration_controller
        self.handler = self.integration_controller.get_handler(ml_handler)

    def get_tables(self):
        models = self.model_controller.get_models()
        tables = []
        for model in models:
            tables.append(TablesRow(TABLE_NAME=model['name']))
        tables.append(TablesRow(TABLE_NAME='predictors'))
        tables.append(TablesRow(TABLE_NAME='predictors_versions'))
        tables.append(TablesRow(TABLE_NAME='databases'))

        return tables

    def has_table(self, table):
        names = [table.TABLE_NAME for table in self.get_tables()]
        return table in names

    def _get_model_columns(self, table_name):
        model = self.model_controller.get_model_data(name=table_name)
        dtype_dict = model.get('dtype_dict')
        if isinstance(dtype_dict, dict) is False:
            return []
        columns = []
        columns += list(dtype_dict.keys())
        predict = model['predict']
        if not isinstance(predict, list):
            predict = [predict]
        columns += [f'{x}_original' for x in predict]
        for col in predict:
            if dtype_dict.get(col) in (dtype.integer, dtype.float):
                columns += [f"{col}_min", f"{col}_max"]
            columns += [f"{col}_confidence"]
            columns += [f"{col}_explain"]
        return columns

    def get_table_columns(self, table):
        if table == 'predictors':
            return ['name', 'status', 'accuracy', 'predict', 'update_status',
                    'mindsdb_version', 'error', 'select_data_query',
                    'training_options']
        if table in ('datasources', 'databases'):
            return ['name', 'database_type', 'host', 'port', 'user']

        columns = []

        if table in [x['name'] for x in self.model_controller.get_models()]:
            columns = self._get_model_columns(table)
            columns += ['when_data', 'select_data_query']

        return columns

    def _select_predictors(self):
        models = self.model_controller.get_models()
        columns = ['name', 'status', 'accuracy', 'predict', 'update_status',
                   'mindsdb_version', 'error', 'select_data_query',
                   'training_options']
        return pd.DataFrame([[
            x['name'],
            x['status'],
            str(x['accuracy']) if x['accuracy'] is not None else None,
            ', '.join(x['predict']) if isinstance(x['predict'], list) else x['predict'],
            x['update'],
            x['mindsdb_version'],
            x['error'],
            x['fetch_data_query'],
            ''   # TODO
        ] for x in models], columns=columns)

    def _select_predictors_versions(self):
        models = self.model_controller.get_models(with_versions=True)
        models.sort(key=lambda x: x['created_at'])
        columns = ['name', 'version', 'active', 'status', 'accuracy', 'predict', 'update_status',
                   'mindsdb_version', 'error', 'select_data_query',
                   'training_options', 'created_at', 'training_time']
        data = []
        model_version_number = {}
        for model in models:
            if model['name'] not in model_version_number:
                model_version_number[model['name']] = 1
            data.append([
                model['name'],
                model_version_number[model['name']],
                model['active'],
                model['status'],
                str(model['accuracy']) if model['accuracy'] is not None else None,
                ', '.join(model['predict']) if isinstance(model['predict'], list) else model['predict'],
                model['update'],
                model['mindsdb_version'],
                model['error'],
                model['fetch_data_query'],
                '',
                str(model['created_at']),
                str(model['training_time'])
            ])
            model_version_number[model['name']] += 1
        return pd.DataFrame(data, columns=columns)

    def _select_integrations(self):
        integrations = self.integration_controller.get_all()
        result = []
        for ds_name, ds_meta in integrations.items():
            connection_data = ds_meta.get('connection_data', {})
            result.append([
                ds_name, ds_meta.get('engine'), connection_data.get('host'), connection_data.get('port'), connection_data.get('user')
            ])
        return pd.DataFrame(
            result,
            columns=['name', 'database_type', 'host', 'port', 'user']
        )

    def delete_predictor(self, name):
        self.model_controller.delete_model(name)

    def get_predictors(self, query: ASTNode):
        predictors_df = self._select_predictors()

        try:
            result_df = query_df(predictors_df, query)
        except Exception as e:
            print(f'Exception! {e}')
            return [], []

        return result_df.to_dict(orient='records'), list(result_df.columns)

    def get_predictors_versions(self, query: ASTNode):
        predictors_df = self._select_predictors_versions()

        try:
            result_df = query_df(predictors_df, query)
        except Exception as e:
            print(f'Exception! {e}')
            return [], []

        return result_df.to_dict(orient='records'), list(result_df.columns)

    def get_integrations(self, query: ASTNode):
        datasources_df = self._select_integrations()
        try:
            result_df = query_df(datasources_df, query)
        except Exception as e:
            print(f'Exception! {e}')
            return [], []
        return result_df.to_dict(orient='records'), list(result_df.columns)

    def query(self, table, where_data=None, ml_handler_name='lightwood'):
        if table == 'predictors':
            return self._select_predictors()
        if table == 'predictors_versions':
            return self._select_predictors_versions()
        if table == 'datasources':
            return self._select_datasources()

        if isinstance(where_data, dict):
            where_data = [where_data]

        if len(where_data) == 0:
            return []

        if ml_handler_name.lower() == 'mindsdb':
            ml_handler_name = 'lightwood'

        if ml_handler_name != 'lightwood':
            handler = self.integration_controller.get_handler(ml_handler_name)
        else:
            handler = self.handler

        result = handler.predict(table, where_data)
        return result
