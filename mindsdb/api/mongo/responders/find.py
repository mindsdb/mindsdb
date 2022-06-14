from bson.int64 import Int64
from collections import OrderedDict

from lightwood.api import dtype

import mindsdb.api.mongo.functions as helpers
from mindsdb.api.mongo.classes import Responder
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE


class Responce(Responder):
    when = {'find': helpers.is_true}

    def result(self, query, request_env, mindsdb_env, session):
        models = mindsdb_env['model_interface'].get_models()
        model_names = [x['name'] for x in models]
        table = query['find']
        where_data = query.get('filter', {})
        if table == 'predictors':
            data = [{
                'name': x['name'],
                'status': x['status'],
                'accuracy': str(x['accuracy']) if x['accuracy'] is not None else None,
                'predict': ', '.join(x['predict'] if isinstance(x['predict'], list) else [x['predict']]),
                'error': x['error'],
                'select_data_query': '',
                'training_options': ''
            } for x in models]
        elif table in model_names:
            # prediction
            model = mindsdb_env['model_interface'].get_model_data(name=query['find'])

            columns = []
            columns += list(model['dtype_dict'].keys())
            predict = model['predict']
            if not isinstance(predict, list):
                predict = [predict]
            columns += [f'{x}_original' for x in predict]
            for col in predict:
                if model['dtype_dict'][col] in (dtype.integer, dtype.float):
                    columns += [f"{col}_min", f"{col}_max"]
                columns += [f"{col}_confidence"]
                columns += [f"{col}_explain"]

            columns += ['when_data', 'select_data_query']

            where_data_list = where_data if isinstance(where_data, list) else [where_data]
            for statement in where_data_list:
                if isinstance(statement, dict):
                    for key in statement:
                        if key not in columns:
                            columns.append(key)

            datasource = where_data
            if 'select_data_query' in where_data:
                integrations = mindsdb_env['integration_controller'].get_all().keys()
                connection = where_data.get('connection')
                if connection is None:
                    if 'default_mongodb' in integrations:
                        connection = 'default_mongodb'
                    else:
                        for integration in integrations:
                            if integration.startswith('mongodb_'):
                                connection = integration
                                break

                if connection is None:
                    raise Exception("Can't find connection from which fetch data")

                handler = mindsdb_env['integration_controller'].get_handler(connection)
                result = handler.native_query(where_data['select_data_query'])

                if result.get('type') != RESPONSE_TYPE.TABLE:
                    raise Exception(f'Error during query: {result.get("error_message")}')

                datasource = result['data_frame']
            if isinstance(datasource, OrderedDict):
                datasource = dict(datasource)

            pred_dict_arr, explanations = mindsdb_env['model_interface'].predict(table, datasource, 'dict&explain')

            predicted_columns = model['predict']
            if not isinstance(predicted_columns, list):
                predicted_columns = [predicted_columns]

            data = []
            all_columns = list(model['dtype_dict'].keys())   # [k for k in pred_dict_arr[0] if k in columns]
            min_max_keys = []
            for col in predicted_columns:
                if model['dtype_dict'][col] in (dtype.integer, dtype.float):
                    min_max_keys.append(col)

            for i in range(len(pred_dict_arr)):
                row = {}
                explanation = explanations[i]

                for value in pred_dict_arr[i].values():
                    row.update(value)
                if 'predicted_value' in row:
                    del row['predicted_value']
                for key in pred_dict_arr[i]:
                    row[key] = pred_dict_arr[i][key]['predicted_value']
                for key in all_columns:
                    if key not in row:
                        row[key] = None

                for key in predicted_columns:
                    row[key + '_confidence'] = explanation[key]['confidence']
                    row[key + '_explain'] = explanation[key]
                for key in min_max_keys:
                    if 'confidence_lower_bound' in explanation[key]:
                        row[key + '_min'] = explanation[key]['confidence_lower_bound']
                    if 'confidence_upper_bound' in explanation[key]:
                        row[key + '_max'] = explanation[key]['confidence_upper_bound']
                data.append(row)

        else:
            # probably wrong table name. Mongo in this case returns empty data
            data = []

        if 'projection' in query and len(data) > 0:
            true_filter = []
            false_filter = []
            for key, value in query['projection'].items():
                if helpers.is_true(value):
                    true_filter.append(key)
                else:
                    false_filter.append(key)

            keys = list(data[0].keys())
            del_id = '_id' in false_filter
            if len(true_filter) > 0:
                for row in data:
                    for key in keys:
                        if key != '_id':
                            if key not in true_filter:
                                del row[key]
                        elif del_id:
                            del row[key]
            else:
                for row in data:
                    for key in false_filter:
                        if key in row:
                            del row[key]

        db = mindsdb_env['config']['api']['mongodb']['database']

        cursor = {
            'id': Int64(0),
            'ns': f"{db}.$cmd.{query['find']}",
            'firstBatch': data
        }
        return {
            'cursor': cursor,
            'ok': 1
        }


responder = Responce()
