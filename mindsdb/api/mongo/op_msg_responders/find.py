from bson.int64 import Int64

from mindsdb.api.mongo.classes import Responder
import mindsdb.api.mongo.functions as helpers


class Responce(Responder):
    when = {'find': helpers.is_true}

    def result(self, query, request_env, mindsdb_env):
        models = mindsdb_env['mindsdb_native'].get_models()
        model_names = [x['name'] for x in models]
        table = query['find']
        where_data = query.get('filter', {})
        if table == 'predictors':
            data = [{
                'name': x['name'],
                'status': x['status'],
                'accuracy': str(x['accuracy']) if x['accuracy'] is not None else None,
                'predict': ', '.join(x['predict']),
                'select_data_query': x['data_source'],
                'external_datasource': '',
                'training_options': ''
            } for x in models]
        elif table in model_names:
            # prediction
            model = mindsdb_env['mindsdb_native'].get_model_data(name=query['find'])

            # TODO remove duplication
            columns = []
            columns += [x['column_name'] for x in model['data_analysis']['input_columns_metadata']]
            columns += [x['column_name'] for x in model['data_analysis']['target_columns_metadata']]
            columns += [f'{x}_original' for x in model['predict']]
            for col in model['predict']:
                if model['data_analysis_v2'][col]['typing']['data_type'] == 'Numeric':
                    columns += [f"{col}_min", f"{col}_max"]
                columns += [f"{col}_confidence"]
                columns += [f"{col}_explain"]

            columns += ['when_data', 'select_data_query', 'external_datasource']

            for key in where_data:
                if key not in columns:
                    raise Exception(f"Unknown column '{key}'. Only columns from this list can be used in query: {', '.join(columns)}")

            # TODO make query
            prediction = mindsdb_env['mindsdb_native'].predict(name=table, when_data=where_data)

            predicted_columns = model['predict']
            ##### doublication
            data = []
            keys = [x for x in list(prediction._data.keys()) if x in columns]
            min_max_keys = []
            for col in predicted_columns:
                if model['data_analysis_v2'][col]['typing']['data_type'] == 'Numeric':
                    min_max_keys.append(col)

            length = len(prediction._data[predicted_columns[0]])
            for i in range(length):
                row = {}
                explanation = prediction[i].explain()
                for key in keys:
                    row[key] = prediction._data[key][i]
                    # +++ FIXME this fix until issue https://github.com/mindsdb/mindsdb/issues/591 not resolved
                    # typing = None
                    # if key in model['data_analysis_v2']:
                    #     typing = model['data_analysis_v2'][key]['typing']['data_subtype']

                    # if typing == 'Timestamp' and row[key] is not None:
                    #     timestamp = datetime.datetime.utcfromtimestamp(row[key])
                    #     row[key] = timestamp.strftime('%Y-%m-%d %H:%M:%S')
                    # elif typing == 'Date':
                    #     timestamp = datetime.datetime.utcfromtimestamp(row[key])
                    #     row[key] = timestamp.strftime('%Y-%m-%d')
                    # ---
                for key in predicted_columns:
                    row[key + '_confidence'] = explanation[key]['confidence']
                    # row[key + '_explain'] = json.dumps(explanation[key])
                    row[key + '_explain'] = explanation[key]
                for key in min_max_keys:
                    row[key + '_min'] = min(explanation[key]['confidence_interval'])
                    row[key + '_max'] = max(explanation[key]['confidence_interval'])
                # row['select_data_query'] = select_data_query
                # row['external_datasource'] = external_datasource
                # row['when_data'] = original_when_data
                # for k in original_target_values:
                #     row[k] = original_target_values[k][i]
                data.append(row)
            #####

            if 'projection' in query:
                # TODO remove columns
                pass

        else:
            # probably wrong table name. Mongo in this case returns empty data
            data = []

        cursor = {
            'id': Int64(0),
            'ns': f"mindsdb.$cmd.{query['find']}",
            'firstBatch': data
        }
        return {
            'cursor': cursor,
            'ok': 1
        }


responder = Responce()
