from mindsdb.api.mongo.classes import Responder
import mindsdb.api.mongo.functions as helpers


class Responce(Responder):
    when = {'insert': helpers.is_true}

    def result(self, query, request_env, mindsdb_env, session):
        try:
            res = self._result(query, request_env, mindsdb_env)
        except Exception as e:
            res = {
                'n': 0,
                'writeErrors': [{
                    'index': 0,
                    'code': 0,
                    'errmsg': str(e)
                }],
                'ok': 1
            }
        return res

    def _result(self, query, request_env, mindsdb_env):
        table = query['insert']
        if table != 'predictors':
            raise Exception("Only insert to 'predictors' table allowed")

        predictors_columns = [
            'name',
            'status',
            'accuracy',
            'predict',
            'select_data_query',
            'external_datasource',
            'training_options'
        ]

        models = mindsdb_env['mindsdb_native'].get_models()

        if len(query['documents']) != 1:
            raise Exception("Must be inserted just one predictor at time")

        for doc in query['documents']:
            if '_id' in doc:
                del doc['_id']

            bad_columns = [x for x in doc if x not in predictors_columns]
            if len(bad_columns) > 0:
                raise Exception(f"Is no possible insert this columns to 'predictors' collection: {', '.join(bad_columns)}")

            if 'name' not in doc:
                raise Exception("Please, specify 'name' field")

            if 'predict' not in doc:
                raise Exception("Please, specify 'predict' field")

            if doc['name'] in [x['name'] for x in models]:
                raise Exception(f"Predictor with name '{doc['name']}' already exists")

            is_external_datasource = 'external_datasource' in doc and isinstance(doc['external_datasource'], str)
            is_select_data_query = 'select_data_query' in doc and isinstance(doc['select_data_query'], dict)

            if is_external_datasource and is_select_data_query:
                raise Exception("'external_datasource' and 'select_data_query' should not be used in one query")
            elif is_external_datasource is False and is_select_data_query is False:
                raise Exception("in query should be 'external_datasource' or 'select_data_query'")

            kwargs = doc.get('training_options', {})

            if is_select_data_query:
                ds, ds_name = mindsdb_env['data_store'].save_datasource(
                    name=doc['name'],
                    source_type='default_mongodb',
                    source=doc['select_data_query']
                )
            elif is_external_datasource:
                ds = mindsdb_env['data_store'].get_datasource_obj(doc['external_datasource'], raw=True)
                ds_name = doc['external_datasource']

            predict = doc['predict']
            if not isinstance(predict, list):
                predict = [x.strip() for x in predict.split(',')]

            ds_columns = [x['name'] for x in mindsdb_env['data_store'].get_datasource(ds_name)['columns']]
            for col in predict:
                if col not in ds_columns:
                    if is_select_data_query:
                        mindsdb_env['data_store'].delete_datasource(ds_name)
                    raise Exception(f"Column '{col}' not exists")

            mindsdb_env['mindsdb_native'].learn(doc['name'], ds, predict, kwargs)

        result = {
            "n": len(query['documents']),
            "ok": 1
        }

        return result


responder = Responce()
