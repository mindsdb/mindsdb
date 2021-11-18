from mindsdb.api.mongo.classes import Responder
from mindsdb.interfaces.storage.db import session, Datasource
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
            'training_options',
            'connection'
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

            select_data_query = doc.get('select_data_query')
            if select_data_query is None:
                raise Exception("'select_data_query' must be in query")

            kwargs = doc.get('training_options', {})

            integrations = mindsdb_env['datasource_controller'].get_db_integrations().keys()
            connection = doc.get('connection')
            if connection is None:
                if 'default_mongodb' in integrations:
                    connection = 'default_mongodb'
                else:
                    for integration in integrations:
                        if integration.startswith('mongodb_'):
                            connection = integration
                            break

            if connection is None:
                raise Exception("Can't find connection for data source")

            ds_name = mindsdb_env['data_store'].get_vacant_name(doc['name'])

            select_data_query = select_data_query if isinstance(select_data_query, dict) else {'query': select_data_query}
            ds = mindsdb_env['data_store'].save_datasource(
                name=ds_name,
                source_type=connection,
                source=select_data_query
            )

            predict = doc['predict']
            if not isinstance(predict, list):
                predict = [x.strip() for x in predict.split(',')]

            ds_columns = [x['name'] for x in mindsdb_env['data_store'].get_datasource(ds_name)['columns']]
            for col in predict:
                if col not in ds_columns:
                    mindsdb_env['data_store'].delete_datasource(ds_name)
                    raise Exception(f"Column '{col}' not exists")

            datasource_record = session.query(Datasource).filter_by(company_id=mindsdb_env['company_id'], name=ds_name).first()
            mindsdb_env['mindsdb_native'].learn(
                doc['name'],
                ds,
                predict,
                datasource_record.id,
                kwargs=dict(kwargs),
                delete_ds_on_fail=True
            )

        result = {
            "n": len(query['documents']),
            "ok": 1
        }

        return result


responder = Responce()
