# @TODO, replace with arrow later: https://mirai-solutions.ch/news/2020/06/11/apache-arrow-flight-tutorial/
import xmlrpc
from xmlrpc.server import SimpleXMLRPCServer
from dateutil.parser import parse as parse_datetime
import os
import pickle
from pathlib import Path
import psutil
import datetime

from mindsdb.utilities.fs import create_directory
from mindsdb.interfaces.database.database import DatabaseWrapper
from mindsdb.utilities.config import Config
from mindsdb.interfaces.storage.fs import FsSotre
from mindsdb.utilities.log import log

class ModelController():
    def __init__(self):
        self.config = Config()
        self.fs_store = FsSotre()
        self.company_id = os.environ.get('MINDSDB_COMPANY_ID', None)
        self.dbw = DatabaseWrapper()
        self.predictor_cache = {}

    def _invalidate_cached_predictors(self):
        from mindsdb_datasources import FileDS, ClickhouseDS, MariaDS, MySqlDS, PostgresDS, MSSQLDS, MongoDS, SnowflakeDS, AthenaDS
        import mindsdb_native
        from mindsdb_native import F
        from mindsdb_native.libs.constants.mindsdb import DATA_SUBTYPES
        from mindsdb.interfaces.storage.db import session, Predictor


        # @TODO: Cache will become stale if the respective NativeInterface is not invoked yet a bunch of predictors remained cached, no matter where we invoke it. In practice shouldn't be a big issue though
        for predictor_name in list(self.predictor_cache.keys()):
            if (datetime.datetime.now() - self.predictor_cache[predictor_name]['created']).total_seconds() > 1200:
                del self.predictor_cache[predictor_name]

    def _setup_for_creation(self, name):
        from mindsdb_datasources import FileDS, ClickhouseDS, MariaDS, MySqlDS, PostgresDS, MSSQLDS, MongoDS, SnowflakeDS, AthenaDS
        import mindsdb_native
        from mindsdb_native import F
        from mindsdb_native.libs.constants.mindsdb import DATA_SUBTYPES
        from mindsdb.interfaces.storage.db import session, Predictor


        if name in self.predictor_cache:
            del self.predictor_cache[name]
        # Here for no particular reason, because we want to run this sometimes but not too often
        self._invalidate_cached_predictors()

        predictor_dir = Path(self.config.paths['predictors']).joinpath(name)
        create_directory(predictor_dir)
        predictor_record = Predictor(company_id=self.company_id, name=name, is_custom=False)

        session.add(predictor_record)
        session.commit()

    def create(self, name):
        from mindsdb_datasources import FileDS, ClickhouseDS, MariaDS, MySqlDS, PostgresDS, MSSQLDS, MongoDS, SnowflakeDS, AthenaDS
        import mindsdb_native
        from mindsdb_native import F
        from mindsdb_native.libs.constants.mindsdb import DATA_SUBTYPES
        from mindsdb.interfaces.storage.db import session, Predictor


        self._setup_for_creation(name)
        predictor = mindsdb_native.Predictor(name=name, run_env={'trigger': 'mindsdb'})
        return predictor

    def learn(self, name, from_data, to_predict, datasource_id, kwargs={}):
        from mindsdb.interfaces.model.learn_process import LearnProcess

        join_learn_process = kwargs.get('join_learn_process', False)
        if 'join_learn_process' in kwargs:
            del kwargs['join_learn_process']

        self._setup_for_creation(name)

        p = LearnProcess(name, from_data, to_predict, kwargs, datasource_id)
        p.start()
        if join_learn_process is True:
            p.join()
            if p.exitcode != 0:
                raise Exception('Learning process failed !')
        return 0

    def predict(self, name, pred_format, when_data=None, kwargs={}):
        from mindsdb_datasources import FileDS, ClickhouseDS, MariaDS, MySqlDS, PostgresDS, MSSQLDS, MongoDS, SnowflakeDS, AthenaDS
        import mindsdb_native
        from mindsdb.interfaces.storage.db import session, Predictor

        if name not in self.predictor_cache:
            # Clear the cache entirely if we have less than 1.2 GB left
            if psutil.virtual_memory().available < 1.2 * pow(10,9):
                self.predictor_cache = {}

            predictor_record = Predictor.query.filter_by(company_id=self.company_id, name=name, is_custom=False).first()
            if predictor_record.data['status'] == 'complete':
                self.fs_store.get(name, f'predictor_{self.company_id}_{predictor_record.id}', self.config['paths']['predictors'])
                self.predictor_cache[name] = {
                    'predictor': mindsdb_native.Predictor(name=name, run_env={'trigger': 'mindsdb'}),
                    'created': datetime.datetime.now()
                }

        if isinstance(when_data, dict) and 'kwargs' in when_data:
            when_data = eval(when_data['class'])(*when_data['args'], **when_data['kwargs'])

        predictions = self.predictor_cache[name]['predictor'].predict(
            when_data=when_data,
            **kwargs
        )
        if pred_format == 'explain' or pred_format == 'new_explain':
            predictions = [p.explain() for p in predictions]
        elif pred_format == 'dict':
            predictions = [p.as_dict() for p in predictions]
        elif pred_format == 'dict&explain':
            predictions = ([p.as_dict() for p in predictions], [p.explain() for p in predictions])
        else:
            raise Exception(f'Unkown predictions format: {pred_format}')

        return xmlrpc.client.Binary(pickle.dumps(predictions))

    def analyse_dataset(self, ds):
        from mindsdb_datasources import FileDS, ClickhouseDS, MariaDS, MySqlDS, PostgresDS, MSSQLDS, MongoDS, SnowflakeDS, AthenaDS
        from mindsdb_native import F

        ds = eval(ds['class'])(*ds['args'], **ds['kwargs'])
        analysis =  F.analyse_dataset(ds)
        return xmlrpc.client.Binary(pickle.dumps(analysis))

    def get_model_data(self, name, db_fix=True):
        from mindsdb_native import F
        from mindsdb_native.libs.constants.mindsdb import DATA_SUBTYPES
        from mindsdb.interfaces.storage.db import session, Predictor


        predictor_record = Predictor.query.filter_by(company_id=self.company_id, name=name, is_custom=False).first()
        model = predictor_record.data
        if model is None or model['status'] == 'training':
            try:
                self.fs_store.get(name, f'predictor_{self.company_id}_{predictor_record.id}', self.config['paths']['predictors'])
                new_model_data = mindsdb_native.F.get_model_data(name)
            except Exception:
                new_model_data = None

            if predictor_record.data is None or (new_model_data is not None and len(new_model_data) > len(predictor_record.data)):
                predictor_record.data = new_model_data
                model = new_model_data
                session.commit()

        # Make some corrections for databases not to break when dealing with empty columns
        if db_fix:
            data_analysis = model['data_analysis_v2']
            for column in model['columns']:
                analysis = data_analysis.get(column)
                if isinstance(analysis, dict) and (len(analysis) == 0 or analysis.get('empty', {}).get('is_empty', False)):
                    data_analysis[column]['typing'] = {
                        'data_subtype': DATA_SUBTYPES.INT
                    }

        model['created_at'] = predictor_record.created_at
        model['updated_at'] = predictor_record.updated_at
        print('\n\n\n', model['created_at'], model['updated_at'], '\n\n\n')
        return xmlrpc.client.Binary(pickle.dumps(model))

    def get_models(self):
        from mindsdb.interfaces.storage.db import session, Predictor

        models = []
        predictor_records = Predictor.query.filter_by(company_id=self.company_id, is_custom=False)
        predictor_names = [
            x.name for x in predictor_records
        ]
        for model_name in predictor_names:
            try:
                bin = self.get_model_data(model_name, db_fix=False)
                model_data = pickle.loads(bin.data)
                reduced_model_data = {}

                for k in ['name', 'version', 'is_active', 'predict', 'status', 'current_phase', 'accuracy', 'data_source']:
                    reduced_model_data[k] = model_data.get(k, None)

                for k in ['train_end_at', 'updated_at', 'created_at']:
                    reduced_model_data[k] = model_data.get(k, None)
                    if reduced_model_data[k] is not None:
                        try:
                            reduced_model_data[k] = parse_datetime(str(reduced_model_data[k]).split('.')[0])
                        except Exception as e:
                            # @TODO Does this ever happen
                            print(f'Date parsing exception while parsing: {k} in get_models: ', e)
                            reduced_model_data[k] = parse_datetime(str(reduced_model_data[k]))

                models.append(reduced_model_data)
            except Exception as e:
                print(f"Can't list data for model: '{model_name}' when calling `get_models(), error: {e}`")

        return xmlrpc.client.Binary(pickle.dumps(models))

    def delete_model(self, name):
        from mindsdb_native import F
        from mindsdb_native.libs.constants.mindsdb import DATA_SUBTYPES
        from mindsdb.interfaces.storage.db import session, Predictor

        predictor_record = Predictor.query.filter_by(company_id=self.company_id, name=name, is_custom=False).first()
        id = predictor_record.id
        session.delete(predictor_record)
        session.commit()
        F.delete_model(name)
        self.dbw.unregister_predictor(name)
        self.fs_store.delete(f'predictor_{self.company_id}_{id}')
        return 0

def ping(): return True

def start():
    controller = ModelController()
    server = SimpleXMLRPCServer(("localhost", 19329))

    server.register_function(controller.create, "create")
    server.register_function(controller.learn, "learn")
    server.register_function(controller.predict, "predict")
    server.register_function(controller.analyse_dataset, "analyse_dataset")
    server.register_function(controller.get_model_data, "get_model_data")
    server.register_function(controller.get_models, "get_models")
    server.register_function(controller.delete_model, "delete_model")
    server.register_function(ping, "ping")

    server.serve_forever()
