# @TODO, replace with arrow later: https://mirai-solutions.ch/news/2020/06/11/apache-arrow-flight-tutorial/
from dateutil.parser import parse as parse_datetime
import os
import ray

from mindsdb.utilities.fs import create_directory
from mindsdb.interfaces.database.database import DatabaseWrapper
from mindsdb.utilities.config import Config
from mindsdb.interfaces.storage.fs import FsSotre
from mindsdb.utilities.log import log


@ray.remote
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
        from mindsdb.utilities.os_specific import get_mp_context
        from mindsdb.interfaces.storage.db import session, Predictor
        from mindsdb.interfaces.storage.fs import FsSotre
        from mindsdb.utilities.config import Config

        from mindsdb_datasources import FileDS, ClickhouseDS, MariaDS, MySqlDS, PostgresDS, MSSQLDS, MongoDS, SnowflakeDS, AthenaDS
        import mindsdb_native
        from mindsdb_native import F
        from mindsdb_native.libs.constants.mindsdb import DATA_SUBTYPES
        from mindsdb.interfaces.storage.db import session, Predictor


        mdb = mindsdb_native.Predictor(name=name, run_env={'trigger': 'mindsdb'})

        predictor_record = Predictor.query.filter_by(company_id=company_id, name=name).first()
        predictor_record.datasource_id = datasource_id
        predictor_record.to_predict = to_predict
        predictor_record.version = mindsdb_native.__version__
        predictor_record.data = {
            'name': name,
            'status': 'training'
        }
        #predictor_record.datasource_id = ... <-- can be done once `learn` is passed a datasource name
        session.commit()

        to_predict = to_predict if isinstance(to_predict, list) else [to_predict]
        data_source = getattr(mindsdb_native, from_data['class'])(*from_data['args'], **from_data['kwargs'])

        try:
            mdb.learn(
                from_data=data_source,
                to_predict=to_predict,
                **kwargs
            )
        except Exception:
            pass

        self.fs_store.put(name, f'predictor_{company_id}_{predictor_record.id}', config['paths']['predictors'])

        model_data = mindsdb_native.F.get_model_data(name)

        predictor_record = Predictor.query.filter_by(company_id=company_id, name=name).first()
        predictor_record.data = model_data
        session.commit()

        self.dbw.register_predictors([model_data])

    def predict(self, name, when_data=None, kwargs={}):
        from mindsdb_datasources import FileDS, ClickhouseDS, MariaDS, MySqlDS, PostgresDS, MSSQLDS, MongoDS, SnowflakeDS, AthenaDS
        import mindsdb_native

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

    def analyse_dataset(self, ds):
        from mindsdb_datasources import FileDS, ClickhouseDS, MariaDS, MySqlDS, PostgresDS, MSSQLDS, MongoDS, SnowflakeDS, AthenaDS
        from mindsdb_native import F

        ds = eval(ds['class'])(*ds['args'], **ds['kwargs'])
        return F.analyse_dataset(ds)

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

        return model

    def get_models(self):
        from mindsdb.interfaces.storage.db import session, Predictor

        models = []
        predictor_names = [
            x.name for x in Predictor.query.filter_by(company_id=self.company_id, is_custom=False)
        ]
        for model_name in predictor_names:
            try:
                model_data = self.get_model_data(model_name, db_fix=False)
                if model_data['status'] == 'training' and parse_datetime(model_data['created_at']) < parse_datetime(self.config['mindsdb_last_started_at']):
                    continue

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

        return models

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
