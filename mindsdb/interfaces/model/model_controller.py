from copy import deepcopy
from mindsdb.interfaces.model.learn_process import LearnProcess
from lightwood.api import predictor
from mindsdb.api.http.namespaces.predictor import Predictor
from typing import Union, Dict, Any
from dateutil.parser import parse as parse_datetime
import psutil
import datetime
import time
import os
import shutil
from contextlib import contextmanager
from packaging import version
import pandas as pd
import lightwood
import autopep8
import mindsdb_datasources
from mindsdb import __version__ as mindsdb_version
from lightwood import __version__ as lightwood_version
import mindsdb.interfaces.storage.db as db
from mindsdb.utilities.fs import create_process_mark, delete_process_mark
from mindsdb.interfaces.database.database import DatabaseWrapper
from mindsdb.utilities.config import Config
from mindsdb.interfaces.storage.fs import FsSotre
from mindsdb.utilities.log import log


class ModelController():
    config: Config
    fs_store: FsSotre
    predictor_cache: Dict[str, Dict[str, Union[Any]]]
    ray_based: bool

    def __init__(self, ray_based: bool) -> None:
        self.config = Config()
        self.fs_store = FsSotre()
        self.predictor_cache = {}
        self.ray_based = ray_based

    def _invalidate_cached_predictors(self) -> None:
        # @TODO: Cache will become stale if the respective ModelInterface is not invoked yet a bunch of predictors remained cached, no matter where we invoke it. In practice shouldn't be a big issue though
        for predictor_name in list(self.predictor_cache.keys()):
            if (datetime.datetime.now() - self.predictor_cache[predictor_name]['created']).total_seconds() > 1200:
                del self.predictor_cache[predictor_name]

    def _lock_predictor(self, id: int, mode: str) -> None:
        from mindsdb.interfaces.storage.db import session, Semaphor

        while True:
            semaphor_record = session.query(Semaphor).filter_by(entity_id=id, entity_type='predictor').first()
            if semaphor_record is not None:
                if mode == 'read' and semaphor_record.action == 'read':
                    return True
            try:
                semaphor_record = Semaphor(entity_id=id, entity_type='predictor', action=mode)
                session.add(semaphor_record)
                session.commit()
                return True
            except Exception:
                pass
            time.sleep(1)

    def _unlock_predictor(self, id: int) -> None:
        from mindsdb.interfaces.storage.db import session, Semaphor
        semaphor_record = session.query(Semaphor).filter_by(entity_id=id, entity_type='predictor').first()
        if semaphor_record is not None:
            session.delete(semaphor_record)
            session.commit()

    @contextmanager
    def _lock_context(self, id, mode: str):
        try:
            self._lock_predictor(id, mode)
            yield True
        finally:
            self._unlock_predictor(id)

    def learn(self, name: str, from_data: dict, to_predict: str, datasource_id: int, kwargs: dict, company_id: int) -> None:
        create_process_mark('learn')

        problem_definition = {'target': to_predict if isinstance(to_predict, str) else to_predict[0]}
        join_learn_process = kwargs.get('join_learn_process', False)
        if 'join_learn_process' in kwargs:
            del kwargs['join_learn_process']

        # Adapt kwargs to problem definition
        if 'timeseries_settings' in kwargs:
            problem_definition['timeseries_settings'] = kwargs['timeseries_settings']

        if 'stop_training_in_x_seconds' in kwargs:
            problem_definition['stop_after'] = kwargs['stop_training_in_x_seconds']

        self.generate_predictor(name, from_data, datasource_id, problem_definition, company_id)

        # TODO: Should we support kwargs['join_learn_process'](?)
        self.fit_predictor(name, from_data, join_learn_process, company_id)

    def predict(self, name: str, when_data: dict, backwards_compatible: bool, company_id: int):
        create_process_mark('predict')
        original_name = name
        name = f'{company_id}@@@@@{name}'

        predictor_record = db.session.query(db.Predictor).filter_by(company_id=company_id, name=original_name).first()
        assert predictor_record is not None
        fs_name = f'predictor_{company_id}_{predictor_record.id}'

        if name not in self.predictor_cache:
            # Clear the cache entirely if we have less than 1.2 GB left
            if psutil.virtual_memory().available < 1.2 * pow(10, 9):
                self.predictor_cache = {}

            if predictor_record.data['status'] == 'complete':
                self.fs_store.get(fs_name, fs_name, self.config['paths']['predictors'])
                self.predictor_cache[name] = {
                    'predictor':
                    lightwood.predictor_from_state(os.path.join(self.config['paths']['predictors'], fs_name)),
                    'created': datetime.datetime.now()
                }

        if isinstance(when_data, dict) and 'kwargs' in when_data and 'args' in when_data:
            ds_cls = getattr(mindsdb_datasources, when_data['class'])
            df = ds_cls(*when_data['args'], **when_data['kwargs']).df
        else:
            # @TODO: Replace with Datasource
            try:
                df = pd.DataFrame(when_data)
            except Exception:
                df = when_data

        predictor = self.predictor_cache[name]['predictor']
        predictions = predictor.predict(df)
        del self.predictor_cache[name]

        delete_process_mark('predict')

        target = predictor_record.to_predict[0]

        if backwards_compatible:
            bc_predictions = []
            for _, row in predictions.iterrows():
                bc_predictions.append({
                    '{}_confidence'.format(target): row['confidence'],
                    '{}_lower_bound'.format(target): row['lower'],
                    '{}_upper_bound'.format(target): row['upper'],
                    '{}_anomaly'.format(target): row['anomaly'],
                    '{}'.format(target): row['prediction'],
                })
            return bc_predictions
        else:
            return [dict(row) for _, row in predictions.iterrows()]

    def analyse_dataset(self, ds: dict, company_id: int) -> lightwood.DataAnalysis:
        create_process_mark('analyse')
        ds_cls = getattr(mindsdb_datasources, ds['class'])
        df = ds_cls(*ds['args'], **ds['kwargs']).df
        analysis = lightwood.analyze_dataset(df)
        delete_process_mark('analyse')
        return analysis.to_dict()  # type: ignore

    def get_model_data(self, name, company_id: int):
        if '@@@@@' in name:
            sn = name.split('@@@@@')
            assert len(sn) < 3  # security
            name = sn[1]

        original_name = name
        name = f'{company_id}@@@@@{name}'

        predictor_record = db.session.query(db.Predictor).filter_by(company_id=company_id, name=original_name).first()
        assert predictor_record is not None

        linked_db_ds = db.session.query(db.Datasource).filter_by(company_id=company_id, id=predictor_record.datasource_id).first()

        # check update availability
        if version.parse(predictor_record.mindsdb_version) < version.parse(mindsdb_version):
            predictor_record.update_status = 'available'
            db.session.commit()

        data = deepcopy(predictor_record.data)
        data['dtype_dict'] = predictor_record.dtype_dict
        data['created_at'] = str(parse_datetime(str(predictor_record.created_at).split('.')[0]))
        data['updated_at'] = str(parse_datetime(str(predictor_record.updated_at).split('.')[0]))
        data['predict'] = predictor_record.to_predict[0]
        data['update'] = predictor_record.update_status
        data['name'] = predictor_record.name
        data['code'] = predictor_record.code
        data['json_ai'] = predictor_record.json_ai
        data['data_source_name'] = linked_db_ds.name if linked_db_ds else None

        return data

    def get_models(self, company_id: int):
        models = []
        for db_p in db.session.query(db.Predictor).filter_by(company_id=company_id):
            model_data = self.get_model_data(db_p.name, company_id=company_id)
            reduced_model_data = {}

            for k in ['name', 'version', 'is_active', 'predict', 'status', 'current_phase', 'accuracy', 'data_source', 'update', 'data_source_name']:
                reduced_model_data[k] = model_data.get(k, None)

            for k in ['train_end_at', 'updated_at', 'created_at']:
                reduced_model_data[k] = model_data.get(k, None)
                if reduced_model_data[k] is not None:
                    try:
                        reduced_model_data[k] = parse_datetime(str(reduced_model_data[k]).split('.')[0])
                    except Exception as e:
                        # @TODO Does this ever happen
                        log.error(f'Date parsing exception while parsing: {k} in get_models: ', e)
                        reduced_model_data[k] = parse_datetime(str(reduced_model_data[k]))

            models.append(reduced_model_data)
        return models

    def delete_model(self, name, company_id: int):
        original_name = name
        name = f'{company_id}@@@@@{name}'

        db_p = db.session.query(db.Predictor).filter_by(company_id=company_id, name=original_name, is_custom=False).first()
        db.session.delete(db_p)
        db.session.commit()

        DatabaseWrapper(company_id).unregister_predictor(name)

        # delete from s3
        self.fs_store.delete(f'predictor_{company_id}_{db_p.id}')

        return 0

    def update_model(self, name: str, company_id: int):
        from mindsdb_worker.updater.update_model import update_model
        from mindsdb.interfaces.storage.db import session, Predictor
        from mindsdb.interfaces.datastore.datastore import DataStore, DataStoreWrapper
        from mindsdb import __version__ as mindsdb_version

        original_name = name
        name = f'{company_id}@@@@@{name}'

        try:
            predictor_record = Predictor.query.filter_by(company_id=company_id, name=original_name, is_custom=False).first()
            assert predictor_record is not None

            predictor_record.update_status = 'updating'

            session.commit()

            # @TODO Fix this function to work with the new lightwood!
            #update_model(name, original_name, self.delete_model, F.rename_model, self.learn_for_update, self._lock_context, company_id, self.config['paths']['predictors'], predictor_record, self.fs_store, DataStoreWrapper(DataStore(), company_id))

            predictor_record = Predictor.query.filter_by(company_id=company_id, name=original_name, is_custom=False).first()

            predictor_record.lightwood_version = lightwood.__version__
            predictor_record.mindsdb_version = mindsdb_version
            predictor_record.update_status = 'up_to_date'

            session.commit()
            
        except Exception as e:
            log.error(e)
            predictor_record.update_status = 'update_failed'  # type: ignore
            session.commit()
            return str(e)
        
        return 'Updated successfully'

    def generate_predictor(self, name: str, from_data: dict, datasource_id, problem_definition_dict: dict, company_id=None):
        print('generate predicrtor start')
        create_process_mark('learn')
        if db.session.query(db.Predictor).filter_by(company_id=company_id, name=name).first() is not None:
            raise Exception('Predictor {} already exists'.format(name))

        if name in self.predictor_cache:
            del self.predictor_cache[name]

        # Here for no particular reason, because we want to run this sometimes but not too often
        self._invalidate_cached_predictors()

        problem_definition = lightwood.ProblemDefinition.from_dict(problem_definition_dict)

        ds_cls = getattr(mindsdb_datasources, from_data['class'])
        ds = ds_cls(*from_data['args'], **from_data['kwargs'])
        df = ds.df

        print(problem_definition, df)
        json_ai = lightwood.json_ai_from_problem(df, problem_definition)
        code = lightwood.code_from_json_ai(json_ai)

        db_p = db.Predictor(
            company_id=company_id,
            name=name,
            json_ai=json_ai.to_dict(),
            code=code,
            datasource_id=datasource_id,
            mindsdb_version=mindsdb_version,
            lightwood_version=lightwood_version,
            to_predict=[problem_definition.target],
            data={'status': 'untrained', 'name': name}
        )
        db.session.add(db_p)
        db.session.commit()
        delete_process_mark('learn')
        print('generate predicrtor end')

    def edit_json_ai(self, name: str, json_ai: dict, company_id=None):
        """Edit an existing predictor's json_ai"""

        predictor_record = db.session.query(db.Predictor).filter_by(company_id=company_id, name=name).first()
        assert predictor_record is not None

        try:
            json_ai: lightwood.JsonAI = lightwood.JsonAI.from_dict(json_ai)  # type: ignore
            code = lightwood.code_from_json_ai(json_ai)
        except Exception as e:
            print(f'Failed to generate predictor from json_ai: {e}')
            return False
        else:
            predictor_record.code = code
            predictor_record.code = json_ai
            db.session.commit()
            return True

    def edit_code(self, name: str, code: str, company_id=None):
        """Edit an existing predictor's code"""

        predictor_record = db.session.query(db.Predictor).filter_by(company_id=company_id, name=name).first()
        assert predictor_record is not None
        
        try:
            # TODO: make this safe from code injection (on lightwood side)
            lightwood.predictor_from_code(code)
        except Exception as e:
            print(f'Failed to generate predictor from json_ai: {e}')
            return False
        else:
            predictor_record.code = code
            predictor_record.json_ai = None
            db.session.commit()
            return True

    def fit_predictor(self, name: str, from_data: dict, join_learn_process: bool, company_id: int):
        create_process_mark('learn')
        print('fit predicrtor start')
        """Train an existing predictor"""

        ds_cls = getattr(mindsdb_datasources, from_data['class'])
        ds = ds_cls(*from_data['args'], **from_data['kwargs'])
        df = ds.df

        predictor_record = db.session.query(db.Predictor).filter_by(company_id=company_id, name=name).first()
        assert predictor_record is not None
        predictor_record.data = {'status': 'training', 'name': name}
        db.session.commit()

        p = LearnProcess(predictor_record.id, df)
        p.start()
        if join_learn_process:
            p.join()

'''
Notes: Remove ray from actors are getting stuck
try:
    from mindsdb_worker.cluster.ray_controller import ray_ify
    import ray
    try:
        ray.init(ignore_reinit_error=True, address='auto')
    except Exception:
        ray.init(ignore_reinit_error=True)
    ModelController = ray_ify(ModelController)
except Exception as e:
    pass
'''