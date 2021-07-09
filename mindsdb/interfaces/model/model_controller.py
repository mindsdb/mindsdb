from mindsdb.api.http.namespaces.predictor import Predictor
from dateutil.parser import parse as parse_datetime
import pickle
from pathlib import Path
import psutil
import datetime
import time
import os
import shutil
from contextlib import contextmanager

import pandas as pd
import lightwood
import autopep8
import mindsdb_datasources

import mindsdb.interfaces.storage.db as db
from mindsdb.utilities.fs import create_directory, create_process_mark, delete_process_mark
from mindsdb.interfaces.database.database import DatabaseWrapper
from mindsdb.utilities.config import Config
from mindsdb.interfaces.storage.fs import FsSotre
from mindsdb.utilities.log import Log


class ModelController():
    def __init__(self, ray_based):
        self.config = Config()
        self.fs_store = FsSotre()
        self.predictor_cache = {}
        self.ray_based = ray_based

    def _invalidate_cached_predictors(self):
        from mindsdb_datasources import (FileDS, ClickhouseDS, MariaDS, MySqlDS,
                                         PostgresDS, MSSQLDS, MongoDS,
                                         SnowflakeDS, AthenaDS)
        from mindsdb.interfaces.storage.db import session, Predictor

        # @TODO: Cache will become stale if the respective NativeInterface is not invoked yet a bunch of predictors remained cached, no matter where we invoke it. In practice shouldn't be a big issue though
        for predictor_name in list(self.predictor_cache.keys()):
            if (datetime.datetime.now() - self.predictor_cache[predictor_name]['created']).total_seconds() > 1200:
                del self.predictor_cache[predictor_name]

    def _lock_predictor(self, id, mode='write', company_id=None):
        from mindsdb.interfaces.storage.db import session, Semaphor

        while True:
            semaphor_record = session.query(Semaphor).filter_by(company_id=company_id, entity_id=id, entity_type='predictor').first()
            if semaphor_record is not None:
                if mode == 'read' and semaphor_record.action == 'read':
                    return True
            try:
                semaphor_record = Semaphor(company_id=company_id, entity_id=id, entity_type='predictor', action=mode)
                session.add(semaphor_record)
                session.commit()
                return True
            except Exception:
                pass
            time.sleep(1)

    def _unlock_predictor(self, id, company_id=None):
        from mindsdb.interfaces.storage.db import session, Semaphor
        semaphor_record = session.query(Semaphor).filter_by(company_id=company_id, entity_id=id, entity_type='predictor').first()
        if semaphor_record is not None:
            session.delete(semaphor_record)
            session.commit()

    @contextmanager
    def _lock_context(self, id, mode='write'):
        try:
            self._lock_predictor(id, mode)
            yield True
        finally:
            self._unlock_predictor(id)

    def _setup_for_creation(self, name, original_name, company_id=None):
        from mindsdb.interfaces.storage.db import session, Predictor

        if name in self.predictor_cache:
            del self.predictor_cache[name]
        # Here for no particular reason, because we want to run this sometimes but not too often
        self._invalidate_cached_predictors()

        predictor_record = Predictor.query.filter_by(company_id=company_id, name=original_name).first()
        if predictor_record is not None:
            raise Exception(f'Predictor with name {original_name} already exists.')

        predictor_dir = Path(self.config['paths']['predictors']).joinpath(name)
        create_directory(predictor_dir)
        predictor_record = Predictor(company_id=company_id, name=original_name, is_custom=False)

        session.add(predictor_record)
        session.commit()

    def _try_outdate_db_status(self, predictor_record):
        from mindsdb import __version__ as mindsdb_version
        from mindsdb.interfaces.storage.db import session
        from packaging import version

        if predictor_record.update_status == 'update_failed':
            return predictor_record

        if version.parse(predictor_record.mindsdb_version) < version.parse(mindsdb_version):
            predictor_record.update_status = 'available'

        session.commit()
        return predictor_record

    def predict(self, name, pred_format, when_data=None, kwargs={}, company_id=None):
        from mindsdb_datasources import (FileDS, ClickhouseDS, MariaDS,
                                         MySqlDS, PostgresDS, MSSQLDS, MongoDS,
                                         SnowflakeDS, AthenaDS)
        import mindsdb_native
        from mindsdb.interfaces.storage.db import session, Predictor

        create_process_mark('predict')
        original_name = name
        name = f'{company_id}@@@@@{name}'

        if name not in self.predictor_cache:
            # Clear the cache entirely if we have less than 1.2 GB left
            if psutil.virtual_memory().available < 1.2 * pow(10, 9):
                self.predictor_cache = {}

            predictor_record = Predictor.query.filter_by(company_id=company_id, name=original_name, is_custom=False).first()
            if predictor_record.data['status'] == 'complete':
                self.fs_store.get(name, f'predictor_{company_id}_{predictor_record.id}', self.config['paths']['predictors'])
                self.predictor_cache[name] = {
                    'predictor': mindsdb_native.Predictor(name=name, run_env={'trigger': 'mindsdb'}),
                    'created': datetime.datetime.now()
                }
                predictor = mindsdb_native.Predictor(name=name, run_env={'trigger': 'mindsdb'})

        if isinstance(when_data, dict) and 'kwargs' in when_data and 'args' in when_data:
            data_source = getattr(mindsdb_datasources, when_data['class'])(*when_data['args'], **when_data['kwargs'])
        else:
            # @TODO: Replace with Datasource
            try:
                data_source = pd.DataFrame(when_data)
            except Exception:
                data_source = when_data

        predictor = self.predictor_cache[name]['predictor']
        predictions = predictor.predict(
            when_data=data_source,
            **kwargs
        )
        del self.predictor_cache[name]
        if pred_format == 'explain' or pred_format == 'new_explain':
            predictions = [p.explain() for p in predictions]
        elif pred_format == 'dict':
            predictions = [p.as_dict() for p in predictions]
        elif pred_format == 'dict&explain':
            predictions = [[p.as_dict() for p in predictions], [p.explain() for p in predictions]]
        else:
            delete_process_mark('predict')
            raise Exception(f'Unkown predictions format: {pred_format}')

        delete_process_mark('predict')

        return predictions

    def analyse_dataset(self, ds, company_id=None):
        ds_cls = getattr(mindsdb_datasources, ds['class'])
        ds = ds_cls(*ds['args'], **ds['kwargs'])
        return lightwood.api.high_level.analyze_dataset(ds.df)

    def get_model_data(self, name, company_id=None):
        db_p = db.session.query(db.Predictor).filter_by(company_id=company_id, name=name).first()
        lw_p = lightwood.api.Predictor.load(os.path.join(self.config['paths']['predictors'], name))
        return lw_p.model_analysis.to_dict()

    def get_models(self, company_id=None):
        predictors_db = db.session.query(db.Predictor).filter_by(company_id=company_id)
        return [self.get_model_data(db_p.name) for db_p in predictors_db]

    def delete_model(self, name, company_id=None):
        db_p = db.session.query(db.Predictor).filter_by(company_id=company_id, name=name).first()
        db.session.delete(db_p)
        db.session.commit()

        DatabaseWrapper(company_id).unregister_predictor(name)

        # Remove locally
        shutil.rmtree(os.path.join(self.config['paths']['predictors'], name))

        # Remove from s3
        self.fs_store.delete(f'predictor_{company_id}_{db_p.id}')

    def generate_lightwood_predictor(self, name: str, from_data: dict, problem_definition: dict, company_id=None):
        if db.session.query(db.Predictor).filter_by(company_id=company_id, name=name).first() is not None:
            raise Exception('Predictor {} already exists'.format(name))

        problem_definition = lightwood.api.types.ProblemDefinition.from_dict(problem_definition)

        ds_cls = getattr(mindsdb_datasources, from_data['class'])
        ds = ds_cls(*from_data['args'], **from_data['kwargs'])
        df = ds.df

        type_information = lightwood.data.infer_types(df, problem_definition.pct_invalid)
        statistical_analysis = lightwood.data.statistical_analysis(df, type_information, problem_definition)
        json_ai = lightwood.api.json_ai.generate_json_ai(type_information=type_information, statistical_analysis=statistical_analysis, problem_definition=problem_definition)
        predictor_code = lightwood.api.high_level.code_from_json_ai(json_ai)
        predictor_code = autopep8.fix_code(predictor_code)  # Note: ~3s overhead, might be more depending on source complexity, should try a few more examples and make a decision

        db_p = db.Predictor(
            company_id=company_id,
            name=name,
            json_ai=json_ai.to_dict(),
            predictor_code=predictor_code,
        )
        db.session.add(db_p)
        db.session.commit()

    def edit_json_ai(self, name: str, json_ai: dict, company_id=None):
        """Edit an existing predictor's json_ai"""

        db_p = db.session.query(db.Predictor).filter_by(company_id=company_id, name=name).first()

        try:
            code = lightwood.api.generate_predictor_code(
                lightwood.api.types.JsonAI.from_dict(json_ai)
            )
        except Exception as e:
            print(f'Failed to generate predictor from json_ai: {e}')
            return False
        else:
            db_p.predictor_code = code
            db_p.json_ai = json_ai
            db.session.commit()
            return True

    def edit_code(self, name: str, code: str, company_id=None):
        """Edit an existing predictor's code"""

        db_p = db.session.query(db.Predictor).filter_by(company_id=company_id, name=name).first()
        
        try:
            # TODO: make this safe from code injection (on lightwood side)
            lightwood.api.high_level.predictor_from_code(code)
        except Exception as e:
            print(f'Failed to generate predictor from json_ai: {e}')
            return False
        else:
            db_p.predictor_code = code
            db_p.json_ai = None
            db.session.commit()
            return True

    def fit_predictor(self, name: str, from_data: dict, company_id=None):
        """Train an existing predictor"""

        ds_cls = getattr(mindsdb_datasources, from_data['class'])
        ds = ds_cls(*from_data['args'], **from_data['kwargs'])
        df = ds.df

        db_p = db.session.query(db.Predictor).filter_by(company_id=company_id, name=name).first()

        lw_p = lightwood.api.high_level.predictor_from_code(db_p.predictor_code)
        lw_p.learn(df)

        # save predictor locally
        lw_p.save(os.path.join(self.config['paths']['predictors'], name))
        
        # save predictor to s3
        self.fs_store.put(name, f'predictor_{company_id}_{db_p.id}', self.config['paths']['predictors'])

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