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

from mindsdb.__about__ import __version__ as mindsdb_version
from lightwood import __version__ as lightwood_version
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

    def learn(self, name, from_data, to_predict, datasource_id, kwargs={}, save=True, company_id=None):
        create_process_mark('learn')
        
        problem_definition = {'target': to_predict}

        # TODO add more important values from kwargs to problem_definition
        if 'timeseries_settings' in kwargs:
            problem_definition['timeseries_settings'] = kwargs['timeseries_settings']
        
        # TODO add more important values from kwargs to problem_definition
        if 'stop_training_in_x_seconds' in kwargs:
            problem_definition['stop_after'] = kwargs['stop_training_in_x_seconds']

        self.generate_lightwood_predictor(name, from_data, datasource_id, problem_definition, company_id)

        # TODO: support kwargs['join_learn_process']
        self.fit_predictor(name, from_data, datasource_id, company_id)

        delete_process_mark('learn')
        return 0

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
        create_process_mark('analyse')
        ds_cls = getattr(mindsdb_datasources, ds['class'])
        ds = ds_cls(*ds['args'], **ds['kwargs'])
        analysis = lightwood.api.high_level.analyze_dataset(ds.df)
        delete_process_mark('analyse')
        return analysis.to_dict()

    def get_model_data(self, name, db_fix=True, company_id=None):
        from mindsdb_native import F
        from mindsdb_native.libs.constants.mindsdb import DATA_SUBTYPES
        import torch
        import gc

        if '@@@@@' in name:
            name = name.split('@@@@@')[1]

        original_name = name
        name = f'{company_id}@@@@@{name}'

        db_p = db.session.query(db.Predictor).filter_by(company_id=company_id, name=original_name, is_custom=False).first()
        linked_db_ds = db.session.query(db.Datasource).filter_by(company_id=company_id, id=db_p.datasource_id).first()
        db_p = self._try_outdate_db_status(db_p)
        model = db_p.data
        if model is None or model['status'] == 'training':
            try:
                self.fs_store.get(name, f'predictor_{company_id}_{db_p.id}', self.config['paths']['predictors'])
                new_model_data = F.get_model_data(name)
            except Exception:
                new_model_data = None

            try:
                torch.cuda.empty_cache()
            except Exception:
                pass
            gc.collect()

            if db_p.data is None or (new_model_data is not None and len(new_model_data) > len(db_p.data)):
                db_p.data = new_model_data
                model = new_model_data
                db.session.commit()

            if db_p.data is None:
                if new_model_data is None:
                    db_p.data = {"name": original_name, "status": "error"}
                    model = {"name": original_name, "status": "error"}
                elif len(new_model_data) > len(db_p.data):
                    db_p.data = new_model_data
                    model = new_model_data
                db.session.commit()

        # Make some corrections for databases not to break when dealing with empty columns
        if db_fix:
            data_analysis = model['data_analysis_v2']
            for column in model['columns']:
                analysis = data_analysis.get(column)
                if isinstance(analysis, dict) and (len(analysis) == 0 or analysis.get('empty', {}).get('is_empty', False)):
                    data_analysis[column]['typing'] = {
                        'data_subtype': DATA_SUBTYPES.INT
                    }

        model['created_at'] = str(parse_datetime(str(db_p.created_at).split('.')[0]))
        model['updated_at'] = str(parse_datetime(str(db_p.updated_at).split('.')[0]))
        model['predict'] = db_p.to_predict
        model['update'] = db_p.update_status
        model['name'] = db_p.name
        model['data_source_name'] = linked_db_ds.name if linked_db_ds else None
        return model

    def get_models(self, company_id=None):
        models = []
        for db_p in db.session.query(db.Predictor).filter_by(company_id=company_id, is_custom=False):

            # An old predictor that used to use mindsdb_native
            if db_p.predictor_code is None:
                model_data = self.get_model_data(db_p.name, db_fix=False, company_id=company_id)
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

            # New predictor that uses lightwood
            else:
                lw_p = lightwood.api.high_level.predictor_from_code(db_p.code)
                models.append(lw_p.model_analysis)

        return models

    def delete_model(self, name, company_id=None):
        original_name = name
        name = f'{company_id}@@@@@{name}'

        db_p = db.session.query(db.Predictor).filter_by(company_id=company_id, name=original_name, is_custom=False).first()
        db.session.delete(db_p)
        db.session.commit()

        # NOTE: should this be name or original_name?
        DatabaseWrapper(company_id).unregister_predictor(name)

        # delete locally
        shutil.rmtree(os.path.join(self.config['paths']['predictors'], name))
        
        # delete from s3
        self.fs_store.delete(f'predictor_{company_id}_{db_p.id}')

        return 0

    def update_model(self, name, company_id=None):
        from mindsdb_native import F
        from mindsdb_worker.updater.update_model import update_model
        from mindsdb.interfaces.storage.db import session, Predictor
        from mindsdb.interfaces.datastore.datastore import DataStore, DataStoreWrapper
        from mindsdb_native import __version__ as native_version
        from mindsdb import __version__ as mindsdb_version

        original_name = name
        name = f'{company_id}@@@@@{name}'

        try:
            predictor_record = Predictor.query.filter_by(company_id=company_id, name=original_name, is_custom=False).first()

            predictor_record.update_status = 'updating'

            session.commit()

            update_model(name, original_name, self.delete_model, F.rename_model, self.learn_for_update, self._lock_context, company_id, self.config['paths']['predictors'], predictor_record, self.fs_store, DataStoreWrapper(DataStore(), company_id))

            predictor_record = Predictor.query.filter_by(company_id=company_id, name=original_name, is_custom=False).first()

            predictor_record.native_version = native_version
            predictor_record.mindsdb_version = mindsdb_version
            predictor_record.update_status = 'up_to_date'

            session.commit()
            
        except Exception as e:
            log.error(e)
            predictor_record.update_status = 'update_failed'
            session.commit()
            return str(e)
        
        return 'Updated successfully'

    def generate_lightwood_predictor(self, name: str, from_data: dict, datasource_id, problem_definition: dict, company_id=None):
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

        create_directory(os.path.join(
            self.config['paths']['predictors'],
            '{}@@@@@{}'.format(company_id, name)
        ))

        db_p = db.Predictor(
            company_id=company_id,
            name=name,
            json_ai=json_ai.to_dict(),
            predictor_code=predictor_code,
            datasource_id=datasource_id,
            mindsdb_version=mindsdb_version,
            lightwood_version=lightwood_version
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

    def fit_predictor(self, name: str, from_data: dict, datasource_id, company_id=None):
        """Train an existing predictor"""

        ds_cls = getattr(mindsdb_datasources, from_data['class'])
        ds = ds_cls(*from_data['args'], **from_data['kwargs'])
        df = ds.df

        # TODO: set db_p.data['status'] to 'training'
        db_p = db.session.query(db.Predictor).filter_by(company_id=company_id, name=name).first()

        lw_p = lightwood.api.high_level.predictor_from_code(db_p.predictor_code)
        lw_p.learn(df)

        # save predictor locally
        lw_p.save(os.path.join(self.config['paths']['predictors'], name))
        
        # save predictor to s3
        self.fs_store.put(name, f'predictor_{company_id}_{db_p.id}', self.config['paths']['predictors'])

    def code_from_json_ai(self, json_ai: dict, company_id=None):
        json_ai = lightwood.api.types.JsonAI.from_dict(json_ai)
        if lightwood.api.json_ai.validate_json_ai(json_ai):
            return lightwood.api.high_level.code_from_json_ai(json_ai)
        else:
            return None


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