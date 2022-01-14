import os
import sys
import time
import json
import psutil
import datetime
from copy import deepcopy
from contextlib import contextmanager
from dateutil.parser import parse as parse_datetime
from typing import Optional, Tuple, Union, Dict, Any

import lightwood
from lightwood.api.types import ProblemDefinition
from lightwood import __version__ as lightwood_version
import numpy as np
import pandas as pd
from pandas.core.frame import DataFrame
import mindsdb_datasources

from mindsdb import __version__ as mindsdb_version
import mindsdb.interfaces.storage.db as db
from mindsdb.utilities.functions import mark_process
from mindsdb.interfaces.database.database import DatabaseWrapper
from mindsdb.utilities.config import Config
from mindsdb.interfaces.storage.fs import FsStore
from mindsdb.utilities.log import log
from mindsdb.interfaces.model.learn_process import LearnProcess, GenerateProcess, FitProcess, UpdateProcess
from mindsdb.interfaces.datastore.datastore import DataStore

IS_PY36 = sys.version_info[1] <= 6


class ModelController():
    config: Config
    fs_store: FsStore
    predictor_cache: Dict[str, Dict[str, Union[Any]]]
    ray_based: bool

    def __init__(self, ray_based: bool) -> None:
        self.config = Config()
        self.fs_store = FsStore()
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

    def _get_from_data_df(self, from_data: dict) -> DataFrame:
        ds_cls = getattr(mindsdb_datasources, from_data['class'])
        ds = ds_cls(*from_data['args'], **from_data['kwargs'])
        return ds.df

    def _unpack_old_args(
        self, from_data: dict, kwargs: dict, to_predict: Optional[Union[str, list]] = None
    ) -> Tuple[pd.DataFrame, ProblemDefinition, bool]:
        problem_definition = kwargs or {}
        if isinstance(to_predict, str):
            problem_definition['target'] = to_predict
        elif isinstance(to_predict, list) and len(to_predict) == 1:
            problem_definition['target'] = to_predict[0]
        elif problem_definition.get('target') is None:
            raise Exception(
                f"Predict target must be 'str' or 'list' with 1 element. Got: {to_predict}"
            )

        join_learn_process = kwargs.get('join_learn_process', False)
        if 'join_learn_process' in kwargs:
            del kwargs['join_learn_process']

        # Adapt kwargs to problem definition
        if 'timeseries_settings' in kwargs:
            problem_definition['timeseries_settings'] = kwargs['timeseries_settings']

        if 'stop_training_in_x_seconds' in kwargs:
            problem_definition['time_aim'] = kwargs['stop_training_in_x_seconds']

        if kwargs.get('ignore_columns') is not None:
            problem_definition['ignore_features'] = kwargs['ignore_columns']

        if (
            problem_definition.get('ignore_features') is not None
            and isinstance(problem_definition['ignore_features'], list) is False
        ):
            problem_definition['ignore_features'] = [problem_definition['ignore_features']]

        df = self._get_from_data_df(from_data)

        return df, problem_definition, join_learn_process

    @mark_process(name='learn')
    def learn(self, name: str, from_data: dict, to_predict: str, datasource_id: int, kwargs: dict,
              company_id: int, delete_ds_on_fail: Optional[bool] = False) -> None:
        predictor_record = db.session.query(db.Predictor).filter_by(company_id=company_id, name=name).first()
        if predictor_record is not None:
            raise Exception('Predictor name must be unique.')

        df, problem_definition, join_learn_process = self._unpack_old_args(from_data, kwargs, to_predict)

        problem_definition = ProblemDefinition.from_dict(problem_definition)
        predictor_record = db.Predictor(
            company_id=company_id,
            name=name,
            datasource_id=datasource_id,
            mindsdb_version=mindsdb_version,
            lightwood_version=lightwood_version,
            to_predict=problem_definition.target,
            learn_args=problem_definition.to_dict(),
            data={'name': name}
        )

        db.session.add(predictor_record)
        db.session.commit()
        predictor_id = predictor_record.id

        p = LearnProcess(df, problem_definition, predictor_id, delete_ds_on_fail)
        p.start()
        if join_learn_process:
            p.join()
            if not IS_PY36:
                p.close()
        db.session.refresh(predictor_record)

        data = {}
        if predictor_record.update_status == 'available':
            data['status'] = 'complete'
        elif predictor_record.json_ai is None and predictor_record.code is None:
            data['status'] = 'generating'
        elif predictor_record.data is None:
            data['status'] = 'editable'
        elif 'training_log' in predictor_record.data:
            data['status'] = 'training'
        elif 'error' not in predictor_record.data:
            data['status'] = 'complete'
        else:
            data['status'] = 'error'

    @mark_process(name='predict')
    def predict(self, name: str, when_data: Union[dict, list, pd.DataFrame], pred_format: str, company_id: int):
        original_name = name
        name = f'{company_id}@@@@@{name}'

        predictor_record = db.session.query(db.Predictor).filter_by(company_id=company_id, name=original_name).first()
        assert predictor_record is not None
        predictor_data = self.get_model_data(name, company_id)
        fs_name = f'predictor_{company_id}_{predictor_record.id}'

        if (
            name in self.predictor_cache
            and self.predictor_cache[name]['updated_at'] != predictor_record.updated_at
        ):
            del self.predictor_cache[name]

        if name not in self.predictor_cache:
            # Clear the cache entirely if we have less than 1.2 GB left
            if psutil.virtual_memory().available < 1.2 * pow(10, 9):
                self.predictor_cache = {}

            if predictor_data['status'] == 'complete':
                self.fs_store.get(fs_name, fs_name, self.config['paths']['predictors'])
                self.predictor_cache[name] = {
                    'predictor': lightwood.predictor_from_state(
                        os.path.join(self.config['paths']['predictors'], fs_name),
                        predictor_record.code
                    ),
                    'updated_at': predictor_record.updated_at,
                    'created': datetime.datetime.now(),
                    'code': predictor_record.code,
                    'pickle': str(os.path.join(self.config['paths']['predictors'], fs_name))
                }
            else:
                raise Exception(
                    f'Trying to predict using predictor {original_name} with status: {predictor_data["status"]}. Error is: {predictor_data.get("error", "unknown")}'
                )

        if isinstance(when_data, dict) and 'kwargs' in when_data and 'args' in when_data:
            ds_cls = getattr(mindsdb_datasources, when_data['class'])
            df = ds_cls(*when_data['args'], **when_data['kwargs']).df
        else:
            if isinstance(when_data, dict):
                when_data = [when_data]
            df = pd.DataFrame(when_data)

        predictions = self.predictor_cache[name]['predictor'].predict(df)
        predictions = predictions.to_dict(orient='records')
        # Bellow is useful for debugging caching and storage issues
        # del self.predictor_cache[name]

        target = predictor_record.to_predict[0]
        if pred_format in ('explain', 'dict', 'dict&explain'):
            explain_arr = []
            dict_arr = []
            for i, row in enumerate(predictions):
                explain_arr.append({
                    target: {
                        'predicted_value': row['prediction'],
                        'confidence': row.get('confidence', None),
                        'confidence_lower_bound': row.get('lower', None),
                        'confidence_upper_bound': row.get('upper', None),
                        'anomaly': row.get('anomaly', None),
                        'truth': row.get('truth', None)
                    }
                })

                td = {'predicted_value': row['prediction']}
                for col in df.columns:
                    if col in row:
                        td[col] = row[col]
                    elif f'order_{col}' in row:
                        td[col] = row[f'order_{col}']
                    elif f'group_{col}' in row:
                        td[col] = row[f'group_{col}']
                    else:
                        orginal_index = row.get('original_index')
                        if orginal_index is None:
                            log.warning('original_index is None')
                            orginal_index = i
                        td[col] = df.iloc[orginal_index][col]
                dict_arr.append({target: td})
            if pred_format == 'explain':
                return explain_arr
            elif pred_format == 'dict':
                return dict_arr
            elif pred_format == 'dict&explain':
                return dict_arr, explain_arr
        # New format -- Try switching to this in 2-3 months for speed, for now above is ok
        else:
            return predictions

    @mark_process(name='analyse')
    def analyse_dataset(self, ds: dict, company_id: int) -> lightwood.DataAnalysis:
        ds_cls = getattr(mindsdb_datasources, ds['class'])
        df = ds_cls(*ds['args'], **ds['kwargs']).df
        analysis = lightwood.analyze_dataset(df)
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

        data = deepcopy(predictor_record.data)
        data['dtype_dict'] = predictor_record.dtype_dict
        data['created_at'] = str(parse_datetime(str(predictor_record.created_at).split('.')[0]))
        data['updated_at'] = str(parse_datetime(str(predictor_record.updated_at).split('.')[0]))
        data['predict'] = predictor_record.to_predict[0]
        data['update'] = predictor_record.update_status
        data['mindsdb_version'] = predictor_record.mindsdb_version
        data['name'] = predictor_record.name
        data['code'] = predictor_record.code
        data['json_ai'] = predictor_record.json_ai
        data['data_source_name'] = linked_db_ds.name if linked_db_ds else None
        data['problem_definition'] = predictor_record.learn_args

        # assume older models are complete, only temporary
        if 'error' in predictor_record.data:
            data['status'] = 'error'
        elif predictor_record.update_status == 'available':
            data['status'] = 'complete'
        elif predictor_record.json_ai is None and predictor_record.code is None:
            data['status'] = 'generating'
        elif predictor_record.data is None:
            data['status'] = 'editable'
        elif 'training_log' in predictor_record.data:
            data['status'] = 'training'
        elif 'error' not in predictor_record.data:
            data['status'] = 'complete'
        else:
            data['status'] = 'error'

        if data.get('accuracies', None) is not None:
            if len(data['accuracies']) > 0:
                data['accuracy'] = float(np.mean(list(data['accuracies'].values())))
        return data

    def get_model_description(self, name: str, company_id: int):
        """
        Similar to `get_model_data` but meant to be seen directly by the user, rather than parsed by something like the Studio predictor view.

        Uses `get_model_data` to compose this, but in the future we might want to make this independent if we deprected `get_model_data`

        :returns: Dictionary of the analysis (meant to be foramtted by the APIs and displayed as json/yml/whatever)
        """ # noqa
        model_description = {}
        model_data = self.get_model_data(name, company_id)

        model_description['accuracies'] = model_data['accuracies']
        model_description['column_importances'] = model_data['column_importances']
        model_description['outputs'] = [model_data['predict']]
        model_description['inputs'] = [col for col in model_data['dtype_dict'] if col not in model_description['outputs']]
        model_description['datasource'] = model_data['data_source_name']
        model_description['model'] = ' --> '.join(str(k) for k in model_data['json_ai'])

        return model_description

    def get_models(self, company_id: int):
        models = []
        for db_p in db.session.query(db.Predictor).filter_by(company_id=company_id):
            model_data = self.get_model_data(db_p.name, company_id=company_id)
            reduced_model_data = {}

            for k in ['name', 'version', 'is_active', 'predict', 'status',
                      'current_phase', 'accuracy', 'data_source', 'update',
                      'data_source_name', 'mindsdb_version', 'error']:
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

        db_p = db.session.query(db.Predictor).filter_by(company_id=company_id, name=original_name).first()
        if db_p is None:
            raise Exception(f"Predictor '{name}' does not exist")
        db.session.delete(db_p)
        if db_p.datasource_id is not None:
            try:
                dataset_record = db.Datasource.query.get(db_p.datasource_id)
                if (
                    isinstance(dataset_record.data, str)
                    and json.loads(dataset_record.data).get('source_type') != 'file'
                ):
                    DataStore().delete_datasource(dataset_record.name, company_id)
            except Exception:
                pass
        db.session.commit()

        DatabaseWrapper(company_id).unregister_predictor(name)

        # delete from s3
        self.fs_store.delete(f'predictor_{company_id}_{db_p.id}')

        return 0

    def rename_model(self, old_name, new_name, company_id: int):
        db_p = db.session.query(db.Predictor).filter_by(company_id=company_id, name=old_name).first()
        db_p.name = new_name
        db.session.commit()
        dbw = DatabaseWrapper(company_id)
        dbw.unregister_predictor(old_name)
        dbw.register_predictors([self.get_model_data(new_name, company_id)])

    @mark_process(name='learn')
    def update_model(self, name: str, company_id: int):
        # TODO: Add version check here once we're done debugging
        predictor_record = db.session.query(db.Predictor).filter_by(company_id=company_id, name=name).first()
        assert predictor_record is not None
        predictor_record.update_status = 'updating'
        db.session.commit()

        p = UpdateProcess(name, company_id)
        p.start()
        return 'Updated in progress'

    @mark_process(name='learn')
    def generate_predictor(self, name: str, from_data: dict, datasource_id, problem_definition_dict: dict,
                           join_learn_process: bool, company_id: int):
        predictor_record = db.session.query(db.Predictor).filter_by(company_id=company_id, name=name).first()
        if predictor_record is not None:
            raise Exception('Predictor name must be unique.')

        df, problem_definition, _ = self._unpack_old_args(from_data, problem_definition_dict)

        problem_definition = ProblemDefinition.from_dict(problem_definition)

        predictor_record = db.Predictor(
            company_id=company_id,
            name=name,
            datasource_id=datasource_id,
            mindsdb_version=mindsdb_version,
            lightwood_version=lightwood_version,
            to_predict=problem_definition.target,
            learn_args=problem_definition.to_dict(),
            data={'name': name}
        )

        db.session.add(predictor_record)
        db.session.commit()
        predictor_id = predictor_record.id

        p = GenerateProcess(df, problem_definition, predictor_id)
        p.start()
        if join_learn_process:
            p.join()
            if not IS_PY36:
                p.close()
        db.session.refresh(predictor_record)

    def edit_json_ai(self, name: str, json_ai: dict, company_id=None):
        predictor_record = db.session.query(db.Predictor).filter_by(company_id=company_id, name=name).first()
        assert predictor_record is not None

        json_ai = lightwood.JsonAI.from_dict(json_ai)
        predictor_record.code = lightwood.code_from_json_ai(json_ai)   
        predictor_record.json_ai = json_ai.to_dict()
        db.session.commit()

    def code_from_json_ai(self, json_ai: dict, company_id=None):
        json_ai = lightwood.JsonAI.from_dict(json_ai)
        code = lightwood.code_from_json_ai(json_ai)
        return code

    def edit_code(self, name: str, code: str, company_id=None):
        """Edit an existing predictor's code"""
        if self.config.get('cloud', False):
            raise Exception('Code editing prohibited on cloud')

        predictor_record = db.session.query(db.Predictor).filter_by(company_id=company_id, name=name).first()
        assert predictor_record is not None

        lightwood.predictor_from_code(code)
        predictor_record.code = code
        predictor_record.json_ai = None
        db.session.commit()

    @mark_process(name='learn')
    def fit_predictor(self, name: str, from_data: dict, join_learn_process: bool, company_id: int) -> None:
        predictor_record = db.session.query(db.Predictor).filter_by(company_id=company_id, name=name).first()
        assert predictor_record is not None

        df = self._get_from_data_df(from_data)
        p = FitProcess(predictor_record.id, df)
        p.start()
        if join_learn_process:
            p.join()
            if not IS_PY36:
                p.close()


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
