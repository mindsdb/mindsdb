import os
import sys
import time
import json
import base64
import psutil
import datetime
from copy import deepcopy
from contextlib import contextmanager
from dateutil.parser import parse as parse_datetime
from typing import Optional, Tuple, Union, Dict, Any
import requests
from typing import List

import lightwood
from lightwood.api.types import ProblemDefinition
from lightwood import __version__ as lightwood_version
import numpy as np
import pandas as pd
from pandas.core.frame import DataFrame

from mindsdb import __version__ as mindsdb_version
import mindsdb.interfaces.storage.db as db
from mindsdb.utilities.functions import mark_process
from mindsdb.utilities.json_encoder import json_serialiser
from mindsdb.utilities.config import Config
from mindsdb.interfaces.storage.fs import FsStore
from mindsdb.utilities.log import log
from mindsdb.interfaces.model.learn_process import LearnProcess, GenerateProcess, FitProcess, UpdateProcess, LearnRemoteProcess
from mindsdb.utilities.with_kwargs_wrapper import WithKWArgsWrapper
from mindsdb.interfaces.database.integrations import IntegrationController
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
from mindsdb.utilities.hooks import after_predict as after_predict_hook

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

    # def _get_from_data_df(self, from_data: dict) -> DataFrame:
    #     if from_data['class'] == 'QueryDS':
    #         ds = QueryDS(*from_data['args'], **from_data['kwargs'])
    #     else:
    #         ds_cls = getattr(mindsdb_datasources, from_data['class'])
    #         ds = ds_cls(*from_data['args'], **from_data['kwargs'])
    #     return ds.df

    # def _unpack_old_args(
    #     self, from_data: dict, kwargs: dict, to_predict: Optional[Union[str, list]] = None
    # ) -> Tuple[pd.DataFrame, ProblemDefinition, bool]:
    def _unpack_old_args(
        self, kwargs: dict, to_predict: Optional[Union[str, list]] = None
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

        while '.' in str(list(kwargs.keys())):
            for k in list(kwargs.keys()):
                if '.' in k:
                    nks = k.split('.')
                    obj = kwargs
                    for nk in nks[:-1]:
                        if nk not in obj:
                            obj[nk] = {}
                        obj = obj[nk]
                    obj[nks[-1]] = kwargs[k]
                    del kwargs[k]

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

        json_ai_override = {}
        json_ai_keys = list(lightwood.JsonAI.__dict__['__annotations__'].keys())
        for k in kwargs:
            if k in json_ai_keys:
                json_ai_override[k] = kwargs[k]

        if (
            problem_definition.get('ignore_features') is not None and isinstance(problem_definition['ignore_features'], list) is False
        ):
            problem_definition['ignore_features'] = [problem_definition['ignore_features']]

        # if from_data is not None:
        #     df = self._get_from_data_df(from_data)
        # else:
        #     df = None

        # return df, problem_definition, join_learn_process, json_ai_override
        return problem_definition, join_learn_process, json_ai_override

    def _check_model_url(self, url):
        # try to post without data and check status code not in (not_found, method_not_allowed)
        try:
            resp = requests.post(url)
            if resp.status_code in (404, 405):
                raise Exception(f'Model url is incorrect, status_code: {resp.status_code}')
        except requests.RequestException as e:
            raise Exception(f'Model url is incorrect: {str(e)}')

    @mark_process(name='learn')
    def learn(self, name: str, training_data: DataFrame, to_predict: str,
              integration_id: int = None, fetch_data_query: str = None,
              kwargs: dict = {}, company_id: int = None, user_class: int = 0) -> None:
        predictor_record = db.session.query(db.Predictor).filter_by(company_id=company_id, name=name).first()
        if predictor_record is not None:
            raise Exception('Predictor name must be unique.')

        problem_definition, join_learn_process, json_ai_override = self._unpack_old_args(kwargs, to_predict)

        is_cloud = self.config.get('cloud', False)
        if is_cloud is True:
            models = self.get_models(company_id)
            count = 0
            for model in models:
                if model.get('status') in ['generating', 'training']:
                    created_at = model.get('created_at')
                    if isinstance(created_at, str):
                        created_at = parse_datetime(created_at)
                    if isinstance(model.get('created_at'), datetime.datetime) is False:
                        continue
                    if (datetime.datetime.now() - created_at) < datetime.timedelta(hours=1):
                        count += 1
                if count == 2:
                    raise Exception('You can train no more than 2 models at the same time')
            if user_class != 1 and len(training_data) > 10000:
                raise Exception('Datasets are limited to 10,000 rows on free accounts')

        if 'url' in problem_definition:
            train_url = problem_definition['url'].get('train', None)
            if train_url is not None:
                self._check_model_url(train_url)
            predict_url = problem_definition['url'].get('predict', None)
            if predict_url is not None:
                self._check_model_url(predict_url)
            com_format = problem_definition['format']
            api_token = problem_definition['API_TOKEN'] if ('API_TOKEN' in problem_definition) else None
            input_column = problem_definition['input_column'] if ('input_column' in problem_definition) else None
            predictor_record = db.Predictor(
                company_id=company_id,
                name=name,
                # dataset_id=dataset_id,
                mindsdb_version=mindsdb_version,
                lightwood_version=lightwood_version,
                to_predict=problem_definition['target'],
                learn_args=ProblemDefinition.from_dict(problem_definition).to_dict(),
                data={'name': name, 'train_url': train_url, 'predict_url': predict_url, 'format': com_format,
                      'status': 'complete' if train_url is None else 'training', 'API_TOKEN': api_token, 'input_column': input_column},
                is_custom=True,
                # @TODO: For testing purposes, remove afterwards!
                dtype_dict=json_ai_override['dtype_dict'],
            )

            db.session.add(predictor_record)
            db.session.commit()
            if train_url is not None:
                p = LearnRemoteProcess(training_data, predictor_record.id)
                p.start()
                if join_learn_process:
                    p.join()
                    if not IS_PY36:
                        p.close()
                db.session.refresh(predictor_record)
            return

        problem_definition = ProblemDefinition.from_dict(problem_definition)

        predictor_record = db.Predictor(
            company_id=company_id,
            name=name,
            integration_id=integration_id,
            fetch_data_query=fetch_data_query,
            mindsdb_version=mindsdb_version,
            lightwood_version=lightwood_version,
            to_predict=problem_definition.target,
            learn_args=problem_definition.to_dict(),
            data={'name': name},
            training_data_columns_count=len(training_data.columns),
            training_data_rows_count=len(training_data)
        )

        db.session.add(predictor_record)
        db.session.commit()
        predictor_id = predictor_record.id

        p = LearnProcess(training_data, problem_definition, predictor_id, json_ai_override)
        p.start()
        if join_learn_process:
            p.join()
            if not IS_PY36:
                p.close()
        db.session.refresh(predictor_record)

    @mark_process(name='predict')
    def predict(self, name: str, when_data: Union[dict, list, pd.DataFrame], pred_format: str, company_id: int):
        original_name = name
        name = f'{company_id}@@@@@{name}'

        predictor_record = db.session.query(db.Predictor).filter_by(company_id=company_id, name=original_name).first()
        assert predictor_record is not None
        predictor_data = self.get_model_data(name, company_id)

        if isinstance(when_data, dict):
            when_data = [when_data]
        df = pd.DataFrame(when_data)

        if predictor_record.is_custom:
            if predictor_data['format'] == 'mlflow':
                resp = requests.post(predictor_data['predict_url'],
                                     data=df.to_json(orient='records'),
                                     headers={'content-type': 'application/json; format=pandas-records'})
                answer: List[object] = resp.json()

                predictions = pd.DataFrame({
                    'prediction': answer
                })

            elif predictor_data['format'] == 'huggingface':
                headers = {"Authorization": "Bearer {API_TOKEN}".format(API_TOKEN=predictor_data['API_TOKEN'])}
                col_data = df[predictor_data['input_column']]
                col_data.rename({predictor_data['input_column']: 'inputs'}, axis='columns')
                serialized_df = json.dumps(col_data.to_dict())
                resp = requests.post(predictor_data['predict_url'], headers=headers, data=serialized_df)
                predictions = pd.DataFrame(resp.json())

            elif predictor_data['format'] == 'ray_server':
                serialized_df = json.dumps(df.to_dict())
                resp = requests.post(predictor_data['predict_url'], json={'df': serialized_df})
                predictions = pd.DataFrame(resp.json())

        else:
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
            predictions = self.predictor_cache[name]['predictor'].predict(df)
            # Bellow is useful for debugging caching and storage issues
            # del self.predictor_cache[name]

        predictions = predictions.to_dict(orient='records')
        after_predict_hook(
            company_id=company_id,
            predictor_id=predictor_record.id,
            rows_in_count=df.shape[0],
            columns_in_count=df.shape[1],
            rows_out_count=len(predictions)
        )
        target = predictor_record.to_predict[0]
        if pred_format in ('explain', 'dict', 'dict&explain'):
            explain_arr = []
            dict_arr = []
            for i, row in enumerate(predictions):
                obj = {
                    target: {
                        'predicted_value': row['prediction'],
                        'confidence': row.get('confidence', None),
                        'anomaly': row.get('anomaly', None),
                        'truth': row.get('truth', None)
                    }
                }
                if 'lower' in row:
                    obj[target]['confidence_lower_bound'] = row.get('lower', None)
                    obj[target]['confidence_upper_bound'] = row.get('upper', None)

                explain_arr.append(obj)

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
    def analyse_dataset(self, df: DataFrame, company_id: int) -> lightwood.DataAnalysis:
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
        if predictor_record is None:
            raise Exception(f"Model does not exists: {original_name}")

        # linked_dataset = db.session.query(db.Dataset).get(predictor_record.dataset_id)

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
        # data['data_source_name'] = linked_dataset.name if linked_dataset else None !!!!!
        data['problem_definition'] = predictor_record.learn_args

        # assume older models are complete, only temporary
        if 'status' in predictor_record.data:
            data['status'] = predictor_record.data['status']
        elif 'error' in predictor_record.data:
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

        Uses `get_model_data` to compose this, but in the future we might want to make this independent if we deprecated `get_model_data`

        :returns: Dictionary of the analysis (meant to be foramtted by the APIs and displayed as json/yml/whatever)
        """ # noqa
        model_description = {}
        model_data = self.get_model_data(name, company_id)

        model_description['accuracies'] = model_data['accuracies']
        model_description['column_importances'] = model_data['column_importances']
        model_description['outputs'] = [model_data['predict']]
        model_description['inputs'] = [col for col in model_data['dtype_dict'] if col not in model_description['outputs']]
        model_description['datasource'] = model_data.get('data_source_name')
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
            raise Exception(f"Predictor '{original_name}' does not exist")

        is_cloud = self.config.get('cloud', False)
        model = self.get_model_data(db_p.name, company_id=company_id)
        if (
            is_cloud is True
            and model.get('status') in ['generating', 'training']
            and isinstance(model.get('created_at'), str) is True
            and (datetime.datetime.now() - parse_datetime(model.get('created_at'))) < datetime.timedelta(hours=1)
        ):
            raise Exception('You are unable to delete models currently in progress, please wait before trying again')

        db.session.delete(db_p)
        db.session.commit()

        # delete from s3
        self.fs_store.delete(f'predictor_{company_id}_{db_p.id}')

        return 0

    def rename_model(self, old_name, new_name, company_id: int):
        db_p = db.session.query(db.Predictor).filter_by(company_id=company_id, name=old_name).first()
        db_p.name = new_name
        db.session.commit()

    @mark_process(name='learn')
    def update_model(self, name: str, company_id: int):
        # TODO: Add version check here once we're done debugging
        predictor_record = db.session.query(db.Predictor).filter_by(company_id=company_id, name=name).first()
        assert predictor_record is not None
        predictor_record.update_status = 'updating'
        db.session.commit()

        integration_controller = WithKWArgsWrapper(IntegrationController(), company_id=company_id)
        integration_record = db.Integration.query.get(predictor_record.integration_id)
        if integration_record is None:
            raise Exception(f"There is no integration for predictor '{name}'")
        integration_handler = integration_controller.get_handler(integration_record.name)

        response = integration_handler.native_query(predictor_record.fetch_data_query)
        if response.type != RESPONSE_TYPE.TABLE:
            raise Exception(f"Can't fetch data for predictor training: {response.error_message}")

        p = UpdateProcess(name, response.data_frame, company_id)
        p.start()
        return 'Updated in progress'

    @mark_process(name='learn')
    def generate_predictor(self, name: str, from_data: dict, dataset_id, problem_definition_dict: dict,
                           join_learn_process: bool, company_id: int):
        predictor_record = db.session.query(db.Predictor).filter_by(company_id=company_id, name=name).first()
        if predictor_record is not None:
            raise Exception('Predictor name must be unique.')

        df, problem_definition, _, _ = self._unpack_old_args(from_data, problem_definition_dict)

        problem_definition = ProblemDefinition.from_dict(problem_definition)

        predictor_record = db.Predictor(
            company_id=company_id,
            name=name,
            # dataset_id=dataset_id,
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
        # TODO
        pass
        # predictor_record = db.session.query(db.Predictor).filter_by(company_id=company_id, name=name).first()
        # assert predictor_record is not None

        # df = self._get_from_data_df(from_data)
        # p = FitProcess(predictor_record.id, df)
        # p.start()
        # if join_learn_process:
        #     p.join()
        #     if not IS_PY36:
        #         p.close()

    def export_predictor(self, name: str, company_id: int) -> json:
        predictor_record = db.session.query(db.Predictor).filter_by(company_id=company_id, name=name).first()
        assert predictor_record is not None

        fs_name = f'predictor_{company_id}_{predictor_record.id}'
        self.fs_store.get(fs_name, fs_name, self.config['paths']['predictors'])
        local_predictor_savefile = os.path.join(self.config['paths']['predictors'], fs_name)
        predictor_binary = open(local_predictor_savefile, 'rb').read()

        # Serialize a predictor record into a dictionary 
        # move into the Predictor db class itself if we use it again somewhere
        predictor_record_serialized = {
            'name': predictor_record.name,
            'data': predictor_record.data,
            'to_predict': predictor_record.to_predict,
            'mindsdb_version': predictor_record.mindsdb_version,
            'native_version': predictor_record.native_version,
            'is_custom': predictor_record.is_custom,
            'learn_args': predictor_record.learn_args,
            'update_status': predictor_record.update_status,
            'json_ai': predictor_record.json_ai,
            'code': predictor_record.code,
            'lightwood_version': predictor_record.lightwood_version,
            'dtype_dict': predictor_record.dtype_dict,
            'predictor_binary': predictor_binary
        }

        return json.dumps(predictor_record_serialized, default=json_serialiser)

    def import_predictor(self, name: str, payload: json, company_id: int) -> None:
        prs = json.loads(payload)

        predictor_record = db.Predictor(
            name=name,
            data=prs['data'],
            to_predict=prs['to_predict'],
            company_id=company_id,
            mindsdb_version=prs['mindsdb_version'],
            native_version=prs['native_version'],
            is_custom=prs['is_custom'],
            learn_args=prs['learn_args'],
            update_status=prs['update_status'],
            json_ai=prs['json_ai'],
            code=prs['code'],
            lightwood_version=prs['lightwood_version'],
            dtype_dict=prs['dtype_dict']
        )

        db.session.add(predictor_record)
        db.session.commit()

        predictor_binary = base64.b64decode(prs['predictor_binary'])
        fs_name = f'predictor_{company_id}_{predictor_record.id}'
        with open(os.path.join(self.config['paths']['predictors'], fs_name), 'wb') as fp:
            fp.write(predictor_binary)

        self.fs_store.put(fs_name, fs_name, self.config['paths']['predictors'])

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
