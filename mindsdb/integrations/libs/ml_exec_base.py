"""
This module defines the wrapper for ML engines which abstracts away a lot of complexity.

In particular, three big components are included:

    - `BaseMLEngineExec` class: this class wraps any object that inherits from `BaseMLEngine` and exposes some endpoints
      normally associated with a DB handler (e.g. `native_query`, `get_tables`), as well as other ML-specific behaviors,
      like `learn()` or `predict()`. Note that while these still have to be implemented at the engine level, the burden
      on that class is lesser given that it only needs to return a pandas DataFrame. It's this class that will take said
      output and format it into the HandlerResponse instance that MindsDB core expects.

    - `learn_process` method: handles async dispatch of the `learn` method in an engine, as well as registering all
      models inside of the internal MindsDB registry.

    - `predict_process` method: handles async dispatch of the `predict` method in an engine.

"""

import socket
import contextlib
import datetime as dt
from types import ModuleType
from typing import Optional, Union

import pandas as pd
from sqlalchemy import func, null
from sqlalchemy.sql.functions import coalesce

from mindsdb.utilities.config import Config
import mindsdb.interfaces.storage.db as db
from mindsdb.__about__ import __version__ as mindsdb_version
from mindsdb.utilities.hooks import after_predict as after_predict_hook
from mindsdb.interfaces.model.functions import (
    get_model_record
)
from mindsdb.integrations.libs.const import PREDICTOR_STATUS
from mindsdb.interfaces.database.database import DatabaseController
from mindsdb.utilities.context import context as ctx
from mindsdb.interfaces.model.functions import get_model_records
from mindsdb.utilities.functions import mark_process
import mindsdb.utilities.profiler as profiler
from mindsdb.utilities.ml_task_queue.producer import MLTaskProducer
from mindsdb.utilities.ml_task_queue.const import ML_TASK_TYPE
from mindsdb.integrations.libs.process_cache import process_cache, empty_callback, MLProcessException

try:
    import torch.multiprocessing as mp
except Exception:
    import multiprocessing as mp
mp_ctx = mp.get_context('spawn')


class MLEngineException(Exception):
    pass


class BaseMLEngineExec:

    def __init__(self, name: str, integration_id: int, handler_module: ModuleType):
        """ML handler interface

        Args:
            name (str): name of the ml_engine
            integration_id (int): id of the ml_engine
            handler_module (ModuleType): module of the ml_engine
        """
        self.name = name
        self.config = Config()
        self.integration_id = integration_id
        self.engine = handler_module.name
        self.handler_module = handler_module

        self.database_controller = DatabaseController()

        self.base_ml_executor = process_cache
        if self.config['ml_task_queue']['type'] == 'redis':
            self.base_ml_executor = MLTaskProducer()

    @profiler.profile()
    def learn(
        self, model_name, project_name,
        data_integration_ref=None,
        fetch_data_query=None,
        problem_definition=None,
        join_learn_process=False,
        label=None,
        is_retrain=False,
        set_active=True,
    ):
        """ Trains a model given some data-gathering SQL statement. """

        # may or may not be provided (e.g. 0-shot models do not need it), so engine will handle it
        target = problem_definition.get('target', [''])  # db.Predictor expects Column(Array(String))

        project = self.database_controller.get_project(name=project_name)

        self.create_validation(target, problem_definition, self.integration_id)

        predictor_record = db.Predictor(
            company_id=ctx.company_id,
            name=model_name,
            integration_id=self.integration_id,
            data_integration_ref=data_integration_ref,
            fetch_data_query=fetch_data_query,
            mindsdb_version=mindsdb_version,
            to_predict=target,
            learn_args=problem_definition,
            data={'name': model_name},
            project_id=project.id,
            training_data_columns_count=None,
            training_data_rows_count=None,
            training_start_at=dt.datetime.now(),
            status=PREDICTOR_STATUS.GENERATING,
            label=label,
            version=(
                db.session.query(
                    coalesce(func.max(db.Predictor.version), 1) + (1 if is_retrain else 0)
                ).filter_by(
                    company_id=ctx.company_id,
                    name=model_name,
                    project_id=project.id,
                    deleted_at=null()
                ).scalar_subquery()),
            active=(not is_retrain),  # if create then active
            training_metadata={
                'hostname': socket.gethostname(),
                'reason': 'retrain' if is_retrain else 'learn'
            }
        )

        db.serializable_insert(predictor_record)

        with self._catch_exception(model_name):
            task = self.base_ml_executor.apply_async(
                task_type=ML_TASK_TYPE.LEARN,
                model_id=predictor_record.id,
                payload={
                    'handler_meta': {
                        'module_path': self.handler_module.__package__,
                        'engine': self.engine,
                        'integration_id': self.integration_id
                    },
                    'context': ctx.dump(),
                    'problem_definition': problem_definition,
                    'set_active': set_active,
                    'data_integration_ref': data_integration_ref,
                    'fetch_data_query': fetch_data_query,
                    'project_name': project_name
                }
            )

            if join_learn_process is True:
                task.result()
                predictor_record = db.Predictor.query.get(predictor_record.id)
                db.session.refresh(predictor_record)
            else:
                # to prevent memory leak need to add any callback
                task.add_done_callback(empty_callback)

        return predictor_record

    def describe(self, model_id: int, attribute: Optional[str] = None) -> pd.DataFrame:
        with self._catch_exception(model_id):
            task = self.base_ml_executor.apply_async(
                task_type=ML_TASK_TYPE.DESCRIBE,
                model_id=model_id,
                payload={
                    'handler_meta': {
                        'module_path': self.handler_module.__package__,
                        'engine': self.engine,
                        'integration_id': self.integration_id
                    },
                    'attribute': attribute,
                    'context': ctx.dump()
                }
            )
            result = task.result()
        return result

    def function_call(self, func_name, args):
        with self._catch_exception():
            task = self.base_ml_executor.apply_async(
                task_type=ML_TASK_TYPE.FUNC_CALL,
                model_id=0,     # can not be None
                payload={
                    'context': ctx.dump(),
                    'name': func_name,
                    'args': args,
                    'handler_meta': {
                        'module_path': self.handler_module.__package__,
                        'engine': self.engine,
                        'integration_id': self.integration_id
                    },
                }
            )
            result = task.result()
        return result

    @profiler.profile()
    @mark_process(name='predict')
    def predict(self, model_name: str, df: pd.DataFrame, pred_format: str = 'dict',
                project_name: str = None, version=None, params: dict = None):
        """ Generates predictions with some model and input data. """

        kwargs = {
            'name': model_name,
            'ml_handler_name': self.name,
            'project_name': project_name
        }
        if version is None:
            kwargs['active'] = True
        else:
            kwargs['active'] = None
            kwargs['version'] = version
        predictor_record = get_model_record(**kwargs)
        if predictor_record is None:
            if version is not None:
                model_name = f'{model_name}.{version}'
            raise Exception(f"Error: model '{model_name}' does not exists!")
        if predictor_record.status != PREDICTOR_STATUS.COMPLETE:
            raise Exception("Error: model creation not completed")

        using = {} if params is None else params
        args = {
            'pred_format': pred_format,
            'predict_params': using,
            'using': using
        }

        with self._catch_exception(model_name):
            task = self.base_ml_executor.apply_async(
                task_type=ML_TASK_TYPE.PREDICT,
                model_id=predictor_record.id,
                payload={
                    'handler_meta': {
                        'module_path': self.handler_module.__package__,
                        'engine': self.engine,
                        'integration_id': self.integration_id
                    },
                    'context': ctx.dump(),
                    'predictor_record': predictor_record,
                    'args': args
                },
                dataframe=df
            )
            predictions = task.result()

        # mdb indexes
        if '__mindsdb_row_id' not in predictions.columns and '__mindsdb_row_id' in df.columns:
            predictions['__mindsdb_row_id'] = df['__mindsdb_row_id']

        after_predict_hook(
            company_id=ctx.company_id,
            predictor_id=predictor_record.id,
            rows_in_count=df.shape[0],
            columns_in_count=df.shape[1],
            rows_out_count=len(predictions)
        )
        return predictions

    def create_validation(self, target, args, integration_id):
        with self._catch_exception():
            task = self.base_ml_executor.apply_async(
                task_type=ML_TASK_TYPE.CREATE_VALIDATION,
                model_id=0,     # can not be None
                payload={
                    'context': ctx.dump(),
                    'target': target,
                    'args': args,
                    'handler_meta': {
                        'module_path': self.handler_module.__package__,
                        'engine': self.engine,
                        'integration_id': integration_id
                    },
                }
            )
            result = task.result()
        return result

    def update(self, args: dict, model_id: int):
        with self._catch_exception(model_id):
            task = self.base_ml_executor.apply_async(
                task_type=ML_TASK_TYPE.UPDATE,
                model_id=model_id,
                payload={
                    'context': ctx.dump(),
                    'args': args,
                    'handler_meta': {
                        'module_path': self.handler_module.__package__,
                        'engine': self.engine,
                        'integration_id': self.integration_id
                    },
                }
            )
            result = task.result()
        return result

    def update_engine(self, connection_args):
        with self._catch_exception():
            task = self.base_ml_executor.apply_async(
                task_type=ML_TASK_TYPE.UPDATE_ENGINE,
                model_id=0,     # can not be None
                payload={
                    'context': ctx.dump(),
                    'connection_args': connection_args,
                    'handler_meta': {
                        'module_path': self.handler_module.__package__,
                        'engine': self.engine,
                        'integration_id': self.integration_id
                    },
                }
            )
            result = task.result()
        return result

    def create_engine(self, connection_args: dict, integration_id: int) -> None:
        with self._catch_exception():
            task = self.base_ml_executor.apply_async(
                task_type=ML_TASK_TYPE.CREATE_ENGINE,
                model_id=0,     # can not be None
                payload={
                    'context': ctx.dump(),
                    'connection_args': connection_args,
                    'handler_meta': {
                        'module_path': self.handler_module.__package__,
                        'engine': self.engine,
                        'integration_id': integration_id
                    },
                }
            )
            result = task.result()
        return result

    @profiler.profile()
    def finetune(
            self, model_name, project_name,
            base_model_version: int,
            data_integration_ref=None,
            fetch_data_query=None,
            join_learn_process=False,
            label=None,
            set_active=True,
            args: Optional[dict] = None
    ):
        # generate new record from latest version as starting point
        project = self.database_controller.get_project(name=project_name)

        search_args = {
            'active': None,
            'name': model_name,
            'status': PREDICTOR_STATUS.COMPLETE
        }
        if base_model_version is not None:
            search_args['version'] = base_model_version
        else:
            search_args['active'] = True
        predictor_records = get_model_records(**search_args)
        if len(predictor_records) == 0:
            raise Exception("Can't find suitable base model")

        predictor_records.sort(key=lambda x: x.training_stop_at, reverse=True)
        predictor_records = [x for x in predictor_records if x.training_stop_at is not None]
        base_predictor_record = predictor_records[0]

        learn_args = base_predictor_record.learn_args
        learn_args['using'] = args if not learn_args.get('using', False) else {**learn_args['using'], **args}

        self.create_validation(
            target=base_predictor_record.to_predict,
            args=learn_args,
            integration_id=self.integration_id
        )

        predictor_record = db.Predictor(
            company_id=ctx.company_id,
            name=model_name,
            integration_id=self.integration_id,
            data_integration_ref=data_integration_ref,
            fetch_data_query=fetch_data_query,
            mindsdb_version=mindsdb_version,
            to_predict=base_predictor_record.to_predict,
            learn_args=learn_args,
            data={'name': model_name},
            project_id=project.id,
            training_data_columns_count=None,
            training_data_rows_count=None,
            training_start_at=dt.datetime.now(),
            status=PREDICTOR_STATUS.GENERATING,
            label=label,
            version=(
                db.session.query(
                    coalesce(func.max(db.Predictor.version), 1) + 1
                ).filter_by(
                    company_id=ctx.company_id,
                    name=model_name,
                    project_id=project.id,
                    deleted_at=null()
                ).scalar_subquery()
            ),
            active=False,
            training_metadata={
                'hostname': socket.gethostname(),
                'reason': 'finetune'
            }
        )
        db.serializable_insert(predictor_record)

        with self._catch_exception(model_name):
            task = self.base_ml_executor.apply_async(
                task_type=ML_TASK_TYPE.FINETUNE,
                model_id=predictor_record.id,
                payload={
                    'handler_meta': {
                        'module_path': self.handler_module.__package__,
                        'engine': self.engine,
                        'integration_id': self.integration_id
                    },
                    'context': ctx.dump(),
                    'model_id': predictor_record.id,
                    'problem_definition': predictor_record.learn_args,
                    'set_active': set_active,
                    'base_model_id': base_predictor_record.id,
                    'data_integration_ref': data_integration_ref,
                    'fetch_data_query': fetch_data_query,
                    'project_name': project_name
                }
            )

            if join_learn_process is True:
                task.result()
                predictor_record = db.Predictor.query.get(predictor_record.id)
                db.session.refresh(predictor_record)
            else:
                # to prevent memory leak need to add any callback
                task.add_done_callback(empty_callback)

        return predictor_record

    @contextlib.contextmanager
    def _catch_exception(self, model_identifier: Optional[Union[int, str]] = None):
        try:
            yield
        except (ImportError, ModuleNotFoundError):
            raise
        except Exception as e:
            if type(e) is MLProcessException:
                e = e.base_exception
            msg = str(e).strip()
            if msg == '':
                msg = e.__class__.__name__
            model_identifier = '' if model_identifier is None else f'/{model_identifier}'
            msg = f'[{self.name}{model_identifier}]: {msg}'
            raise MLEngineException(msg) from e
