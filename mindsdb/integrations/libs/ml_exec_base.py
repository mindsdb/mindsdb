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

"""  # noqa

import time
import threading
import datetime as dt
from typing import Optional, Callable
from concurrent.futures import ProcessPoolExecutor, Future

import pandas as pd
from sqlalchemy import func, null
from sqlalchemy.sql.functions import coalesce

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.utilities.config import Config
import mindsdb.interfaces.storage.db as db
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.__about__ import __version__ as mindsdb_version
from mindsdb.utilities.hooks import after_predict as after_predict_hook
from mindsdb.interfaces.model.model_controller import ModelController
from mindsdb.interfaces.model.functions import (
    get_model_record
)
from mindsdb.integrations.libs.const import PREDICTOR_STATUS
from mindsdb.interfaces.database.database import DatabaseController
from mindsdb.interfaces.storage.model_fs import ModelStorage, HandlerStorage
from mindsdb.utilities.context import context as ctx
from mindsdb.interfaces.model.functions import get_model_records
from mindsdb.integrations.handlers_client.ml_client_factory import MLClientFactory
from mindsdb.integrations.libs.learn_process import learn_process
from mindsdb.utilities.functions import mark_process
import mindsdb.utilities.profiler as profiler

from .ml_handler_proc import MLHandlerWrapper, MLHandlerPersistWrapper

import torch.multiprocessing as mp
mp_ctx = mp.get_context('spawn')


class MLEngineException(Exception):
    pass


def init_ml_handler(module_path):
    import importlib  # noqa

    from mindsdb.integrations.libs.learn_process import learn_process  # noqa

    importlib.import_module(module_path)


def dummy_task():
    return None


class WarmProcess:
    """ Class-wrapper for a process that persist for a long time. The process
        may be initialized with any handler requirements. Current implimentation
        is based on ProcessPoolExecutor just because of multiprocessing.pool
        produce daemon processes, which can not be used for learning. That
        bahaviour may be changed only using inheritance.
    """
    def __init__(self, initializer: Optional[Callable] = None, initargs: tuple = ()):
        """ create and init new process

            Args:
                initializer (Callable): the same as ProcessPoolExecutor initializer
                initargs (tuple): the same as ProcessPoolExecutor initargs
        """
        self.pool = ProcessPoolExecutor(1, initializer=initializer, initargs=initargs)
        # region bacause of ProcessPoolExecutor does not start new process
        # untill it get a task, we need manually run dummy task to force init.
        self.task = self.pool.submit(dummy_task)
        self._init_done = False
        self.task.add_done_callback(self._init_done_callback)
        # endregion

    def __del__(self):
        self.pool.shutdown(wait=False)

    def _init_done_callback(self, _task):
        """ callback for initial task
        """
        self._init_done = True

    def ready(self) -> bool:
        """ check is process ready to get a task or not

            Returns:
                bool
        """
        if self._init_done is False:
            self.task.result()
            self._init_done = True
        if self.task is None or self.task.done():
            return True
        return False

    def apply_async(self, func: Callable, *args: tuple, **kwargs: dict) -> Future:
        """ Run new task

            Args:
                func (Callable): function to run
                args (tuple): args to be passed to function
                kwargs (dict): kwargs to be passed to function

            Returns:
                Future
        """
        if not self.ready():
            raise Exception('Process task is not ready')
        self.task = self.pool.submit(
            func, *args, **kwargs
        )
        return self.task


class ProcessCache:
    """ simple cache for WarmProcess-es
    """
    def __init__(self, ttl: int = 120):
        """ Args:
            ttl (int) time to live for unused process
        """
        self.cache = {}
        self._init = False
        self._lock = threading.Lock()
        self._ttl = ttl
        self._keep_alive = {}
        self._stop_event = threading.Event()
        self.cleaner_thread = None
        self._start_clean()

    def __del__(self):
        self._stop_clean()

    def _start_clean(self) -> None:
        """ start worker that close connections after ttl expired
        """
        if (
            isinstance(self.cleaner_thread, threading.Thread)
            and self.cleaner_thread.is_alive()
        ):
            return
        self._stop_event.clear()
        self.cleaner_thread = threading.Thread(target=self._clean)
        self.cleaner_thread.daemon = True
        self.cleaner_thread.start()

    def _stop_clean(self) -> None:
        """ stop clean worker
        """
        self._stop_event.set()

    def init(self, preload_handlers: dict):
        """ run processes for specified handlers

            Args:
                preload_handlers (dict): {handler_class: count_of_processes}
        """
        with self._lock:
            if self._init is False:
                self._init = True
                for handler in preload_handlers:
                    self._keep_alive[handler.__name__] = preload_handlers[handler]
                    self.cache[handler.__name__] = {
                        'last_usage_at': time.time(),
                        'processes': [
                            WarmProcess(init_ml_handler, (handler.__module__,))
                            for _x in range(preload_handlers[handler])
                        ]
                    }

    def apply_async(self, handler: object, func: Callable, *args, **kwargs) -> Future:
        """ run new task. If possible - do it in existing process, if not - start new one.

            Args:
                handler (object): handler class
                func (Callable): function to run
                args (tuple): args to be passed to function
                kwargs (dict): kwargs to be passed to function

            Returns:
                Future
        """
        with self._lock:
            handler_name = handler.__name__
            if handler_name not in self.cache:
                warm_process = WarmProcess(init_ml_handler, (handler.__module__,))
                self.cache[handler_name] = {
                    'last_usage_at': None,
                    'processes': [warm_process]
                }
            else:
                for warm_process in self.cache[handler_name]['processes']:
                    if warm_process.ready():
                        break
                else:
                    warm_process = WarmProcess(init_ml_handler, (handler.__module__,))
                    self.cache[handler_name]['processes'].append(warm_process)
            task = warm_process.apply_async(func, *args, **kwargs)
            self.cache[handler_name]['last_usage_at'] = time.time()
        return task

    def _clean(self) -> None:
        """ worker that stop unused processes
        """
        while self._stop_event.wait(timeout=10) is False:
            with self._lock:
                for handler_name in self.cache.keys():
                    last_usage_at = self.cache[handler_name]['last_usage_at']
                    processes = self.cache[handler_name]['processes']
                    if (
                        (
                            handler_name not in self._keep_alive
                            or self._keep_alive[handler_name] < len(processes)
                        )
                        and last_usage_at is not None
                        and (time.time() - last_usage_at) > self._ttl
                    ):
                        for i, process in enumerate(processes):
                            if process.ready():
                                processes.pop(i)
                                del process
                                break


process_cache = ProcessCache()


class BaseMLEngineExec:

    def __init__(self, name, **kwargs):
        """
        ML handler interface converter
        """  # noqa
        # TODO move this class to model controller

        self.name = name
        self.config = Config()
        self.handler_controller = kwargs.get('handler_controller')
        self.company_id = kwargs.get('company_id')
        self.fs_store = kwargs.get('file_storage')
        self.storage_factory = kwargs.get('storage_factory')
        self.integration_id = kwargs.get('integration_id')
        self.execution_method = kwargs.get('execution_method')
        self.engine = kwargs.get("integration_engine")

        self.model_controller = ModelController()
        self.database_controller = DatabaseController()

        self.parser = parse_sql
        self.dialect = 'mindsdb'

        self.is_connected = True

        self.handler_class = MLClientFactory(handler_class=kwargs['handler_class'], engine=self.engine)

    def _get_ml_handler(self, predictor_id=None):
        # returns instance or wrapper over it

        integration_id = self.integration_id

        class_path = [self.handler_class.__module__, self.handler_class.__name__]

        if self.execution_method == 'subprocess':
            handler = MLHandlerWrapper()

            handler.init_handler(class_path, integration_id, predictor_id, ctx.dump())
            return handler

        elif self.execution_method == 'subprocess_keep':
            handler = MLHandlerPersistWrapper()

            handler.init_handler(class_path, integration_id, predictor_id, ctx.dump())
            return handler

        elif self.execution_method == 'remote':
            raise NotImplementedError()

        else:
            handlerStorage = HandlerStorage(integration_id)
            modelStorage = ModelStorage(predictor_id)

            ml_handler = self.handler_class(
                engine_storage=handlerStorage,
                model_storage=modelStorage,
            )
            return ml_handler

    def get_tables(self) -> Response:
        """ Returns all models currently registered that belong to the ML engine."""
        all_models = self.model_controller.get_models(integration_id=self.integration_id)
        all_models_names = [[x['name']] for x in all_models]
        response = Response(
            RESPONSE_TYPE.TABLE,
            pd.DataFrame(
                all_models_names,
                columns=['table_name']
            )
        )
        return response

    def get_columns(self, table_name: str) -> Response:
        """ Retrieves standard info about a model, e.g. data types. """  # noqa
        predictor_record = get_model_record(name=table_name, ml_handler_name=self.name)
        if predictor_record is None:
            return Response(
                RESPONSE_TYPE.ERROR,
                error_message=f"Error: model '{table_name}' does not exist!"
            )

        data = []
        if predictor_record.dtype_dict is not None:
            for key, value in predictor_record.dtype_dict.items():
                data.append((key, value))
        result = Response(
            RESPONSE_TYPE.TABLE,
            pd.DataFrame(
                data,
                columns=['COLUMN_NAME', 'DATA_TYPE']
            )
        )
        return result

    def native_query(self, query: str) -> Response:
        """ Intakes a raw SQL query and returns the answer given by the ML engine. """
        query_ast = self.parser(query, dialect=self.dialect)
        return self.query(query_ast)

    def query_(self, query: ASTNode) -> Response:
        raise Exception('Should not be used')

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
        # TODO move to model_controller
        """ Trains a model given some data-gathering SQL statement. """

        target = problem_definition['target']

        project = self.database_controller.get_project(name=project_name)

        # handler-side validation
        # self.handler_class is a instance of MLClientFactory
        # so need to check self.handler_class.handler_class attribute
        # which is a class of a real MLHandler
        if hasattr(self.handler_class.handler_class, 'create_validation'):
            self.handler_class.handler_class.create_validation(target, args=problem_definition)

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
        )

        db.serializable_insert(predictor_record)

        class_path = [self.handler_class.__module__, self.handler_class.__name__]

        task = process_cache.apply_async(
            self.handler_class,
            learn_process,
            class_path,
            self.engine,
            ctx.dump(),
            self.integration_id,
            predictor_record.id,
            problem_definition,
            set_active,
            data_integration_ref=data_integration_ref,
            fetch_data_query=fetch_data_query,
            project_name=project_name
        )

        if join_learn_process is True:
            task.result()
            predictor_record = db.Predictor.query.get(predictor_record.id)
            db.session.refresh(predictor_record)

        return predictor_record

    @profiler.profile()
    @mark_process(name='predict')
    def predict(self, model_name: str, data: list, pred_format: str = 'dict',
                project_name: str = None, version=None, params: dict = None):
        """ Generates predictions with some model and input data. """
        if isinstance(data, dict):
            data = [data]
        df = pd.DataFrame(data)
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

        ml_handler = self._get_ml_handler(predictor_record.id)

        args = {
            'pred_format': pred_format,
            'predict_params': {} if params is None else params
        }
        # FIXME
        # if self.handler_class.__name__ == 'LightwoodHandler':
        if self.handler_class.__name__ == 'LightwoodHandler':
            args['code'] = predictor_record.code
            args['target'] = predictor_record.to_predict[0]
            args['dtype_dict'] = predictor_record.dtype_dict
            args['learn_args'] = predictor_record.learn_args

        if self.handler_class.__name__ in ('LangChainHandler',):
            from mindsdb.api.mysql.mysql_proxy.controllers import SessionController
            from mindsdb.api.mysql.mysql_proxy.executor.executor_commands import ExecuteCommands

            sql_session = SessionController()
            sql_session.database = 'mindsdb'

            command_executor = ExecuteCommands(sql_session, executor=None)

            args['executor'] = command_executor

        try:
            predictions = ml_handler.predict(df, args)
        except Exception as e:
            msg = str(e).strip()
            if msg == '':
                msg = e.__class__.__name__
            msg = f'[{self.name}/{model_name}]: {msg}'
            raise MLEngineException(msg) from e

        ml_handler.close()

        # mdb indexes
        if '__mindsdb_row_id' not in predictions.columns and '__mindsdb_row_id' in df.columns:
            predictions['__mindsdb_row_id'] = df['__mindsdb_row_id']

        after_predict_hook(
            company_id=self.company_id,
            predictor_id=predictor_record.id,
            rows_in_count=df.shape[0],
            columns_in_count=df.shape[1],
            rows_out_count=len(predictions)
        )
        return predictions

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
            active=False
        )
        with profiler.Context('finetune-update-record-insert'):
            db.serializable_insert(predictor_record)

        class_path = [self.handler_class.__module__, self.handler_class.__name__]

        with profiler.Context('finetune-update'):
            task = process_cache.apply_async(
                self.handler_class,
                learn_process,
                class_path,
                self.engine,
                ctx.dump(),
                self.integration_id,
                predictor_record.id,
                predictor_record.learn_args,
                set_active,
                base_predictor_record.id,
                data_integration_ref=data_integration_ref,
                fetch_data_query=fetch_data_query,
                project_name=project_name
            )

            if join_learn_process is True:
                task.result()
                predictor_record = db.Predictor.query.get(predictor_record.id)
                db.session.refresh(predictor_record)
            else:
                # return the base predictor record if process is not joined
                predictor_record = db.Predictor.query.get(base_predictor_record.id)

        return predictor_record
