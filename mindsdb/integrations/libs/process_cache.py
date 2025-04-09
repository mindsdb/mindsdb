import time
import threading
from typing import Optional, Callable
from concurrent.futures import ProcessPoolExecutor, Future

from pandas import DataFrame

import mindsdb.interfaces.storage.db as db
from mindsdb.utilities.config import Config
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.ml_task_queue.const import ML_TASK_TYPE
from mindsdb.integrations.libs.ml_handler_process import (
    learn_process,
    update_process,
    predict_process,
    describe_process,
    create_engine_process,
    update_engine_process,
    create_validation_process,
    func_call_process
)


def init_ml_handler(module_path):
    import importlib  # noqa

    import mindsdb.integrations.libs.ml_handler_process  # noqa

    db.init()
    importlib.import_module(module_path)


def dummy_task():
    return None


def empty_callback(_task):
    return None


class MLProcessException(Exception):
    """Wrapper for exception to safely send it back to the main process.

    If exception can not be pickled (pickle.loads(pickle.dumps(e))) then it may lead to termination of the ML process.
    Also in this case, the error sent to the user will not be relevant. This wrapper should prevent it.
    """
    base_exception_bytes: bytes = None

    def __init__(self, base_exception: Exception, message: str = None) -> None:
        super().__init__(message)
        self.message = f'{base_exception.__class__.__name__}: {base_exception}'

    @property
    def base_exception(self) -> Exception:
        return RuntimeError(self.message)


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
        self.last_usage_at = time.time()
        self._markers = set()
        # region bacause of ProcessPoolExecutor does not start new process
        # untill it get a task, we need manually run dummy task to force init.
        self.task = self.pool.submit(dummy_task)
        self._init_done = False
        self.task.add_done_callback(self._init_done_callback)
        # endregion

    def __del__(self):
        self.shutdown()

    def shutdown(self, wait: bool = False) -> None:
        """Like ProcessPoolExecutor.shutdown

        Args:
            wait (bool): If True then shutdown will not return until all running futures have finished executing
        """
        self.pool.shutdown(wait=wait)

    def _init_done_callback(self, _task):
        """ callback for initial task
        """
        self._init_done = True

    def _update_last_usage_at_callback(self, _task):
        self.last_usage_at = time.time()

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

    def add_marker(self, marker: tuple):
        """ remember that that process processed task for that model

            Args:
                marker (tuple): identifier of model
        """
        if marker is not None:
            self._markers.add(marker)

    def has_marker(self, marker: tuple) -> bool:
        """ check if that process processed task for model

            Args:
                marker (tuple): identifier of model

            Returns:
                bool
        """
        if marker is None:
            return False
        return marker in self._markers

    def is_marked(self) -> bool:
        """ check if process has any marker

            Returns:
                bool
        """
        return len(self._markers) > 0

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
        self.task.add_done_callback(self._update_last_usage_at_callback)
        self.last_usage_at = time.time()
        return self.task


def warm_function(func, context: str, *args, **kwargs):
    ctx.load(context)
    try:
        return func(*args, **kwargs)
    except Exception as e:
        if type(e) in (ImportError, ModuleNotFoundError):
            raise
        raise MLProcessException(base_exception=e)


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
        self.cleaner_thread = threading.Thread(target=self._clean, name='ProcessCache.clean')
        self.cleaner_thread.daemon = True
        self.cleaner_thread.start()

    def _stop_clean(self) -> None:
        """ stop clean worker
        """
        self._stop_event.set()

    def init(self):
        """ run processes for specified handlers
        """
        from mindsdb.interfaces.database.integrations import integration_controller
        preload_handlers = {}
        config = Config()
        is_cloud = config.get('cloud', False) # noqa

        if config['ml_task_queue']['type'] != 'redis':
            if is_cloud:
                lightwood_handler = integration_controller.get_handler_module('lightwood')
                if lightwood_handler is not None and lightwood_handler.Handler is not None:
                    preload_handlers[lightwood_handler.Handler] = 4 if is_cloud else 1

                huggingface_handler = integration_controller.get_handler_module('huggingface')
                if huggingface_handler is not None and huggingface_handler.Handler is not None:
                    preload_handlers[huggingface_handler.Handler] = 1

                openai_handler = integration_controller.get_handler_module('openai')
                if openai_handler is not None and openai_handler.Handler is not None:
                    preload_handlers[openai_handler.Handler] = 1

        with self._lock:
            if self._init is False:
                self._init = True
                for handler in preload_handlers:
                    self._keep_alive[handler.name] = preload_handlers[handler]
                    self.cache[handler.name] = {
                        'last_usage_at': time.time(),
                        'handler_module': handler.__module__,
                        'processes': [
                            WarmProcess(init_ml_handler, (handler.__module__,))
                            for _x in range(preload_handlers[handler])
                        ]
                    }

    def apply_async(self, task_type: ML_TASK_TYPE, model_id: Optional[int],
                    payload: dict, dataframe: Optional[DataFrame] = None) -> Future:
        """ run new task. If possible - do it in existing process, if not - start new one.

            Args:
                task_type (ML_TASK_TYPE): type of the task (learn, predict, etc)
                model_id (int): id of the model
                payload (dict): any 'lightweight' data that needs to be send in the process
                dataframe (DataFrame): DataFrame to be send in the process

            Returns:
                Future
        """
        self._start_clean()
        handler_module_path = payload['handler_meta']['module_path']
        integration_id = payload['handler_meta']['integration_id']
        if task_type in (ML_TASK_TYPE.LEARN, ML_TASK_TYPE.FINETUNE):
            func = learn_process
            kwargs = {
                'data_integration_ref': payload['data_integration_ref'],
                'problem_definition': payload['problem_definition'],
                'fetch_data_query': payload['fetch_data_query'],
                'project_name': payload['project_name'],
                'model_id': model_id,
                'base_model_id': payload.get('base_model_id'),
                'set_active': payload['set_active'],
                'integration_id': integration_id,
                'module_path': handler_module_path
            }
        elif task_type == ML_TASK_TYPE.PREDICT:
            func = predict_process
            kwargs = {
                'predictor_record': payload['predictor_record'],
                'ml_engine_name': payload['handler_meta']['engine'],
                'args': payload['args'],
                'dataframe': dataframe,
                'integration_id': integration_id,
                'module_path': handler_module_path
            }
        elif task_type == ML_TASK_TYPE.DESCRIBE:
            func = describe_process
            kwargs = {
                'attribute': payload.get('attribute'),
                'model_id': model_id,
                'integration_id': integration_id,
                'module_path': handler_module_path
            }
        elif task_type == ML_TASK_TYPE.CREATE_VALIDATION:
            func = create_validation_process
            kwargs = {
                'target': payload.get('target'),
                'args': payload.get('args'),
                'integration_id': integration_id,
                'module_path': handler_module_path
            }
        elif task_type == ML_TASK_TYPE.CREATE_ENGINE:
            func = create_engine_process
            kwargs = {
                'connection_args': payload['connection_args'],
                'integration_id': integration_id,
                'module_path': handler_module_path
            }
        elif task_type == ML_TASK_TYPE.UPDATE_ENGINE:
            func = update_engine_process
            kwargs = {
                'connection_args': payload['connection_args'],
                'integration_id': integration_id,
                'module_path': handler_module_path
            }
        elif task_type == ML_TASK_TYPE.UPDATE:
            func = update_process
            kwargs = {
                'args': payload['args'],
                'integration_id': integration_id,
                'model_id': model_id,
                'module_path': handler_module_path
            }
        elif task_type == ML_TASK_TYPE.FUNC_CALL:
            func = func_call_process
            kwargs = {
                'name': payload['name'],
                'args': payload['args'],
                'integration_id': integration_id,
                'module_path': handler_module_path
            }
        else:
            raise Exception(f'Unknown ML task type: {task_type}')

        ml_engine_name = payload['handler_meta']['engine']
        model_marker = (model_id, payload['context']['company_id'])
        with self._lock:
            if ml_engine_name not in self.cache:
                warm_process = WarmProcess(init_ml_handler, (handler_module_path,))
                self.cache[ml_engine_name] = {
                    'last_usage_at': None,
                    'handler_module': handler_module_path,
                    'processes': [warm_process]
                }
            else:
                warm_process = None
                if model_marker is not None:
                    try:
                        warm_process = next(
                            p for p in self.cache[ml_engine_name]['processes']
                            if p.ready() and p.has_marker(model_marker)
                        )
                    except StopIteration:
                        pass
                if warm_process is None:
                    try:
                        warm_process = next(
                            p for p in self.cache[ml_engine_name]['processes']
                            if p.ready()
                        )
                    except StopIteration:
                        pass
                if warm_process is None:
                    warm_process = WarmProcess(init_ml_handler, (handler_module_path,))
                    self.cache[ml_engine_name]['processes'].append(warm_process)

            task = warm_process.apply_async(warm_function, func, payload['context'], **kwargs)
            self.cache[ml_engine_name]['last_usage_at'] = time.time()
            warm_process.add_marker(model_marker)
        return task

    def _clean(self) -> None:
        """ worker that stop unused processes
        """
        while self._stop_event.wait(timeout=10) is False:
            with self._lock:
                for handler_name in self.cache.keys():
                    processes = self.cache[handler_name]['processes']
                    processes.sort(key=lambda x: x.is_marked())

                    expected_count = 0
                    if handler_name in self._keep_alive:
                        expected_count = self._keep_alive[handler_name]

                    # stop processes which was used, it needs to free memory
                    for i, process in enumerate(processes):
                        if (
                            process.ready()
                            and process.is_marked()
                            and (time.time() - process.last_usage_at) > self._ttl
                        ):
                            processes.pop(i)
                            # del process
                            process.shutdown()
                            break

                    while expected_count > len(processes):
                        processes.append(
                            WarmProcess(init_ml_handler, (self.cache[handler_name]['handler_module'],))
                        )

    def shutdown(self, wait: bool = True) -> None:
        """Call 'shutdown' for each process cache

        wait (bool): like ProcessPoolExecutor.shutdown wait arg.
        """
        with self._lock:
            for handler_name in self.cache:
                for process in self.cache[handler_name]['processes']:
                    process.shutdown(wait=wait)
                self.cache[handler_name]['processes'] = []

    def remove_processes_for_handler(self, handler_name: str) -> None:
        """
            Remove all warm processes for a given handler.
            This is useful when the previous processes use an outdated instance of the handler.
            A good example is when the dependencies for a handler are installed after attempting to use the handler.

            Args:
                handler_name (str): name of the handler.
        """
        with self._lock:
            if handler_name in self.cache:
                for process in self.cache[handler_name]['processes']:
                    process.shutdown()

                self.cache[handler_name]['processes'] = []


process_cache = ProcessCache()
