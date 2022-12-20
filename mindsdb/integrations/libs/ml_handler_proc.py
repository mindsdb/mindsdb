"""
Utility functions for run ml handler in subprocess.
Interation is performed using stdin, stdout.

Flow
    1. subprocess starts and initialize ml hanlder instance inside itself
    2. waits incoming command from stdin
    3. passes this command to ml handler instance
    4. sends response from ml handler to stdout

Protocol over stdin/out:

- [8 bytes] length of the command
- [length] command: is picked dict with keys: method, args, kwargs

if 'error' in response dict, parent process trows exception

"""

import pickle
import subprocess
import sys
import struct
import traceback
import importlib
import threading
import queue
from threading import Lock


# =================  child process ====================

class MLHandlerProcess:

    def __init__(self):
        self.ml_instance = None

    def mainloop(self):
        # replace print output to stderr
        sys.stdout = sys.stderr

        self.stdin = open(0, 'rb')
        self.stdout = open(1, 'wb')

        while True:
            params = self.read_input()
            try:
                method = params['method']
                args = params.get('args', [])
                kwargs = params.get('kwargs', {})

                if hasattr(self, method):
                    # it is control method
                    ret = getattr(self, method)(*args, **kwargs)
                else:
                    # it is handler method
                    ret = getattr(self.ml_instance, method)(*args, **kwargs)

            except SystemExit:
                raise
            except Exception as e:
                error = traceback.format_exc()
                self.send_output({'error': str(e), 'trace': error})
                continue

            self.send_output(ret)

    def send_output(self, obj):
        # read stdin
        encoded = pickle.dumps(obj)

        length_enc = struct.pack('L', len(encoded))

        self.stdout.write(length_enc)
        self.stdout.write(encoded)
        self.stdout.flush()

    def read_input(self):
        # write to stdout
        length_enc = self.stdin.read(8)
        length = struct.unpack('L', length_enc)[0]

        encoded = self.stdin.read(length)
        obj = pickle.loads(encoded)

        return obj

    def init_handler(self, class_path, integration_id, predictor_id, context_dump):
        # mdb initialization
        import mindsdb.interfaces.storage.db as db
        from mindsdb.utilities.context import context as ctx
        ctx.load(context_dump)
        db.init()

        from mindsdb.interfaces.storage.model_fs import ModelStorage, HandlerStorage

        module_name, class_name = class_path
        module = importlib.import_module(module_name)
        HandlerClass = getattr(module, class_name)

        handlerStorage = HandlerStorage(integration_id)
        modelStorage = ModelStorage(predictor_id)

        ml_handler = HandlerClass(
            engine_storage=handlerStorage,
            model_storage=modelStorage,
        )
        self.ml_instance = ml_handler

    def exit(self):
        sys.exit(0)


# =================  parent wrapper ====================

class MLHandlerWrapper:
    def __init__(self):
        # TODO change to virtualenv from config
        python_path = sys.executable
        wrapper_path = __file__
        p = subprocess.Popen(
            [python_path, wrapper_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.proc = p

    def __del__(self):
        if self.proc is not None:
            self.close()

    def close(self):
        self.send_command({'method': 'exit'})
        # self.proc.stdin.close()
        self.proc.wait()
        self.proc = None

    def __getattr__(self, method_name):
        # is call of the method
        def call(*args, **kwargs):
            return self.exec_command(method_name, *args, **kwargs)
        return call

    def send_command(self, params):
        params_enc = pickle.dumps(params)
        length_enc = struct.pack('L', len(params_enc))

        # send len
        self.proc.stdin.write(length_enc)

        self.proc.stdin.write(params_enc)
        self.proc.stdin.flush()

    def exec_command(self, method, *args, **kwargs):
        # protocol: len of package [8bytes], packed (pickled dict) [len]

        params = {
            'method': method,
            'args': args,
            'kwargs': kwargs
        }
        self.send_command(params)

        length_enc = self.proc.stdout.read(8)
        length = struct.unpack('L', length_enc)[0]

        ret_enc = self.proc.stdout.read(length)
        ret = pickle.loads(ret_enc)

        if ret is not None and 'error' in ret:
            raise RuntimeError(ret)
        return ret


# ================= thread to keep wrapper opened ====================

process_thread = None
queue_in = queue.Queue()
queue_out = queue.Queue()
queue_lock = Lock()


def process_keeper():
    wrapper = MLHandlerWrapper()

    while True:
        method_name, args, kwargs = queue_in.get()
        method = wrapper.__getattr__(method_name)
        try:
            ret = method(*args, **kwargs)

        except RuntimeError as e:
            ret = e
        except Exception as e:
            # TODO to something else?
            ret = e
        queue_in.task_done()
        queue_out.put(ret)


class MLHandlerPersistWrapper:
    '''
        keeps child process opened
    '''

    def __init__(self):
        global process_thread
        if process_thread is None:
            process_thread = threading.Thread(target=process_keeper, daemon=True)
            process_thread.start()

    def __getattr__(self, method_name):
        global queue_in, queue_out, queue_lock

        # is call of the method

        def call(*args, **kwargs):
            # send one task at the time
            queue_lock.acquire()

            queue_in.put([method_name, args, kwargs])
            result = queue_out.get()

            queue_lock.release()

            if isinstance(result, Exception):
                raise result

            return result

        return call

    def close(self):
        # not close child pricess
        pass


# run child process
if __name__ == '__main__':
    MLHandlerProcess().mainloop()
