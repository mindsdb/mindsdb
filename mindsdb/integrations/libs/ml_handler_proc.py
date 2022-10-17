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
            except:
                error = traceback.format_exc()
                self.send_output({'error': error})
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

    def init(self, class_path, company_id, integration_id, predictor_id):
        # mdb initialization
        import mindsdb.interfaces.storage.db as db
        db.init()

        from mindsdb.interfaces.storage.model_fs import ModelStorage, HandlerStorage

        module_name, class_name = class_path
        module = importlib.import_module(module_name)
        klass = getattr(module, class_name)

        handlerStorage = HandlerStorage(company_id, integration_id)
        modelStorage = ModelStorage(company_id, predictor_id)

        ml_handler = klass(
            engine_storage=handlerStorage,
            model_storage=modelStorage,
        )
        self.ml_instance = ml_handler

    def exit(self):
        sys.exit(0)


class MLHandlerWraper:
    def __init__(self, class_path, company_id, integration_id, predictor_id):

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

        self.exec_command(
            'init',
             class_path=class_path,
             company_id=company_id,
             integration_id=integration_id,
             predictor_id=predictor_id
       )

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
            raise RuntimeError(ret['error'])
        return ret


if __name__ == '__main__':
    MLHandlerProcess().mainloop()
