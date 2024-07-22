"""
Utility functions used in the 'Bring Your Own Model' (BYOM) engine.

These functions interact with interfaces (stdin, stdout), python files, and the actual BYOM engine.

In particular, they:
    - Wrap and run python code in separate python proceess.
    - Communicate with parent process throughout stdin/out using pickle to serialize objects.


The flow is as follows:

    1. Receive module code, method with parameters and stored attributes from parent process
    2. A python class object is created from the code
    3. Class is instanced and filled with stored attributes
    4. A calls to the chosen method of the class is performed with any relevant parameters that were passed
    5. Response is generated, appropriately packaged and sent to stdout
    6. Exit
"""

import io
import sys
import pickle
import inspect
from enum import Enum

import pandas as pd


class BYOM_METHOD(Enum):
    CHECK = 1
    TRAIN = 2
    PREDICT = 3
    FINETUNE = 4
    DESCRIBE = 5
    FUNC_CALL = 6


def pd_encode(df):
    return df.to_parquet(engine='pyarrow')


def pd_decode(encoded):
    fd = io.BytesIO()
    fd.write(encoded)
    fd.seek(0)
    return pd.read_parquet(fd, engine='pyarrow')


def encode(obj):
    return pickle.dumps(obj, protocol=5)


def decode(encoded):
    return pickle.loads(encoded)


def return_output(obj):
    # read stdin
    encoded = encode(obj)
    with open(1, 'wb') as fd:
        fd.write(encoded)
    sys.exit(0)


def get_input():
    # write to stdout
    with open(0, 'rb') as fd:
        encoded = fd.read()
        obj = decode(encoded)
    return obj


def import_string(code, module_name='model'):
    # import string as python module

    import types
    module = types.ModuleType(module_name)

    exec(code, module.__dict__)
    # sys.modules['my_module'] = module
    return module


def find_model_class(module):
    # find the first class that contains predict and train methods
    for _, cls in inspect.getmembers(module, inspect.isclass):
        if inspect.getmodule(cls) is not None:
            # is imported class
            continue

        funcs = [
            name
            for name, _ in inspect.getmembers(cls, inspect.isfunction)
        ]
        if 'predict' in funcs and 'train' in funcs:
            # found
            return cls


def get_methods_info(module):
    # get all methods and their types
    methods = {}
    for method_name, method in inspect.getmembers(module, inspect.isfunction):

        sig = inspect.signature(method)
        input_params = [
            {'name': name, 'type': param.annotation.__name__}
            for name, param in sig.parameters.items()
        ]
        methods[method_name] = {
            'input_params': input_params,
            'output_type': sig.return_annotation.__name__
        }
    return methods


def check_module(module, mode):
    # checks module and returns info

    methods = {}
    if mode == 'custom_function':
        methods = get_methods_info(module)

    else:
        # is BYOM, check it.
        model_class = find_model_class(module)
        if model_class is None:
            raise RuntimeError('Unable to find model class (it has to have `train` and `predict` methods)')

        # try to initialize
        model_class()
    return {'methods': methods}


def main():
    # replace print output to stderr
    sys.stdout = sys.stderr

    params = get_input()

    method = BYOM_METHOD(params['method'])
    code = params['code']

    module = import_string(code)

    if method == BYOM_METHOD.FUNC_CALL:
        func_name = params['func_name']
        args = params['args']

        func = getattr(module, func_name)
        return return_output(func(*args))

    if method == BYOM_METHOD.CHECK:

        mode = params['mode']
        info = check_module(module, mode)

        return return_output(info)

    model_class = find_model_class(module)

    if method == BYOM_METHOD.TRAIN:
        df = params['df']
        if df is not None:
            df = pd_decode(df)
        to_predict = params['to_predict']
        args = params['args']
        model = model_class()

        call_args = [df, to_predict]
        if args:
            call_args.append(args)
        model.train(*call_args)

        # return model
        data = model.__dict__

        model_state = encode(data)
        return_output(model_state)

    elif method == BYOM_METHOD.PREDICT:
        model_state = params['model_state']
        df = pd_decode(params['df'])
        args = params['args']

        model = model_class()
        model.__dict__ = decode(model_state)

        call_args = [df]
        if args:
            call_args.append(args)
        res = model.predict(*call_args)
        return_output(pd_encode(res))

    elif method == BYOM_METHOD.FINETUNE:
        model_state = params['model_state']
        df = pd_decode(params['df'])
        args = params['args']

        model = model_class()
        model.__dict__ = decode(model_state)

        call_args = [df]
        if args:
            call_args.append(args)

        model.finetune(*call_args)

        # return model
        data = model.__dict__
        model_state = encode(data)
        return_output(model_state)

    elif method == BYOM_METHOD.DESCRIBE:
        model_state = params['model_state']
        model = model_class()
        model.__dict__ = decode(model_state)
        try:
            df = model.describe(params.get('attribute'))
        except Exception:
            return_output(pd_encode(pd.DataFrame()))
        return_output(pd_encode(df))

    raise NotImplementedError(method)


if __name__ == '__main__':
    main()
