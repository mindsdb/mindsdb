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

import sys
import pickle
import inspect
import io

import pandas as pd


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
    # find the first class that contents predict and train methods
    for _, klass in inspect.getmembers(module, inspect.isclass):
        funcs = [
            name
            for name, _ in inspect.getmembers(klass, inspect.isfunction)
        ]
        if 'predict' in funcs and 'train' in funcs:
            return klass


def main():
    # replace print output to stderr
    sys.stdout = sys.stderr

    params = get_input()

    method = params['method']
    code = params['code']

    module = import_string(code)

    model_class = find_model_class(module)

    if method == 'train':
        df = pd_decode(params['df'])
        to_predict = params['to_predict']
        model = model_class()
        model.train(df, to_predict)

        # return model
        data = model.__dict__

        model_state = encode(data)
        return_output(model_state)

    elif method == 'predict':
        model_state = params['model_state']
        df = pd_decode(params['df'])

        model = model_class()
        model.__dict__ = decode(model_state)

        res = model.predict(df)
        return_output(pd_encode(res))

    raise NotImplementedError(method)


if __name__ == '__main__':
    main()
