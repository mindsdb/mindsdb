from duckdb import typing
from mindsdb.interfaces.storage.model_fs import HandlerStorage


def python_to_duckdb_type(py_type):
    if py_type == 'int':
        return typing.BIGINT
    elif py_type == 'float':
        return typing.DOUBLE
    elif py_type == 'str':
        return typing.VARCHAR
    elif py_type == 'bool':
        return typing.BOOLEAN
    elif py_type == 'bytes':
        return typing.BLOB
    else:
        # Unknown
        return typing.VARCHAR


# duckdb don't like *args
def function_maker(n_args, other_function):
    return [
        lambda: other_function(),
        lambda arg_0: other_function(arg_0),
        lambda arg_0, arg_1: other_function(arg_0, arg_1),
        lambda arg_0, arg_1, arg_2: other_function(arg_0, arg_1, arg_2),
        lambda arg_0, arg_1, arg_2, arg_3: other_function(arg_0, arg_1, arg_2, arg_2),
    ][n_args]


class BYOMUserFunctions:
    """
    User functions based on BYOM handler
    """

    def __init__(self, session):
        self.session = session
        self.functions = {}

        # get all byom engines
        self.byom_engines = []
        for name, info in session.integration_controller.get_all().items():
            if info['type'] == 'ml' and info['engine'] == 'byom':
                self.byom_engines.append(name)

        self.byom_methods = {}
        self.byom_handlers = {}

    def get_byom_methods(self, engine):
        if engine not in self.byom_methods:
            ml_handler = self.session.integration_controller.get_ml_handler(engine)

            storage = HandlerStorage(ml_handler.integration_id)
            methods = storage.json_get('methods')
            self.byom_methods[engine] = methods
            self.byom_handlers[engine] = ml_handler

        return self.byom_methods[engine]

    def check_function(self, node):
        engine = node.namespace
        if engine not in self.byom_engines:
            return

        methods = self.get_byom_methods(engine)

        fnc_name = node.op.lower()
        if fnc_name not in methods:
            # do nothing
            return

        new_name = f'{node.namespace}_{fnc_name}'
        node.op = new_name

        def callback(*args):
            return self.byom_handlers[engine].function_call(fnc_name, args)

        input_types = [
            python_to_duckdb_type(param['type'])
            for param in methods[fnc_name]['input_params']
        ]
        output_type = methods[fnc_name]['output_type']

        self.functions[new_name] = {
            'callback': function_maker(len(input_types), callback),
            'input': input_types,
            'output': python_to_duckdb_type(output_type)
        }

    def register(self, connection):
        for name, info in self.functions.items():
            connection.create_function(
                name,
                info['callback'],
                info['input'],
                info['output'],
                null_handling="special"
            )
