import os

from duckdb.typing import BIGINT, DOUBLE, VARCHAR, BLOB, BOOLEAN
from mindsdb.interfaces.storage.model_fs import HandlerStorage


def python_to_duckdb_type(py_type):
    if py_type == 'int':
        return BIGINT
    elif py_type == 'float':
        return DOUBLE
    elif py_type == 'str':
        return VARCHAR
    elif py_type == 'bool':
        return BOOLEAN
    elif py_type == 'bytes':
        return BLOB
    else:
        # Unknown
        return VARCHAR


# duckdb doesn't like *args
def function_maker(n_args, other_function):
    return [
        lambda: other_function(),
        lambda arg_0: other_function(arg_0),
        lambda arg_0, arg_1: other_function(arg_0, arg_1),
        lambda arg_0, arg_1, arg_2: other_function(arg_0, arg_1, arg_2),
        lambda arg_0, arg_1, arg_2, arg_3: other_function(arg_0, arg_1, arg_2, arg_2),
    ][n_args]


class BYOMFunctionsController:
    """
    User functions based on BYOM handler
    """

    def __init__(self, session):
        self.session = session

        self.byom_engines = None
        self.byom_methods = {}
        self.byom_handlers = {}

        self.callbacks = {}

    def get_engines(self):
        # get all byom engines
        if self.byom_engines is None:
            # first run
            self.byom_engines = []
            for name, info in self.session.integration_controller.get_all().items():
                if info['type'] == 'ml' and info['engine'] == 'byom':
                    if info['connection_data'].get('mode') == 'custom_function':
                        self.byom_engines.append(name)
        return self.byom_engines

    def get_methods(self, engine):
        if engine not in self.byom_methods:
            ml_handler = self.session.integration_controller.get_ml_handler(engine)

            storage = HandlerStorage(ml_handler.integration_id)
            methods = storage.json_get('methods')
            self.byom_methods[engine] = methods
            self.byom_handlers[engine] = ml_handler

        return self.byom_methods[engine]

    def check_function(self, node):
        engine = node.namespace
        if engine not in self.get_engines():
            return

        methods = self.get_methods(engine)

        fnc_name = node.op.lower()
        if fnc_name not in methods:
            # do nothing
            return

        new_name = f'{node.namespace}_{fnc_name}'
        node.op = new_name

        if new_name in self.callbacks:
            # already exists
            return self.callbacks[new_name]

        def callback(*args):
            return self.method_call(engine, fnc_name, args)

        input_types = [
            param['type']
            for param in methods[fnc_name]['input_params']
        ]

        meta = {
            'name': new_name,
            'callback': callback,
            'input_types': input_types,
            'output_type': methods[fnc_name]['output_type']
        }

        self.callbacks[new_name] = meta
        return meta

    def method_call(self, engine, method_name, args):
        return self.byom_handlers[engine].function_call(method_name, args)

    def create_function_set(self):
        return DuckDBFunctions(self)


class FunctionController(BYOMFunctionsController):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def check_function(self, node):
        meta = super().check_function(node)
        if meta is not None:
            return meta

        # builtin function
        if node.op.lower() == 'llm':
            return self.llm_call_function(node)

    def llm_call_function(self, node):
        name = node.op.lower()

        if name in self.callbacks:
            return self.callbacks[name]

        param_prefix = 'LLM_FUNCTION_'
        chat_model_params = {}
        for k, v in os.environ.items():
            if k.startswith(param_prefix):
                param_name = k[len(param_prefix):]
                if param_name == 'MODEL':
                    chat_model_params['model_name'] = v
                else:
                    chat_model_params[param_name.lower()] = v

        if 'provider' not in chat_model_params:
            chat_model_params['provider'] = 'openai'

        try:
            from langchain_core.messages import HumanMessage
            from mindsdb.interfaces.agents.langchain_agent import create_chat_model
            llm = create_chat_model(chat_model_params)
        except Exception as e:
            raise RuntimeError(f'Unable to use LLM function, check ENV variables: {e}')

        def callback(question):
            resp = llm([HumanMessage(question)])
            return resp.content

        meta = {
            'name': name,
            'callback': callback,
            'input_types': ['str'],
            'output_type': 'str'
        }
        self.callbacks[name] = meta
        return meta


class DuckDBFunctions:
    def __init__(self, controller):
        self.controller = controller
        self.functions = {}

    def check_function(self, node):

        meta = self.controller.check_function(node)
        if meta is None:
            return

        name = meta['name']

        if name in self.functions:
            return

        input_types = [
            python_to_duckdb_type(param)
            for param in meta['input_types']
        ]

        self.functions[name] = {
            'callback': function_maker(len(input_types), meta['callback']),
            'input': input_types,
            'output': python_to_duckdb_type(meta['output_type'])
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
