import os
from typing import Callable

from mindsdb_sql_parser import Function, Constant, Variable

from mindsdb.utilities import log
from mindsdb.interfaces.storage.fs import RESOURCE_GROUP
from mindsdb.interfaces.storage.json import get_json_storage


logger = log.getLogger(__name__)


ENV_VAR_PREFIX = "MDB_"


class VariablesController:
    def __init__(self) -> None:
        self._storage = get_json_storage(resource_id=0, resource_group=RESOURCE_GROUP.SYSTEM)
        self._store_key = "variables"
        self._data = None

    def _get_data(self) -> dict:
        if self._data is None:
            self._data = self._storage.get(self._store_key)
            if self._data is None:
                self._data = {}
        return self._data

    def get_value(self, name: str):
        data = self._get_data()
        if name not in data:
            raise ValueError(f"Variable {name} is not defined")
        return data[name]

    def set_value(self, name: str, value):
        data = self._get_data()
        data[name] = value
        self._storage.set(self._store_key, data)

    def _from_env(self, name: Constant) -> str:
        # gets variable value from environment.
        # available names are restricted by ENV_VAR_PREFIX to don't provide access to arbitrary venv variable

        var_name = name.value
        if not var_name.startswith(ENV_VAR_PREFIX):
            raise ValueError(f"Can access only to variable names starting with {ENV_VAR_PREFIX}")
        if var_name not in os.environ:
            raise ValueError(f"Environment variable {var_name} is not defined")
        return os.environ[var_name]

    def _get_function(self, name: str) -> Callable:
        if name == "from_env":
            return self._from_env
        raise ValueError(f"Function {name} is not found")

    def set_variable(self, name: str, value):
        # store new value for variable in database
        #   if value is a function - extract value using this function

        name = name.lower()
        if isinstance(value, Function):
            fnc = self._get_function(value.op)
            value = fnc(*value.args)

        elif isinstance(value, Constant):
            value = value.value

        else:
            # ignore
            return

        self.set_value(name, value)

    def fill_parameters(self, var):
        # recursively check input and fill Variables if they exist there

        if isinstance(var, Variable):
            return self.get_value(var.value.lower())
        if isinstance(var, Function):
            fnc = self._get_function(var.op)
            return fnc(*var.args)
        elif isinstance(var, dict):
            return {key: self.fill_parameters(value) for key, value in var.items()}
        elif isinstance(var, list):
            return [self.fill_parameters(value) for value in var]
        return var


variables_controller = VariablesController()
