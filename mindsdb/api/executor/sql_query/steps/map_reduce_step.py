import copy

from mindsdb_sql.parser.ast import (
    BinaryOperation,
    UnaryOperation,
    Constant,
)
from mindsdb_sql.planner.steps import (
    MapReduceStep,
    FetchDataframeStep,
    MultipleSteps,
)

from mindsdb.api.executor.sql_query.result_set import ResultSet
from mindsdb.api.executor.exceptions import LogicError

from .base import BaseStepCall


def markQueryVar(where):
    if isinstance(where, BinaryOperation):
        markQueryVar(where.args[0])
        markQueryVar(where.args[1])
    elif isinstance(where, UnaryOperation):
        markQueryVar(where.args[0])
    elif isinstance(where, Constant):
        if str(where.value).startswith('$var['):
            where.is_var = True
            where.var_name = where.value


def replaceQueryVar(where, var_value, var_name):
    if isinstance(where, BinaryOperation):
        replaceQueryVar(where.args[0], var_value, var_name)
        replaceQueryVar(where.args[1], var_value, var_name)
    elif isinstance(where, UnaryOperation):
        replaceQueryVar(where.args[0], var_value, var_name)
    elif isinstance(where, Constant):
        if hasattr(where, 'is_var') and where.is_var is True and where.value == f'$var[{var_name}]':
            where.value = var_value


def join_query_data(target, source):
    if len(target.columns) == 0:
        target = source
    else:
        target.add_from_result_set(source)
    return target


class MapReduceStepCall(BaseStepCall):

    bind = MapReduceStep

    def call(self, step: MultipleSteps):
        if step.reduce != 'union':
            raise LogicError(f'Unknown MapReduceStep type: {step.reduce}')

        # extract vars
        step_data = self.steps_data[step.values.step_num]
        vars = []
        for row in step_data.get_records():
            var_group = {}
            vars.append(var_group)
            for name, value in row.items():
                if name != '__mindsdb_row_id':
                    var_group[name] = value

        substep = step.step
        data = self._steps_reduce(substep, vars)

        return data

    def _steps_reduce(self, step, vars):

        data = ResultSet()

        for var_group in vars:
            steps2 = copy.deepcopy(step)

            self._fill_vars(steps2, var_group)

            sub_data = self.sql_query.execute_step(steps2)
            data = join_query_data(data, sub_data)

        return data

    def _fill_vars(self, step, var_group):
        if isinstance(step, MultipleSteps):
            for substep in step.steps:
                self._fill_vars(substep, var_group)
        if isinstance(step, FetchDataframeStep):
            markQueryVar(step.query.where)
            for name, value in var_group.items():
                replaceQueryVar(step.query.where, value, name)
