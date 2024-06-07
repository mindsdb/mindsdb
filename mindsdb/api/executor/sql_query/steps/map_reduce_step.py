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
from .fetch_dataframe import FetchDataframeStepCall


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


def unmarkQueryVar(where):
    if isinstance(where, BinaryOperation):
        unmarkQueryVar(where.args[0])
        unmarkQueryVar(where.args[1])
    elif isinstance(where, UnaryOperation):
        unmarkQueryVar(where.args[0])
    elif isinstance(where, Constant):
        if hasattr(where, 'is_var') and where.is_var is True:
            where.value = where.var_name


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

    def call(self, step):
        if step.reduce != 'union':
            raise LogicError(f'Unknown MapReduceStep type: {step.reduce}')

        step_data = self.steps_data[step.values.step_num]
        vars = []
        for row in step_data.get_records():
            var_group = {}
            vars.append(var_group)
            for name, value in row.items():
                if name != '__mindsdb_row_id':
                    var_group[name] = value

        data = ResultSet()

        substep = step.step
        if type(substep) is FetchDataframeStep:
            query = substep.query
            if len(vars) == 0:
                substep.query.limit = Constant(0)
                substep.query.where = None

                sub_data = self._fetch_dataframe_step(substep)

                for column in sub_data.columns:
                    data.add_column(column)

                data.add_from_result_set(sub_data)

            for var_group in vars:
                markQueryVar(query.where)
                for name, value in var_group.items():
                    replaceQueryVar(query.where, value, name)
                sub_data = self._fetch_dataframe_step(substep)
                if len(data.columns) == 0:
                    data = sub_data
                else:
                    data.add_from_result_set(sub_data)

                unmarkQueryVar(query.where)
        elif type(substep) is MultipleSteps:
            data = self._multiple_steps_reduce(substep, vars)
        else:
            raise LogicError(f'Unknown step type: {step.step}')
        return data

    def _multiple_steps_reduce(self, step, vars):
        if step.reduce != 'union':
            raise LogicError(f'Unknown MultipleSteps type: {step.reduce}')

        data = ResultSet()

        # mark vars
        steps = []
        for substep in step.steps:
            if isinstance(substep, FetchDataframeStep) is False:
                raise LogicError(f'Wrong step type for MultipleSteps: {step}')
            substep = copy.deepcopy(substep)
            markQueryVar(substep.query.where)
            steps.append(substep)

        for var_group in vars:
            steps2 = copy.deepcopy(steps)
            for name, value in var_group.items():
                for substep in steps2:
                    replaceQueryVar(substep.query.where, value, name)
            sub_data = self._multiple_steps(steps2)
            data = join_query_data(data, sub_data)

        return data

    def _multiple_steps(self, steps):
        data = ResultSet()
        for substep in steps:
            sub_data = self._fetch_dataframe_step(substep)
            data = join_query_data(data, sub_data)
        return data

    def _fetch_dataframe_step(self, step):
        return FetchDataframeStepCall(self.sql_query).call(step)
