import copy

from mindsdb_sql_parser.ast import (
    BinaryOperation,
    UnaryOperation,
    Constant,
)
from mindsdb.api.executor.planner.steps import (
    MapReduceStep,
    FetchDataframeStep,
    MultipleSteps,
    SubSelectStep,
)

from mindsdb.api.executor.sql_query.result_set import ResultSet
from mindsdb.api.executor.exceptions import LogicError
from mindsdb.utilities.partitioning import process_dataframe_in_partitions

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

        partition = getattr(step, 'partition', None)

        if partition is not None:
            data = self._reduce_partition(step, partition)

        else:
            data = self._reduce_vars(step)

        return data

    def _reduce_partition(self, step, partition):
        if not isinstance(partition, int):
            raise ValueError('Only integers are supported in partition definition.')
        if partition <= 0:
            raise ValueError('Partition must be a positive number')

        input_idx = step.values.step_num
        input_data = self.steps_data[input_idx]
        input_columns = list(input_data.columns)

        substeps = step.step
        if not isinstance(substeps, list):
            substeps = [substeps]

        data = ResultSet()

        df = input_data.get_raw_df()

        def callback(chunk):
            return self._exec_partition(chunk, substeps, input_idx, input_columns)

        for result in process_dataframe_in_partitions(df, callback, partition):
            if result:
                data = join_query_data(data, result)

        return data

    def _exec_partition(self, df, substeps, input_idx, input_columns):

        input_data2 = ResultSet(columns=input_columns.copy())
        input_data2.add_raw_df(df)

        # execute with modified previous results
        steps_data2 = self.steps_data.copy()
        steps_data2[input_idx] = input_data2

        sub_data = None
        for substep in substeps:
            sub_data = self.sql_query.execute_step(substep, steps_data=steps_data2)
            steps_data2[substep.step_num] = sub_data

        return sub_data

    def _reduce_vars(self, step):
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

        data = ResultSet()

        for var_group in vars:
            steps2 = copy.deepcopy(substep)

            self._fill_vars(steps2, var_group)

            sub_data = self.sql_query.execute_step(steps2)
            data = join_query_data(data, sub_data)

        return data

    def _fill_vars(self, step, var_group):
        if isinstance(step, MultipleSteps):
            for substep in step.steps:
                self._fill_vars(substep, var_group)
        elif isinstance(step, (FetchDataframeStep, SubSelectStep)):
            markQueryVar(step.query.where)
            for name, value in var_group.items():
                replaceQueryVar(step.query.where, value, name)
