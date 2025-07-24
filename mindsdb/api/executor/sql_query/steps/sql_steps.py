import pandas as pd

from mindsdb.api.executor.planner.steps import (
    LimitOffsetStep,
    DataStep,
)

from mindsdb.api.executor.sql_query.result_set import ResultSet

from .base import BaseStepCall


class LimitOffsetStepCall(BaseStepCall):

    bind = LimitOffsetStep

    def call(self, step):
        step_data = self.steps_data[step.dataframe.step_num]

        df = step_data.get_raw_df()

        step_data2 = ResultSet(columns=list(step_data.columns))

        if isinstance(step.offset, int):
            df = df[step.offset:]
        if isinstance(step.limit, int):
            df = df[:step.limit]

        step_data2.add_raw_df(df)

        return step_data2


class DataStepCall(BaseStepCall):

    bind = DataStep

    def call(self, step):
        # create resultset
        df = pd.DataFrame(step.data)
        return ResultSet.from_df(df, database='', table_name='')
