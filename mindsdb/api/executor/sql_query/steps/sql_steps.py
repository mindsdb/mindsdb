import pandas as pd

from mindsdb_sql.planner.steps import (
    LimitOffsetStep,
    DataStep,
)

from mindsdb.api.executor.sql_query.result_set import ResultSet

from .base import BaseStepCall


class LimitOffsetStepCall(BaseStepCall):

    bind = LimitOffsetStep

    def call(self, step):
        step_data = self.steps_data[step.dataframe.step_num]

        step_data2 = ResultSet()
        for col in step_data.columns:
            step_data2.add_column(col)

        records = step_data.get_records_raw()

        if isinstance(step.offset, int):
            records = records[step.offset:]
        if isinstance(step.limit, int):
            records = records[:step.limit]

        for record in records:
            step_data2.add_record_raw(record)

        return step_data2


class DataStepCall(BaseStepCall):

    bind = DataStep

    def call(self, step):
        # create resultset
        df = pd.DataFrame(step.data)
        return ResultSet().from_df(df, database='', table_name='')
