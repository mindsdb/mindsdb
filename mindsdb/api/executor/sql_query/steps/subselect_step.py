from mindsdb_sql.parser.ast import (
    Identifier,
    Select,
)
from mindsdb_sql.planner.steps import SubSelectStep
from mindsdb_sql.planner.utils import query_traversal

from mindsdb.api.executor.sql_query.result_set import ResultSet, Column
from mindsdb.api.executor.utilities.sql import query_df

from .base import BaseStepCall


class SubSelectStepCall(BaseStepCall):

    bind = SubSelectStep

    def call(self, step):
        result = self.steps_data[step.dataframe.step_num]

        table_name = step.table_name
        if table_name is None:
            table_name = 'df_table'
        else:
            table_name = table_name

        query = step.query
        query.from_table = Identifier('df_table')

        if step.add_absent_cols and isinstance(query, Select):
            query_cols = set()

            def f_all_cols(node, **kwargs):
                if isinstance(node, Identifier):
                    query_cols.add(node.parts[-1])

            query_traversal(query.where, f_all_cols)

            result_cols = [col.name for col in result.columns]

            for col_name in query_cols:
                if col_name not in result_cols:
                    result.add_column(Column(col_name))

        df = result.to_df()
        res = query_df(df, query)

        result2 = ResultSet()
        # get database from first column
        database = result.columns[0].database
        result2.from_df(res, database, table_name)

        return result2
