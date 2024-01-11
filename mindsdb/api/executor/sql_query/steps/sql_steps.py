import pandas as pd

from mindsdb_sql.planner.steps import (
    LimitOffsetStep,
    DataStep,
)

from mindsdb.api.executor.sql_query.result_set import ResultSet

from .base import BaseStepCall


# class GroupByStepCall(BaseStepCall):
#
#     bind = GroupByStep
#
#     def call(self, step):
#         # used only in join of two regular tables
#         step_data = self.steps_data[step.dataframe.step_num]
#
#         df = step_data.to_df()
#
#         query = Select(targets=step.targets, from_table='df', group_by=step.columns).to_string()
#         res = query_df(df, query)
#
#         # stick all columns to first table
#         appropriate_table = step_data.get_tables()[0]
#
#         data = ResultSet()
#
#         data.from_df(res, appropriate_table['database'], appropriate_table['table_name'],
#                      appropriate_table['table_alias'])
#
#         # columns are changed
#         self.set_columns_list(data.columns)
#         return data


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


# class FilterStepCall(BaseStepCall):
#
#     bind = FilterStep
#
#     def call(self, step):
#         # used only in join of two regular tables
#         result_set = self.steps_data[step.dataframe.step_num]
#
#         df, col_names = result_set.to_df_cols()
#         col_idx = {}
#         for name, col in col_names.items():
#             col_idx[col.alias] = name
#             col_idx[(col.table_alias, col.alias)] = name
#
#         # analyze condition and change name of columns
#         def check_fields(node, is_table=None, **kwargs):
#             if is_table:
#                 raise NotSupportedYet('Subqueries is not supported in WHERE')
#             if isinstance(node, Identifier):
#                 # only column name
#                 col_name = node.parts[-1]
#
#                 if len(node.parts) == 1:
#                     key = col_name
#                 else:
#                     table_name = node.parts[-2]
#                     key = (table_name, col_name)
#
#                 if key not in col_idx:
#                     raise KeyColumnDoesNotExist(f'Table not found for column: {key}')
#
#                 new_name = col_idx[key]
#                 return Identifier(parts=[new_name])
#
#         where_query = step.query
#         query_traversal(where_query, check_fields)
#
#         query_context_controller.remove_lasts(where_query)
#         query = Select(targets=[Star()], from_table=Identifier('df'), where=where_query)
#
#         res = query_df(df, query)
#
#         result_set2 = ResultSet().from_df_cols(res, col_names)
#
#         return result_set2


class DataStepCall(BaseStepCall):

    bind = DataStep

    def call(self, step):
        # create resultset
        df = pd.DataFrame(step.data)
        return ResultSet().from_df(df, database='', table_name='')
