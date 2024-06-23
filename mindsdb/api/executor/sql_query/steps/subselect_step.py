from collections import defaultdict

from mindsdb_sql.parser.ast import (
    Identifier,
    Select,
    Star
)
from mindsdb_sql.planner.steps import SubSelectStep, QueryStep
from mindsdb_sql.planner.utils import query_traversal

from mindsdb.api.executor.sql_query.result_set import ResultSet, Column
from mindsdb.api.executor.utilities.sql import query_df

from mindsdb.interfaces.query_context.context_controller import query_context_controller

from mindsdb.api.executor.exceptions import KeyColumnDoesNotExist

from .base import BaseStepCall
from .fetch_dataframe import get_fill_param_fnc


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
        res = query_df(df, query, session=self.session)

        result2 = ResultSet()
        # get database from first column
        database = result.columns[0].database
        result2.from_df(res, database, table_name)

        return result2


class QueryStepCall(BaseStepCall):

    bind = QueryStep

    def call(self, step):
        query = step.query

        if step.from_table is not None:
            result_set = self.steps_data[step.from_table.step_num]
        else:
            # only from_table can content result
            prev_step_num = query.from_table.value.step_num
            result_set = self.steps_data[prev_step_num]

        df, col_names = result_set.to_df_cols()
        col_idx = {}
        tbl_idx = defaultdict(list)
        for name, col in col_names.items():
            col_idx[col.alias] = name
            col_idx[(col.table_alias, col.alias)] = name
            # add to tables
            tbl_idx[col.table_name].append(name)
            if col.table_name != col.table_alias:
                tbl_idx[col.table_alias].append(name)

        # analyze condition and change name of columns
        def check_fields(node, is_target=None, **kwargs):

            if isinstance(node, Identifier):
                # only column name
                col_name = node.parts[-1]
                if is_target and isinstance(col_name, Star):
                    if len(node.parts) == 1:
                        # left as is
                        return
                    else:
                        # replace with all columns from table
                        table_name = node.parts[-2]
                        return [
                            Identifier(parts=[col])
                            for col in tbl_idx.get(table_name, [])
                        ]

                if len(node.parts) == 1:
                    key = col_name
                else:
                    table_name = node.parts[-2]
                    key = (table_name, col_name)

                if key not in col_idx:
                    raise KeyColumnDoesNotExist(f'Table not found for column: {key}')

                new_name = col_idx[key]
                return Identifier(parts=[new_name], alias=node.alias)

        # fill params
        fill_params = get_fill_param_fnc(self.steps_data)
        query_traversal(query, fill_params)

        query_traversal(query, check_fields)
        query.where = query_context_controller.remove_lasts(query.where)

        query.from_table = Identifier('df_table')
        res = query_df(df, query, session=self.session)

        data = ResultSet().from_df_cols(res, col_names, strict=False)

        return data
