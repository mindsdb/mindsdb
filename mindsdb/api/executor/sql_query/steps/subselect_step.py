from collections import defaultdict

import pandas as pd

from mindsdb_sql_parser.ast import (
    Identifier,
    Select,
    Star,
    Constant,
    Function,
    Variable,
    BinaryOperation,
)

from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import SERVER_VARIABLES
from mindsdb.api.executor.planner.step_result import Result
from mindsdb.api.executor.planner.steps import SubSelectStep, QueryStep
from mindsdb.api.executor.sql_query.result_set import ResultSet, Column
from mindsdb.api.executor.utilities.sql import query_df
from mindsdb.api.executor.exceptions import KeyColumnDoesNotExist
from mindsdb.integrations.utilities.query_traversal import query_traversal
from mindsdb.interfaces.query_context.context_controller import query_context_controller

from .base import BaseStepCall
from .fetch_dataframe import get_fill_param_fnc


class SubSelectStepCall(BaseStepCall):
    bind = SubSelectStep

    def call(self, step):
        result = self.steps_data[step.dataframe.step_num]

        table_name = step.table_name
        if table_name is None:
            table_name = "df_table"
        else:
            table_name = table_name

        query = step.query
        query.from_table = Identifier("df_table")

        if step.add_absent_cols and isinstance(query, Select):
            query_cols = set()

            def f_all_cols(node, **kwargs):
                if isinstance(node, Identifier):
                    query_cols.add(node.parts[-1])
                elif isinstance(node, Result):
                    prev_result = self.steps_data[node.step_num]
                    return Constant(prev_result.get_column_values(col_idx=0)[0])

            query_traversal(query.where, f_all_cols)

            result_cols = [col.name for col in result.columns]

            for col_name in query_cols:
                if col_name not in result_cols:
                    result.add_column(Column(name=col_name))

        # inject previous step values
        if isinstance(query, Select):
            fill_params = get_fill_param_fnc(self.steps_data)
            query_traversal(query, fill_params)

        df = result.to_df()
        res = query_df(df, query, session=self.session)

        # get database from first column
        database = result.columns[0].database

        return ResultSet.from_df(res, database, table_name)


class QueryStepCall(BaseStepCall):
    bind = QueryStep

    def call(self, step: QueryStep):
        query = step.query

        if step.from_table is not None:
            if isinstance(step.from_table, pd.DataFrame):
                result_set = ResultSet.from_df(step.from_table)
            else:
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

        lower_col_idx = {}
        for key, value in col_idx.items():
            if isinstance(key, int):
                key = str(key)
            if isinstance(key, str):
                lower_col_idx[key.lower()] = value
                continue
            lower_col_idx[tuple(str(x).lower() for x in key)] = value

        # get aliases of first level
        aliases = []
        for col in query.targets:
            if col.alias is not None:
                aliases.append(col.alias.parts[0])

        # analyze condition and change name of columns
        def check_fields(node, is_target=None, **kwargs):
            if isinstance(node, Function):
                function_name = node.op.lower()

                functions_results = {
                    "database": self.session.database,
                    "current_user": self.session.username,
                    "user": self.session.username,
                    "version": "8.0.17",
                    "current_schema": "public",
                    "schema": "public",
                    "connection_id": self.context.get("connection_id"),
                }
                if function_name in functions_results:
                    return Constant(functions_results[function_name], alias=Identifier(parts=[function_name]))

            if isinstance(node, Variable):
                var_name = node.value
                column_name = f"@@{var_name}"
                result = SERVER_VARIABLES.get(column_name)
                if result is None:
                    raise ValueError(f"Unknown variable '{var_name}'")
                else:
                    return Constant(result[0], alias=Identifier(parts=[column_name]))

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
                        return [Identifier(parts=[col]) for col in tbl_idx.get(table_name, [])]

                if node.parts[-1].lower() == "session_user":
                    return Constant(self.session.username, alias=node)
                if node.parts[-1].lower() == "$$":
                    # NOTE: sinve version 9.0 mysql client sends query 'select $$'.
                    # Connection can be continued only if answer is parse error.
                    raise ValueError(
                        "You have an error in your SQL syntax; check the manual that corresponds to your server "
                        "version for the right syntax to use near '$$' at line 1"
                    )

                match node.parts, node.is_quoted:
                    case [column_name], [column_quoted]:
                        if column_name in aliases:
                            # key is defined as alias
                            return

                        key = column_name if column_quoted else column_name.lower()

                        if key not in col_idx and key not in lower_col_idx:
                            # it can be local alias of a query, like:
                            # SELECT t1.a + t2.a col1, min(t1.a) c
                            # FROM dummy_data.tbl1 as t1
                            # JOIN pg.tbl2 as t2 on t1.c=t2.c
                            # group by col1
                            # order by c -- <--- "с" is alias
                            return
                    case [*_, table_name, column_name], [*_, column_quoted]:
                        key = (table_name, column_name) if column_quoted else (table_name.lower(), column_name.lower())

                search_idx = col_idx if column_quoted else lower_col_idx

                if key not in search_idx:
                    raise KeyColumnDoesNotExist(f"Table not found for column: {key}")

                new_name = search_idx[key]
                return Identifier(parts=[new_name], alias=node.alias)

        # fill params
        fill_params = get_fill_param_fnc(self.steps_data)
        query_traversal(query, fill_params)

        if not step.strict_where:
            # remove conditions with not-existed columns.
            #   these conditions can be already used as input to model or knowledge base
            #   but can be absent in their output

            def remove_not_used_conditions(node, **kwargs):
                # find last in where
                if isinstance(node, BinaryOperation):
                    for arg in node.args:
                        if isinstance(arg, Identifier) and len(arg.parts) > 1:
                            key = tuple(arg.parts[-2:])
                            if key not in col_idx:
                                # exclude
                                node.args = [Constant(0), Constant(0)]
                                node.op = "="

            query_traversal(query.where, remove_not_used_conditions)

        query_traversal(query, check_fields)
        query.where = query_context_controller.remove_lasts(query.where)

        query.from_table = Identifier("df_table")
        res = query_df(df, query, session=self.session)

        return ResultSet.from_df_cols(df=res, columns_dict=col_names, strict=False)
