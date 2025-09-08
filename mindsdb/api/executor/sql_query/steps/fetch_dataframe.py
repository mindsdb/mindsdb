from mindsdb_sql_parser.ast import (
    Identifier,
    Constant,
    Select,
    Join,
    Parameter,
    BinaryOperation,
    Tuple,
    Union,
    Intersect,
)

from mindsdb.api.executor.planner.steps import FetchDataframeStep
from mindsdb.api.executor.datahub.classes.response import DataHubResponse
from mindsdb.api.executor.sql_query.result_set import ResultSet
from mindsdb.api.executor.exceptions import UnknownError
from mindsdb.integrations.utilities.query_traversal import query_traversal
from mindsdb.interfaces.query_context.context_controller import query_context_controller

from .base import BaseStepCall


def get_table_alias(table_obj, default_db_name):
    # (database, table, alias)
    if isinstance(table_obj, Identifier):
        if len(table_obj.parts) == 1:
            name = (default_db_name, table_obj.parts[0])
        else:
            name = (table_obj.parts[0], table_obj.parts[-1])
    elif isinstance(table_obj, Select):
        # it is subquery
        if table_obj.alias is None:
            name = "t"
        else:
            name = table_obj.alias.parts[0]
        name = (default_db_name, name)
    elif isinstance(table_obj, Join):
        # get from first table
        return get_table_alias(table_obj.left, default_db_name)
    else:
        # unknown yet object
        return default_db_name, "t", "t"

    if table_obj.alias is not None:
        name = name + (".".join(table_obj.alias.parts),)
    else:
        name = name + (name[1],)
    return name


def get_fill_param_fnc(steps_data):
    def fill_params(node, callstack=None, **kwargs):
        if not isinstance(node, Parameter):
            return

        rs = steps_data[node.value.step_num]
        items = [Constant(i) for i in rs.get_column_values(col_idx=0)]

        is_single_item = True
        if callstack:
            node_prev = callstack[0]
            if isinstance(node_prev, BinaryOperation):
                # Check case: 'something IN Parameter()'
                if node_prev.op.lower() == "in" and node_prev.args[1] is node:
                    is_single_item = False

        if is_single_item and len(items) == 1:
            # extract one value for option 'col=(subselect)'
            node = items[0]
        else:
            node = Tuple(items)
        return node

    return fill_params


class FetchDataframeStepCall(BaseStepCall):
    bind = FetchDataframeStep

    def call(self, step):
        dn = self.session.datahub.get(step.integration)
        query = step.query

        if dn is None:
            raise UnknownError(f"Unknown integration name: {step.integration}")

        if query is None:
            table_alias = (self.context.get("database"), "result", "result")

            # fetch raw_query
            response: DataHubResponse = dn.query(native_query=step.raw_query, session=self.session)
            df = response.data_frame
        else:
            if isinstance(step.query, (Union, Intersect)):
                table_alias = ["", "", ""]
            else:
                table_alias = get_table_alias(step.query.from_table, self.context.get("database"))

            # TODO for information_schema we have 'database' = 'mindsdb'

            # fill params
            fill_params = get_fill_param_fnc(self.steps_data)
            query_traversal(query, fill_params)

            query, context_callback = query_context_controller.handle_db_context_vars(query, dn, self.session)

            response: DataHubResponse = dn.query(query=query, session=self.session)
            df = response.data_frame

            if context_callback:
                context_callback(df, response.columns)

        # if query registered, set progress
        if self.sql_query.run_query is not None:
            self.sql_query.run_query.set_progress(processed_rows=len(df))
        return ResultSet.from_df(
            df,
            table_name=table_alias[1],
            table_alias=table_alias[2],
            database=table_alias[0],
            mysql_types=response.mysql_types,
        )
