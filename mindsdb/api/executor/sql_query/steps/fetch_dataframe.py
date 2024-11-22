from mindsdb_sql_parser.ast import (
    Identifier,
    Constant,
    Select,
    Join,
    Parameter,
    BinaryOperation,
    Tuple,
)
from mindsdb.api.executor.planner.steps import FetchDataframeStep
from mindsdb.integrations.utilities.query_traversal import query_traversal

from mindsdb.api.executor.sql_query.result_set import ResultSet
from mindsdb.api.executor.exceptions import UnknownError
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
            name = 't'
        else:
            name = table_obj.alias.parts[0]
        name = (default_db_name, name)
    elif isinstance(table_obj, Join):
        # get from first table
        return get_table_alias(table_obj.left, default_db_name)
    else:
        # unknown yet object
        return default_db_name, 't', 't'

    if table_obj.alias is not None:
        name = name + ('.'.join(table_obj.alias.parts),)
    else:
        name = name + (name[1],)
    return name


def get_fill_param_fnc(steps_data):
    def fill_params(node, parent_query=None, **kwargs):
        if isinstance(node, BinaryOperation):
            if isinstance(node.args[1], Parameter):
                rs = steps_data[node.args[1].value.step_num]
                items = [Constant(i) for i in rs.get_column_values(col_idx=0)]
                if node.op == '=' and len(items) == 1:
                    # extract one value for option 'col=(subselect)'
                    node.args[1] = items[0]
                else:
                    node.args[1] = Tuple(items)
                return node

        if isinstance(node, Parameter):
            rs = steps_data[node.value.step_num]
            items = [Constant(i) for i in rs.get_column_values(col_idx=0)]
            return Tuple(items)
    return fill_params


class FetchDataframeStepCall(BaseStepCall):

    bind = FetchDataframeStep

    def call(self, step):

        dn = self.session.datahub.get(step.integration)
        query = step.query

        if dn is None:
            raise UnknownError(f'Unknown integration name: {step.integration}')

        if query is None:
            table_alias = (self.context.get('database'), 'result', 'result')

            # fetch raw_query
            df, columns_info = dn.query(
                native_query=step.raw_query,
                session=self.session
            )
        else:
            table_alias = get_table_alias(step.query.from_table, self.context.get('database'))

            # TODO for information_schema we have 'database' = 'mindsdb'

            # fill params
            fill_params = get_fill_param_fnc(self.steps_data)
            query_traversal(query, fill_params)

            query, context_callback = query_context_controller.handle_db_context_vars(query, dn, self.session)

            df, columns_info = dn.query(
                query=query,
                session=self.session
            )

            if context_callback:
                context_callback(df, columns_info)

        result = ResultSet()

        result.from_df(
            df,
            table_name=table_alias[1],
            table_alias=table_alias[2],
            database=table_alias[0]
        )

        return result
