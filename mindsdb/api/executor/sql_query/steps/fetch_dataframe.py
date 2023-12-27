from mindsdb_sql.parser.ast import (
    Identifier,
    Constant,
    Select,
    Join,
    Parameter,
    Tuple,
)
from mindsdb_sql.planner.steps import FetchDataframeStep
from mindsdb_sql.planner.utils import query_traversal

from mindsdb.api.executor.sql_query.result_set import ResultSet, Column
from mindsdb.api.mysql.mysql_proxy.utilities import SqlApiUnknownError
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


class FetchDataframeStepCall(BaseStepCall):

    bind = FetchDataframeStep

    def call(self, step):

        dn = self.session.datahub.get(step.integration)
        query = step.query

        if dn is None:
            raise SqlApiUnknownError(f'Unknown integration name: {step.integration}')

        if query is None:
            table_alias = (self.context.get('database'), 'result', 'result')

            # fetch raw_query
            data, columns_info = dn.query(
                native_query=step.raw_query,
                session=self.session
            )
        else:
            table_alias = get_table_alias(step.query.from_table, self.context.get('database'))

            # TODO for information_schema we have 'database' = 'mindsdb'

            # fill params
            def fill_params(node, **kwargs):
                if isinstance(node, Parameter):
                    rs = self.steps_data[node.value.step_num]
                    items = [Constant(i[0]) for i in rs.get_records_raw()]
                    return Tuple(items)

            query_traversal(query, fill_params)

            query, context_callback = query_context_controller.handle_db_context_vars(query, dn, self.session)

            data, columns_info = dn.query(
                query=query,
                session=self.session
            )

            if context_callback:
                context_callback(data, columns_info)

        result = ResultSet()
        for column in columns_info:
            result.add_column(Column(
                name=column['name'],
                type=column.get('type'),
                table_name=table_alias[1],
                table_alias=table_alias[2],
                database=table_alias[0]
            ))
        result.add_records(data)

        return result
