import copy

from mindsdb_sql_parser.ast import (
    Identifier,
    Constant,
    Delete,
    Parameter,
    Tuple,
)
from mindsdb.integrations.utilities.query_traversal import query_traversal
from mindsdb.api.executor.planner.steps import DeleteStep

from mindsdb.api.executor.sql_query.result_set import ResultSet

from .base import BaseStepCall


class DeleteStepCall(BaseStepCall):

    bind = DeleteStep

    def call(self, step):
        if len(step.table.parts) > 1:
            integration_name = step.table.parts[0]
            table_name_parts = step.table.parts[1:]
        else:
            integration_name = self.context['database']
            table_name_parts = step.table.parts

        dn = self.session.datahub.get(integration_name)

        # make command
        query = Delete(
            table=Identifier(parts=table_name_parts),
            where=copy.deepcopy(step.where),
        )

        # fill params
        def fill_params(node, **kwargs):
            if isinstance(node, Parameter):
                rs = self.steps_data[node.value.step_num]
                items = [Constant(i) for i in rs.get_column_values(col_idx=0)]
                return Tuple(items)

        query_traversal(query.where, fill_params)

        response = dn.query(query=query, session=self.session)
        return ResultSet(affected_rows=response.affected_rows)
