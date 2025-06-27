from mindsdb_sql_parser.ast import (
    BinaryOperation,
    Identifier,
    Constant,
    Update,
)
from mindsdb.api.executor.planner.steps import UpdateToTable
from mindsdb.integrations.utilities.query_traversal import query_traversal

from mindsdb.api.executor.sql_query.result_set import ResultSet
from mindsdb.api.executor.exceptions import WrongArgumentError

from .base import BaseStepCall


class UpdateToTableCall(BaseStepCall):

    bind = UpdateToTable

    def call(self, step):
        if len(step.table.parts) > 1:
            integration_name = step.table.parts[0]
            table_name_parts = step.table.parts[1:]
        else:
            integration_name = self.context['database']
            table_name_parts = step.table.parts

        dn = self.session.datahub.get(integration_name)

        result_step = step.dataframe

        params_map_index = []

        if step.update_command.keys is not None:
            result_data = self.steps_data[result_step.result.step_num]

            where = None
            update_columns = {}

            key_columns = [i.to_string() for i in step.update_command.keys]
            if len(key_columns) == 0:
                raise WrongArgumentError('No key columns in update statement')
            for col in result_data.columns:
                name = col.name
                value = Constant(None)

                if name in key_columns:
                    # put it to where

                    condition = BinaryOperation(
                        op='=',
                        args=[Identifier(name), value]
                    )
                    if where is None:
                        where = condition
                    else:
                        where = BinaryOperation(
                            op='and',
                            args=[where, condition]
                        )
                else:
                    # put to update
                    update_columns[name] = value

                params_map_index.append([name, value])

            if len(update_columns) is None:
                raise WrongArgumentError(f'No columns for update found in: {result_data.columns}')

            update_query = Update(
                table=Identifier(parts=table_name_parts),
                update_columns=update_columns,
                where=where
            )

        else:
            # make command
            update_query = Update(
                table=Identifier(parts=table_name_parts),
                update_columns=step.update_command.update_columns,
                where=step.update_command.where
            )

            if result_step is None:
                # run as is
                response = dn.query(query=update_query, session=self.session)
                return ResultSet(affected_rows=response.affected_rows)
            result_data = self.steps_data[result_step.result.step_num]

            # link nodes with parameters for fast replacing with values
            input_table_alias = step.update_command.from_select_alias
            if input_table_alias is None:
                raise WrongArgumentError('Subselect in update requires alias')

            def prepare_map_index(node, is_table, **kwargs):
                if isinstance(node, Identifier) and not is_table:
                    # is input table field
                    if node.parts[0] == input_table_alias.parts[0]:
                        node2 = Constant(None)
                        param_name = node.parts[-1]
                        params_map_index.append([param_name, node2])
                        # replace node with constant
                        return node2
                    elif node.parts[0] == table_name_parts[0]:
                        # remove updated table alias
                        node.parts = node.parts[1:]

            # do mapping
            query_traversal(update_query, prepare_map_index)

        # check all params is input data:
        data_header = [col.alias for col in result_data.columns]

        for param_name, _ in params_map_index:
            if param_name not in data_header:
                raise WrongArgumentError(f'Field {param_name} not found in input data. Input fields: {data_header}')

        # perform update
        for row in result_data.get_records():
            # run update from every row from input data

            # fill params:
            for param_name, param in params_map_index:
                param.value = row[param_name]

            response = dn.query(query=update_query, session=self.session)
        return ResultSet(affected_rows=response.affected_rows)
