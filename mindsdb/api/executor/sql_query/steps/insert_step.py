from mindsdb_sql_parser.ast import (
    Identifier,
)

from mindsdb.api.executor.planner.steps import SaveToTable, InsertToTable, CreateTableStep
from mindsdb.api.executor.sql_query.result_set import ResultSet, Column
from mindsdb.api.executor.exceptions import NotSupportedYet, LogicError
from mindsdb.integrations.libs.response import INF_SCHEMA_COLUMNS_NAMES

from .base import BaseStepCall


class InsertToTableCall(BaseStepCall):
    bind = InsertToTable

    def call(self, step):
        is_replace = False
        is_create = False

        if type(step) == SaveToTable:
            is_create = True

            if step.is_replace:
                is_replace = True

        if len(step.table.parts) > 1:
            integration_name = step.table.parts[0]
            table_name = Identifier(parts=step.table.parts[1:])
        else:
            integration_name = self.context["database"]
            table_name = step.table

        dn = self.session.datahub.get(integration_name)

        if hasattr(dn, "create_table") is False:
            raise NotSupportedYet(f"Creating table in '{integration_name}' is not supported")

        if step.dataframe is not None:
            data = self.steps_data[step.dataframe.step_num]
        elif step.query is not None:
            data = ResultSet()
            if step.query.columns is None:
                # Is query like: INSERT INTO table VALUES (...)
                table_columns_df = dn.get_table_columns_df(str(table_name))
                columns_names = table_columns_df[INF_SCHEMA_COLUMNS_NAMES.COLUMN_NAME].to_list()
                for column_name in columns_names:
                    data.add_column(Column(name=column_name))
            else:
                # Is query like: INSERT INTO table (column_name, ...) VALUES (...)
                for col in step.query.columns:
                    data.add_column(Column(name=col.name))

            records = []
            for row in step.query.values:
                record = []
                for v in row:
                    if isinstance(v, Identifier) and v.parts[0] == "None":
                        # Allow explicitly inserting NULL values.
                        record.append(None)
                        continue
                    # Value is a constant
                    record.append(v.value)
                records.append(record)

            data.add_raw_values(records)
        else:
            raise LogicError(f"Data not found for insert: {step}")

        #  del 'service' columns
        for col in data.find_columns("__mindsdb_row_id"):
            data.del_column(col)
        for col in data.find_columns("__mdb_forecast_offset"):
            data.del_column(col)

        # region del columns filtered at projection step
        columns_list = self.get_columns_list()
        if columns_list is not None:
            filtered_column_names = [x.name for x in columns_list]
            for col in data.columns:
                if col.name.startswith("predictor."):
                    continue
                if col.name in filtered_column_names:
                    continue
                data.del_column(col)
        # endregion

        # drop double names
        col_names = set()
        for col in data.columns:
            if col.alias in col_names:
                data.del_column(col)
            else:
                col_names.add(col.alias)

        response = dn.create_table(
            table_name=table_name, result_set=data, is_replace=is_replace, is_create=is_create, params=step.params
        )
        return ResultSet(affected_rows=response.affected_rows)


class SaveToTableCall(InsertToTableCall):
    bind = SaveToTable


class CreateTableCall(BaseStepCall):
    bind = CreateTableStep

    def call(self, step):
        if len(step.table.parts) > 1:
            integration_name = step.table.parts[0]
            table_name = Identifier(parts=step.table.parts[1:])
        else:
            integration_name = self.context["database"]
            table_name = step.table

        dn = self.session.datahub.get(integration_name)

        dn.create_table(table_name=table_name, columns=step.columns, is_replace=step.is_replace, is_create=True)
        return ResultSet()
