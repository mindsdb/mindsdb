from mindsdb_sql.parser.ast import (
    Identifier,
    Constant,
    Select,
    Star,
)
from mindsdb_sql.planner.steps import (
    GetPredictorColumns,
    GetTableColumns,
)

from mindsdb.api.executor.sql_query.result_set import ResultSet, Column

from .base import BaseStepCall


class GetPredictorColumnsCall(BaseStepCall):

    bind = GetPredictorColumns

    def call(self, step):

        mindsdb_database_name = 'mindsdb'

        predictor_name = step.predictor.parts[-1]
        dn = self.session.datahub.get(mindsdb_database_name)
        columns = dn.get_table_columns(predictor_name)

        data = ResultSet()
        for column_name in columns:
            data.add_column(Column(
                name=column_name,
                table_name=predictor_name,
                database=mindsdb_database_name
            ))
        return data


class GetTableColumnsCall(BaseStepCall):

    bind = GetTableColumns

    def call(self, step):

        table = step.table
        dn = self.session.datahub.get(step.namespace)
        ds_query = Select(from_table=Identifier(table), targets=[Star()], limit=Constant(0))

        data, columns_info = dn.query(ds_query, session=self.session)

        data = ResultSet()
        for column in columns_info:
            data.add_column(Column(
                name=column['name'],
                type=column.get('type'),
                table_name=table,
                database=self.context.get('database')
            ))
        return data
