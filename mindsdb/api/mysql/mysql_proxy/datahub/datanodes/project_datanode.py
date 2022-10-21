import numpy as np

from sqlalchemy.types import (
    Integer, Float, Text
)
from mindsdb_sql.parser.ast import Insert, Identifier, CreateTable, TableColumn, DropTables

from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.datanode import DataNode
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
from mindsdb.api.mysql.mysql_proxy.datahub.classes.tables_row import TablesRow, TABLES_ROW_TYPE


class ProjectDataNode(DataNode):
    type = 'project'

    def __init__(self, project, integration_controller):
        self.project = project
        self.integration_controller = integration_controller

    def get_type(self):
        return self.type

    def get_tables(self):
        tables = self.project.get_tables()
        tables = [{'TABLE_NAME': x} for x in tables.keys()]
        result = [TablesRow.from_dict(row) for row in tables]
        return result

    def has_table(self, table_name):
        tables = self.project.get_tables()
        return table_name in tables

    def get_table_columns(self, table_name):
        return self.project.get_columns(table_name)

    def predict(self, model_name: str, data) -> list:
        project_tables = self.project.get_tables()
        predictor_table_meta = project_tables[model_name]
        handler = self.integration_controller.get_handler(predictor_table_meta['engine_name'])
        predictions = handler.predict(model_name, data)
        return predictions

    def query(self, query=None, native_query=None):
        # TODO
        if query is not None:
            result = self.integration_handler.query(query)
        else:
            # try to fetch native query
            result = self.integration_handler.native_query(native_query)

        if result.type == RESPONSE_TYPE.ERROR:
            raise Exception(result.error_message)
        if result.type == RESPONSE_TYPE.QUERY:
            return result.query, None
        if result.type == RESPONSE_TYPE.OK:
            return

        df = result.data_frame
        df = df.replace({np.nan: None})
        columns_info = [
            {
                'name': k,
                'type': v
            }
            for k, v in df.dtypes.items()
        ]
        data = df.to_dict(orient='records')
        return data, columns_info
