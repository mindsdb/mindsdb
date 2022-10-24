from copy import deepcopy

import numpy as np

from sqlalchemy.types import (
    Integer, Float, Text
)
from mindsdb_sql.parser.ast import (
    BinaryOperation,
    Identifier,
    Constant
)

from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.datanode import DataNode
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
from mindsdb.api.mysql.mysql_proxy.datahub.classes.tables_row import TablesRow, TABLES_ROW_TYPE


class ProjectDataNode(DataNode):
    type = 'project'

    def __init__(self, project, integration_controller, information_schema):
        self.project = project
        self.integration_controller = integration_controller
        self.information_schema = information_schema

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
        predictions = handler.predict(model_name, data, project_name=self.project.name)
        return predictions

    def query(self, query=None, native_query=None):
        # is it query to 'models' or 'models_versions'?
        query_table = query.from_table.parts[0]
        if query_table in ('models', 'models_versions'):
            new_query = deepcopy(query)
            project_filter = BinaryOperation('=', args=[
                Identifier('project'),
                Constant(self.project.name)
            ])
            if new_query.where is None:
                new_query.where = project_filter
            else:
                new_query.where = BinaryOperation('and', args=[
                    new_query.where,
                    project_filter
                ])
            data, columns_info = self.information_schema.query(new_query)
            return data, columns_info

        # TODO views

        raise Exception(f"Unknown table '{query_table}'")
