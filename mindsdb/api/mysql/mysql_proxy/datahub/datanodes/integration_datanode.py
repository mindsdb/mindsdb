import pandas as pd
from sqlalchemy.types import (
    Integer, Float, Text
)
from mindsdb_sql.parser.ast import Insert, Identifier, CreateTable, TableColumn, DropTables

from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.datanode import DataNode

from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
from mindsdb.api.mysql.mysql_proxy.datahub.classes.tables_row import TablesRow, TABLES_ROW_TYPE


class IntegrationDataNode(DataNode):
    type = 'integration'

    def __init__(self, integration_name, ds_type, integration_controller):
        self.integration_name = integration_name
        self.ds_type = ds_type
        self.integration_controller = integration_controller
        self.integration_handler = self.integration_controller.get_handler(self.integration_name)

    def get_type(self):
        return self.type

    def get_tables(self):
        response = self.integration_handler.get_tables()
        if response.type is RESPONSE_TYPE.TABLE:
            result_dict = response.data_frame.to_dict(orient='records')
            result = []
            for row in result_dict:
                result.append(TablesRow.from_dict(row))
            return result
        else:
            raise Exception(f"Can't get tables: {response.error_message}")

    def has_table(self, tableName):
        return True

    def get_table_columns(self, tableName):
        return []

    def create_table(self, table_name_parts, columns, data, is_replace=False, is_create=False):
        # is_create - create table
        # is_replace - drop table if exists
        # is_create==False and is_replace==False: just insert

        table_columns_meta = []
        table_columns = []
        for table in columns:
            for column in columns[table]:
                column_type = None
                for row in data:
                    column_value = row[table][column]
                    if isinstance(column_value, int):
                        column_type = Integer
                    elif isinstance(column_value, float):
                        column_type = Float
                    elif isinstance(column_value, str):
                        column_type = Text
                column_type = column_type or Text
                table_columns.append(
                    TableColumn(
                        name=column[-1],
                        type=column_type
                    )
                )
                table_columns_meta.append({
                    'table': table,
                    'name': column,
                    'type': column_type
                })

        if is_replace:
            # drop
            drop_ast = DropTables(
                tables=[Identifier(parts=table_name_parts)],
                if_exists=True
            )
            result = self.integration_handler.query(drop_ast)
            if result.type == RESPONSE_TYPE.ERROR:
                raise Exception(result.error_message)
            is_create = True

        if is_create:
            create_table_ast = CreateTable(
                name=Identifier(parts=table_name_parts),
                columns=table_columns,
                is_replace=True
            )

            result = self.integration_handler.query(create_table_ast)
            if result.type == RESPONSE_TYPE.ERROR:
                raise Exception(result.error_message)

        insert_columns = [Identifier(parts=[x['name'][-1]]) for x in table_columns_meta]
        formatted_data = []
        for row in data:
            new_row = []
            for column_meta in table_columns_meta:
                value = row[column_meta['table']][column_meta['name']]
                python_type = str
                if column_meta['type'] == Integer:
                    python_type = int
                elif column_meta['type'] == Float:
                    python_type = float

                try:
                    value = python_type(value) if value is not None else value
                except Exception:
                    pass
                new_row.append(value)
            formatted_data.append(new_row)

        insert_ast = Insert(
            table=Identifier(parts=table_name_parts),
            columns=insert_columns,
            values=formatted_data
        )

        result = self.integration_handler.query(insert_ast)
        if result.type == RESPONSE_TYPE.ERROR:
            raise Exception(result.error_message)


    def query(self, query):
        result = self.integration_handler.query(query)

        if result.type == RESPONSE_TYPE.ERROR:
            raise Exception(result.error_message)
        if result.type == RESPONSE_TYPE.QUERY:
            return result.query, None
        if result.type == RESPONSE_TYPE.OK:
            return

        df = result.data_frame
        columns_info = [
            {
                'name': k,
                'type': v
            }
            for k, v in df.dtypes.items()
        ]
        data = df.to_dict(orient='records')
        return data, columns_info
