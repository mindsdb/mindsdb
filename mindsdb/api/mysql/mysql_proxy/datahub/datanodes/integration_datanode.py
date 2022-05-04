import pandas as pd
from sqlalchemy.types import (
    Integer, Float, Text
)
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb_sql.parser.ast import Insert, Identifier, Constant, CreateTable, TableColumn, DropTables

from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.datanode import DataNode
from mindsdb.utilities.log import log


class IntegrationDataNode(DataNode):
    type = 'integration'

    def __init__(self, integration_name, data_store, ds_type):
        self.integration_name = integration_name
        self.data_store = data_store
        self.ds_type = ds_type

    def get_type(self):
        return self.type

    def get_tables(self):
        return []

    def has_table(self, tableName):
        return True

    def get_table_columns(self, tableName):
        return []

    def create_table(self, table_name_parts, columns, data):
        dso, _creation_info = self.data_store.create_datasource(self.integration_name, {'query': 'select 1'})
        if hasattr(dso, 'execute') is False:
            raise Exception(f"Cant create table in {self.integration_name}")
        if self.ds_type not in ('postgres', 'mysql', 'mariadb'):
            raise Exception(f'At this moment is no possible to create table in "{self.ds_type}"')

        if self.ds_type in ('mysql', 'mariadb'):
            dialect = 'mysql'
        elif self.ds_type == 'postgres':
            dialect = 'postgres'
        renderer = SqlalchemyRender(dialect)

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
        create_table_ast = CreateTable(
            name=Identifier(parts=table_name_parts),
            columns=table_columns,
            is_replace=True
        )

        create_query_str = renderer.get_string(create_table_ast, with_failback=False)

        drop_ast = DropTables(
            tables=[Identifier(parts=table_name_parts)],
            if_exists=True
        )

        drop_query_str = renderer.get_string(drop_ast, with_failback=False)
        dso.execute(drop_query_str)
        dso.execute(create_query_str)

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

        query_str = renderer.get_string(insert_ast, with_failback=False)
        dso.execute(query_str)

    def select(self, query):
        if isinstance(query, str):
            query_str = query
        else:
            if self.ds_type in ('postgres', 'snowflake'):
                dialect = 'postgres'
            else:
                dialect = 'mysql'
            render = SqlalchemyRender(dialect)
            try:
                query_str = render.get_string(query, with_failback=False)
            except Exception as e:
                log.error(f"Exception during query casting to '{dialect}' dialect. Query: {query}. Error: {e}")
                query_str = render.get_string(query, with_failback=True)

        dso, _creation_info = self.data_store.create_datasource(self.integration_name, {'query': query_str})
        data = dso.df.to_dict(orient='records')
        column_names = list(dso.df.columns)

        for column_name in column_names:
            if pd.core.dtypes.common.is_datetime_or_timedelta_dtype(dso.df[column_name]):
                pass_data = dso.df[column_name].dt.to_pydatetime()
                for i, rec in enumerate(data):
                    rec[column_name] = pass_data[i].timestamp()

        if len(column_names) == 0:
            column_names = ['dataframe_is_empty']

        columns_info = [
            {
                'name': k,
                'type': v
            }
            for k, v in dso.df.dtypes.items()
        ]

        return data, columns_info
