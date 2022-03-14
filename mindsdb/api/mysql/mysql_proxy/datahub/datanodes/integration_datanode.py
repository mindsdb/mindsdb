import pandas as pd
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb_sql.parser.ast import Insert, Identifier, Constant

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

    def create_table(self, table_name, columns, data):
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
        query_str = renderer.get_string(query, with_failback=False)
        dso.execute(query_str)

        values = []
        for row in data:
            values.append([Constant(x) for x in row])
        expected_ast = Insert(
            table=Identifier(table_name),
            columns=[Identifier(x) for x in columns],
            values=values
        )
        query_str = renderer.get_string(expected_ast, with_failback=False)
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

        return data, column_names
