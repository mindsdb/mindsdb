import pandas as pd
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender

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

    def select(self, query):
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
