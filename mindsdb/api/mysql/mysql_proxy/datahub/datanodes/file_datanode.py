from pandas import DataFrame as DF

from mindsdb.api.mysql.mysql_proxy.classes.sql_query import get_all_tables
from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.datanode import DataNode
from mindsdb.api.mysql.mysql_proxy.utilities.sql import query_df


class FileDataNode(DataNode):
    type = 'file'

    def __init__(self, data_store):
        self.data_store = data_store

    def get_tables(self):
        return self.data_store.get_files_names()

    def has_table(self, table):
        return table in self.get_tables()

    def get_table_columns(self, table):
        file_meta = self.data_store.get_file_meta(table)
        return [x['name'] for x in file_meta['columns']]

    def select(self, query):
        query_tables = get_all_tables(query)

        if len(query_tables) != 1:
            raise Exception(f'Only one table can be used in query to information_schema: {query}')

        data = self.data_store.get_data(query_tables[0], where=None, limit=None, offset=None)
        data_df = DF(data['data'])
        result = query_df(data_df, query)

        columns_info = [
            {
                'name': k,
                'type': v
            }
            for k, v in result.dtypes.items()
        ]

        return result.to_dict(orient='records'), columns_info
