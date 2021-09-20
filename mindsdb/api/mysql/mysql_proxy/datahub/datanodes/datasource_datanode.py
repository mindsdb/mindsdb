from mindsdb.api.mysql.mysql_proxy.classes.sql_query import get_all_tables
from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.datanode import DataNode


class DataSourceDataNode(DataNode):
    type = 'mindsdb-datasource'

    def __init__(self, data_store):
        self.datastore = data_store

    def getTables(self):
        dss = self.datastore.get_datasources()
        return [x['name'] for x in dss]

    def hasTable(self, table):
        return table in self.getTables()

    def getTableColumns(self, table):
        ds = self.datastore.get_datasource(table)
        return [x['name'] for x in ds['columns']]

    def select_query(self, query):
        query_tables = get_all_tables(query)

        if len(query_tables) != 1:
            raise Exception(f'Only one table can be used in query to information_schema: {query}')

        data = self.datastore.get_data(query_tables[0], where=None, limit=None, offset=None)
        return data['data']

    def select(self, table, columns=None, where=None, where_data=None, order_by=None, group_by=None, came_from=None):
        data = self.datastore.get_data(table, where=None, limit=None, offset=None)
        return data['data']
