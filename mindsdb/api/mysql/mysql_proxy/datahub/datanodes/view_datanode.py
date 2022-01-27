from pandas import DataFrame as DF
from mindsdb_sql import parse_sql

from mindsdb.api.mysql.mysql_proxy.classes.sql_query import get_all_tables
from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.datanode import DataNode
from mindsdb.api.mysql.mysql_proxy.utilities.sql import query_df


class ViewDataNode(DataNode):
    type = 'view'

    def __init__(self, view_interface, datasource_interface):
        self.view_interface = view_interface
        self.datasource_interface = datasource_interface

    def get_tables(self):
        views = self.view_interface.get_all()
        return list(views.keys())

    def has_table(self, table):
        views = self.view_interface.get_all()
        return table in views

    def get_table_columns(self, table):
        # TODO
        ds = self.datastore.get_datasource(table)
        return [x['name'] for x in ds['columns']]

    def select(self, query):
        # TODO
        if isinstance(query, str):
            query = parse_sql(query, dialect='mysql')
        query_str = str(query)

        table = query.from_table.parst[-1]
        view_metadata = self.view_interface.get(table)

        datasource = self.datasource_interface()

        # if ds_name is None:
        #     ds_name = data_store.get_vacant_name(predictor_name)

        # ds = data_store.save_datasource(ds_name, integration_name, {'query': struct['select']})
        # ds_data = data_store.get_datasource(ds_name)

        # query_tables = get_all_tables(query)

        # if len(query_tables) != 1:
        #     raise Exception(f'Only one table can be used in query to information_schema: {query}')

        # data = self.datastore.get_data(query_tables[0], where=None, limit=None, offset=None)
        # data_df = DF(data['data'])
        # result = query_df(data_df, query)
        # return result.to_dict(orient='records'), result.columns.to_list()
        return None
