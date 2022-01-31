from pandas import DataFrame as DF
from mindsdb_sql import parse_sql

from mindsdb.api.mysql.mysql_proxy.classes.sql_query import get_all_tables
from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.datanode import DataNode
from mindsdb.api.mysql.mysql_proxy.utilities.sql import query_df


class ViewDataNode(DataNode):
    type = 'view'

    def __init__(self, view_interface, datasource_interface, data_store):
        self.view_interface = view_interface
        self.datasource_interface = datasource_interface
        self.data_store = data_store

    def get_tables(self):
        views = self.view_interface.get_all()
        return list(views.keys())

    def has_table(self, table):
        views = self.view_interface.get_all()
        return table in views

    def get_table_columns(self, table):
        # TODO
        raise Exception('not iomplemented')

    def select(self, query):
        if isinstance(query, str):
            query = parse_sql(query, dialect='mysql')
        query_str = str(query)

        table = query.from_table.parts[-1]
        view_metadata = self.view_interface.get(name=table)

        datasource = self.datasource_interface.get_db_integration_by_id(view_metadata['datasource_id'])
        datasource_name = datasource['name']

        dataset_name = self.data_store.get_vacant_name(table)
        self.data_store.save_datasource(dataset_name, datasource_name, {'query': view_metadata['query']})
        try:
            dataset_object = self.data_store.get_datasource_obj(dataset_name)
            data_df = dataset_object.df
        finally:
            self.data_store.delete_datasource(dataset_name)

        result = query_df(data_df, query_str)
        return result.to_dict(orient='records'), result.columns.to_list()
