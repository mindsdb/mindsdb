from mindsdb_sql import parse_sql

from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.datanode import DataNode
from mindsdb.api.mysql.mysql_proxy.utilities.sql import query_df


class ViewDataNode(DataNode):
    type = 'view'

    def __init__(self, view_interface, integration_controller):
        self.view_interface = view_interface
        self.integration_controller = integration_controller

    def get_tables(self):
        views = self.view_interface.get_all()
        return list(views.keys())

    def has_table(self, table):
        views = self.view_interface.get_all()
        return table in views

    def get_table_columns(self, table):
        # TODO
        raise Exception('not iomplemented')

    def query(self, query):
        if isinstance(query, str):
            query = parse_sql(query, dialect='mysql')
        query_str = str(query)

        table = query.from_table.parts[-1]
        view_metadata = self.view_interface.get(name=table)

        integration = self.integration_controller.get_by_id(view_metadata['integration_id'])
        integration_name = integration['name']

        integration_handler = self.integration_controller.get_handler(integration_name)
        result = integration_handler.query(view_metadata['query'])
        data_df = result['data_frame']

        result = query_df(data_df, query_str)

        columns_info = [
            {
                'name': k,
                'type': v
            }
            for k, v in result.dtypes.items()
        ]

        return result.to_dict(orient='records'), columns_info
