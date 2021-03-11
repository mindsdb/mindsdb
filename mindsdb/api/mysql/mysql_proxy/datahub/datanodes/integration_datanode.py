from moz_sql_parser import format
import time

from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.datanode import DataNode
from mindsdb.interfaces.datastore.datastore import DataStore


class IntegrationDataNode(DataNode):
    type = 'integration'

    def __init__(self, config, integration_name):
        self.config = config
        self.integration_name = integration_name
        self.default_store = DataStore()

    def getType(self):
        return self.type

    def getTables(self):
        return []

    def hasTable(self, tableName):
        return True

    def getTableColumns(self, tableName):
        return []

    def select(self, table=None, columns=None, where=None, where_data=None, order_by=None, group_by=None, came_from=None):
        if isinstance(where, dict):
            where = [where]

        if isinstance(where, list):
            for el in where:
                if isinstance(el, dict):
                    for key in el:
                        if isinstance(el[key], list) and len(el[key]) > 0 and isinstance(el[key][0], str) and '.' in el[key][0]:
                            el[key][0] = el[key][0][el[key][0].find('.') + 1:]
            where = {'and': where}

        query = format({"from": table, 'select': columns, "where": where})

        ds, ds_name = self.default_store.save_datasource(f'temp_ds_{int(time.time()*100)}', self.integration_name, {'query': query})
        dso = self.default_store.get_datasource_obj(ds_name)
        data = dso.df.T.to_dict().values()
        self.default_store.delete_datasource(ds_name)

        return data
