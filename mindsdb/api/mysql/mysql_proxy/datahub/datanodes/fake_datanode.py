from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.datanode import DataNode
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import TYPES
import random


class FakeDataNode(DataNode):
    ''' datasource for test
        returns randome data
    '''

    type = 'fake'

    tables = {}

    def __init__(self, tables={}):
        self.tables = tables

    def getTables(self):
        return list(self.tables.keys())

    def hasTable(self, tableName):
        tableNames = self.tables.keys()
        return tableName in tableNames

    def getTableColumns(self, tableName):
        return self.tables[tableName]

    def select(self, table=None, columns=None, where=None, where_data=None, order_by=None, group_by=None, came_from=None):
        if columns is None:
            columns = self.getTableColumns(table)
        result = {
            'columns': [],
            'data': []
        }
        result['columns'] = [{
            'name': column,
            'type': TYPES.MYSQL_TYPE_SHORT
        } for column in columns]
        for i in range(random.randint(5, 10)):
            row = [random.randint(0, 10) for column in columns]
            result['data'].append(row)

        return result
