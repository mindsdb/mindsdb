class DataNode:
    type = 'meta'

    def __init__(self):
        pass

    def getType(self):
        return self.type

    def getTables(self):
        pass

    def hasTable(self, tableName):
        pass

    def getTableColumns(self, tableName):
        pass

    def select(self, table=None, columns=None, where=None, where_data=None, order_by=None, group_by=None, came_from=None):
        return []
