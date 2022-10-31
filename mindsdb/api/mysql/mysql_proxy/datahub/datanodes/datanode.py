class DataNode:
    type = 'meta'

    def __init__(self):
        pass

    def get_type(self):
        return self.type

    def get_tables(self):
        pass

    def has_table(self, tableName):
        pass

    def get_table_columns(self, tableName):
        pass

    def query(self, query=None, native_query=None, session=None):
        return []
