from mindsdb.api.executor.datahub.classes.response import DataHubResponse


class DataNode:
    type = 'meta'

    def __init__(self):
        pass

    def get_type(self):
        return self.type

    def get_tables(self):
        pass

    def get_table_columns(self, tableName, schema_name=None):
        pass

    def query(self, query=None, native_query=None, session=None) -> DataHubResponse:
        return []
