from pandas import DataFrame

from mindsdb.api.executor.datahub.classes.response import DataHubResponse


class DataNode:
    type = 'meta'

    def __init__(self):
        pass

    def get_type(self):
        return self.type

    def get_tables(self):
        pass

    def get_table_columns_df(self, table_name: str, schema_name: str | None = None) -> DataFrame:
        pass

    def get_table_columns_names(self, table_name: str, schema_name: str | None = None) -> list[str]:
        pass

    def query(self, query=None, native_query=None, session=None) -> DataHubResponse:
        pass
