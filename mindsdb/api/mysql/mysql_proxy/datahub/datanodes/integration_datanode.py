import pandas as pd

from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.datanode import DataNode


class IntegrationDataNode(DataNode):
    type = 'integration'

    def __init__(self, integration_name, data_store):
        self.integration_name = integration_name
        self.data_store = data_store

    def getType(self):
        return self.type

    def getTables(self):
        return []

    def hasTable(self, tableName):
        return True

    def getTableColumns(self, tableName):
        return []

    def select(self, query):
        sql_query = str(query)

        dso, _creation_info = self.data_store.create_datasource(self.integration_name, {'query': sql_query})
        data = dso.df.to_dict(orient='records')
        column_names = list(dso.df.columns)

        for column_name in column_names:
            if pd.core.dtypes.common.is_datetime_or_timedelta_dtype(dso.df[column_name]):
                pass_data = dso.df[column_name].dt.to_pydatetime()
                for i, rec in enumerate(data):
                    rec[column_name] = pass_data[i].timestamp()

        return data, column_names
