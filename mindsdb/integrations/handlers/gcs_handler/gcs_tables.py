from mindsdb.integrations.libs.api_handler import APIResource
from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator
import pandas as pd
from typing import List


class ListFilesTable(APIResource):

    def list(self,
             targets: List[str] = None,
             conditions: List[FilterCondition] = None,
             limit: int = None,
             *args, **kwargs) -> pd.DataFrame:

        buckets = None
        for condition in conditions:
            if condition.column == 'bucket':
                if condition.op == FilterOperator.IN:
                    buckets = condition.value
                elif condition.op == FilterOperator.EQUAL:
                    buckets = [condition.value]
                condition.applied = True

        data = []
        for obj in self.handler.get_objects(limit=limit, buckets=buckets):
            path = obj['Key']
            path = path.replace('`', '')
            item = {
                'path': path,
                'bucket': obj['Bucket'],
                'name': path[path.rfind('/') + 1:],
                'extension': path[path.rfind('.') + 1:]
            }

            data.append(item)

        return pd.DataFrame(data=data, columns=self.get_columns())

    def get_columns(self) -> List[str]:
        return ["path", "name", "extension", "bucket", "content"]


class FileTable(APIResource):

    def list(self, targets: List[str] = None, table_name=None, *args, **kwargs) -> pd.DataFrame:
        return self.handler.read_as_table(table_name)

    def add(self, data, table_name=None):
        df = pd.DataFrame(data)
        return self.handler.add_data_to_table(table_name, df)
