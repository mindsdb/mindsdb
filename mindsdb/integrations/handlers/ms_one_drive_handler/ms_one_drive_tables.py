from typing import List, Text

import pandas as pd

from mindsdb.integrations.libs.api_handler import APIResource
from mindsdb.integrations.utilities.sql_utils import (
    FilterCondition, 
    SortColumn
)


class ListFilesTable(APIResource):

    def list(self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[Text] = None,
        **kwargs
    ):
        client = self.handler.connect()
        files = client.get_root_drive_items()

        data = []
        for file in files:
            item = {
                "name": file["name"]
            }
            data.append(item)

        df = pd.DataFrame(data)
        return df

    def get_columns(self):
        return ["name"]