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
        files = client.get_all_items()

        data = []
        for file in files:
            item = {
                "name": file["name"],
                "path": file["path"],
                "extension": file["name"].split(".")[-1]
            }

            # If the 'content' column is explicitly requested, fetch the content of the file.
            if targets and "content" in targets:
                item["content"] = client.get_item_content(file["path"])

            # If a SELECT * query is executed, i.e., targets is empty, set the content to None.
            elif not targets:
                item["content"] = None

            data.append(item)

        df = pd.DataFrame(data)
        return df

    def get_columns(self):
        return ["name", "path", "extension", "content"]