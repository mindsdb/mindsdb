from io import BytesIO
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
    

class FileTable(APIResource):

    def list(self, targets: List[str] = None, table_name=None, *args, **kwargs) -> pd.DataFrame:
        client = self.handler.connect()

        file_content = BytesIO(client.get_item_content(table_name))
        file_extension = table_name.split(".")[-1]

        # Read the file content based and return a DataFrame based on the file extension.
        if file_extension == "csv":
            df = pd.read_csv(file_content)

        elif file_extension == "tsv":
            df = pd.read_csv(file_content, sep="\t")

        elif file_extension == "json":
            df = pd.read_json(file_content)

        elif file_extension == "parquet":
            df = pd.read_parquet(file_content)
            
        return df