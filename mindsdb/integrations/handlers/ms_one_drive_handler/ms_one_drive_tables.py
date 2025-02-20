from io import BytesIO
from typing import List, Text

import pandas as pd

from mindsdb.integrations.libs.api_handler import APIResource
from mindsdb.integrations.utilities.sql_utils import (
    FilterCondition,
    SortColumn
)

from mindsdb.integrations.utilities.files.file_reader import FileReader


class ListFilesTable(APIResource):
    """
    The table abstraction for querying the files (tables) in Microsoft OneDrive.
    """

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[Text] = None,
        **kwargs
    ):
        """
        Lists the files in Microsoft OneDrive.

        Args:
            conditions (List[FilterCondition]): The conditions to filter the files.
            limit (int): The maximum number of files to return.
            sort (List[SortColumn]): The columns to sort the files by.
            targets (List[Text]): The columns to return in the result.

        Returns:
            pd.DataFrame: The list of files in Microsoft OneDrive based on the specified clauses.
        """
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
    """
    The table abstraction for querying the content of a file (table) in Microsoft OneDrive.
    """

    def list(self, targets: List[str] = None, table_name=None, *args, **kwargs) -> pd.DataFrame:
        """
        Retrieves the content of the specified file (table) in Microsoft OneDrive.

        Args:
            targets (List[str]): The columns to return in the result.
            table_name (str): The name of the file (table) to retrieve.

        Returns:
            pd.DataFrame: The content of the specified file (table) in Microsoft OneDrive.
        """
        client = self.handler.connect()

        file_content = client.get_item_content(table_name)

        reader = FileReader(file=BytesIO(file_content), name=table_name)

        return reader.get_page_content()
