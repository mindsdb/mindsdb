from typing import List

import pandas as pd

from mindsdb.integrations.handlers.confluence_handler.confluence_api_client import ConfluenceAPIClient
from mindsdb.integrations.libs.api_handler import APIResource
from mindsdb.integrations.utilities.sql_utils import (
    FilterCondition,
    FilterOperator,
    SortColumn
)
from mindsdb.utilities import log


logger = log.getLogger(__name__)


class ConfluencePagesTable(APIResource):
    """
    The table abstraction for the 'pages' resource of the Confluence API.
    """
    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[str] = None,
        **kwargs
    ):
        """
        Executes a parsed SELECT SQL query on the 'pages' resource of the Confluence API.

        Args:
            conditions (List[FilterCondition]): The list of parsed filter conditions.
            limit (int): The maximum number of records to return.
            sort (List[SortColumn]): The list of parsed sort columns.
            targets (List[str]): The list of target columns to return.
        """
        pages = []
        client: ConfluenceAPIClient = self.handler.connect()

        space_id, page_ids = None, None
        for condition in conditions:
            if condition.column == "spaceId":
                if condition.op == FilterOperator.EQUAL:
                    space_id = condition.value

                else:
                    raise ValueError(
                        f"Unsupported operator '{condition.op}' for column 'spaceId'."
                    )

            if condition.column == "id":
                if condition.op == FilterOperator.EQUAL:
                    page_ids = [condition.value]

                elif condition.op == FilterOperator.IN:
                    page_ids = condition.value

                else:
                    raise ValueError(
                        f"Unsupported operator '{condition.op}' for column 'page_id'."
                    )
            
        if page_ids:
            for page_id in page_ids:
                page = client.get_page_by_id(page_id)
                pages.append(page)

        elif space_id:
            pages = client.get_pages_in_space(space_id)

        else:
            pages = client.get_pages()

        pages_df = pd.json_normalize(pages, sep="_")
        pages_df = pages_df[self.get_columns()]

        return pages_df
    
    def get_columns(self) -> List[str]:
        """
        Retrieves the attributes (columns) of the 'chat messages' resource.

        Returns:
            List[Text]: A list of attributes (columns) of the 'chat messages' resource.
        """
        return [
            "id",
            "status",
            "title",
            "spaceId",
            "parentId",
            "parentType",
            "position",
            "authorId",
            "ownerId",
            "lastOwnerId",
            "createdAt",
            "version_createdAt",
            "version_message",
            "version_number",
            "version_minorEdit",
            "version_authorId",
            "body_storage_representation",
            "body_storage_value",
            "_links_webui",
            "_links_editui",
            "_links_tinyui",
        ]

