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

        page_ids, space_ids, statuses, title = None, None, None, None
        for condition in conditions:
            if condition.column == "id":
                if condition.op == FilterOperator.EQUAL:
                    page_ids = [condition.value]

                elif condition.op == FilterOperator.IN:
                    page_ids = condition.value

                else:
                    raise ValueError(
                        f"Unsupported operator '{condition.op}' for column 'page_id'."
                    )
                
                condition.applied = True

            if condition.column == "spaceId":
                if condition.op == FilterOperator.EQUAL:
                    space_ids = [condition.value]

                elif condition.op == FilterOperator.IN:
                    space_ids = condition.value

                else:
                    raise ValueError(
                        f"Unsupported operator '{condition.op}' for column 'spaceId'."
                    )
                
                condition.applied = True

            if condition.column == "status":
                if condition.op == FilterOperator.EQUAL:
                    statuses = [condition.value]

                elif condition.op == FilterOperator.IN:
                    statuses = condition.value

                else:
                    raise ValueError(
                        f"Unsupported operator '{condition.op}' for column 'status'."
                    )
                
                condition.applied = True

            if condition.column == "title":
                if condition.op == FilterOperator.EQUAL:
                    title = condition.value

                else:
                    raise ValueError(
                        f"Unsupported operator '{condition.op}' for column 'title'."
                    )
                
                condition.applied = True

        pages = client.get_pages(
            page_ids=page_ids,
            space_ids=space_ids,
            statuses=statuses,
            title=title,
            limit=limit
        )

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

