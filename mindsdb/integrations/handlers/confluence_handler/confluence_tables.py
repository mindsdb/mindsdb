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


class ConfluenceSpacesTable(APIResource):
    """
    The table abstraction for the 'spaces' resource of the Confluence API.
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
        Executes a parsed SELECT SQL query on the 'spaces' resource of the Confluence API.

        Args:
            conditions (List[FilterCondition]): The list of parsed filter conditions.
            limit (int): The maximum number of records to return.
            sort (List[SortColumn]): The list of parsed sort columns.
            targets (List[str]): The list of target columns to return.
        """
        spaces = []
        client: ConfluenceAPIClient = self.handler.connect()

        ids, keys, type, statuses = None, None, None, None
        for condition in conditions:
            if condition.column == "id":
                if condition.op == FilterOperator.EQUAL:
                    ids = [condition.value]

                elif condition.op == FilterOperator.IN:
                    ids = condition.value

                else:
                    raise ValueError(
                        f"Unsupported operator '{condition.op}' for column 'id'."
                    )
                
                condition.applied = True

            if condition.column == "key":
                if condition.op == FilterOperator.EQUAL:
                    keys = [condition.value]

                elif condition.op == FilterOperator.IN:
                    keys = condition.value

                else:
                    raise ValueError(
                        f"Unsupported operator '{condition.op}' for column 'key'."
                    )
                
                condition.applied = True

            if condition.column == "type":
                if condition.op == FilterOperator.EQUAL:
                    type = condition.value

                else:
                    raise ValueError(
                        f"Unsupported operator '{condition.op}' for column 'type'."
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

        spaces = client.get_spaces(
            ids=ids,
            keys=keys,
            type=type,
            status=statuses,
            limit=limit
        )

        spaces_df = pd.json_normalize(spaces, sep="_")
        spaces_df = spaces_df[self.get_columns()]

        return spaces_df
    
    def get_columns(self) -> List[str]:
        """
        Retrieves the attributes (columns) of the 'spaces' resource.

        Returns:
            List[Text]: A list of attributes (columns) of the 'spaces' resource.
        """
        return [
            "id",
            "key",
            "name",
            "type",
            "description_view_representation",
            "description_view_value",
            "status",
            "authorId",
            "createdAt",
            "homepageId",
            "_links_webui",
            "currentActiveAlias",
        ]


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


class ConfluenceBlogPostsTable(APIResource):
    """
    The table abstraction for the 'blogposts' resource of the Confluence API.
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
        Executes a parsed SELECT SQL query on the 'blogposts' resource of the Confluence API.

        Args:
            conditions (List[FilterCondition]): The list of parsed filter conditions.
            limit (int): The maximum number of records to return.
            sort (List[SortColumn]): The list of parsed sort columns.
            targets (List[str]): The list of target columns to return.
        """
        blogposts = []
        client: ConfluenceAPIClient = self.handler.connect()

        post_ids, space_ids, statuses, title = None, None, None, None
        for condition in conditions:
            if condition.column == "id":
                if condition.op == FilterOperator.EQUAL:
                    post_ids = [condition.value]

                elif condition.op == FilterOperator.IN:
                    post_ids = condition.value

                else:
                    raise ValueError(
                        f"Unsupported operator '{condition.op}' for column 'id'."
                    )
                
                condition.applied = True

            if condition.column == "spaceKey":
                if condition.op == FilterOperator.EQUAL:
                    space_ids = [condition.value]

                elif condition.op == FilterOperator.IN:
                    space_ids = condition.value

                else:
                    raise ValueError(
                        f"Unsupported operator '{condition.op}' for column 'spaceKey'."
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

        blogposts = client.get_blogposts(
            post_ids=post_ids,
            space_ids=space_ids,
            statuses=statuses,
            title=title,
            limit=limit
        )

        blogposts_df = pd.json_normalize(blogposts, sep="_")
        blogposts_df = blogposts_df[self.get_columns()]

        return blogposts_df

    def get_columns(self) -> List[str]:
        """
        Retrieves the attributes (columns) of the 'blogposts' resource.

        Returns:
            List[Text]: A list of attributes (columns) of the 'blogposts' resource.
        """
        return [
            "id",
            "status",
            "title",
            "spaceId",
            "authorId",
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