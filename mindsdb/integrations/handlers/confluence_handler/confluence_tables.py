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

        ids, keys, space_type, status = None, None, None, None
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
                    space_type = condition.value

                else:
                    raise ValueError(
                        f"Unsupported operator '{condition.op}' for column 'type'."
                    )

                condition.applied = True

            if condition.column == "status":
                if condition.op == FilterOperator.EQUAL:
                    status = condition.value

                else:
                    raise ValueError(
                        f"Unsupported operator '{condition.op}' for column 'status'."
                    )

                condition.applied = True

        sort_condition = None
        if sort:
            for sort_column in sort:
                if sort_column.column in ["id", "key", "name"]:
                    if sort_column.ascending:
                        sort_condition = sort_column.column

                    else:
                        sort_condition = f"-{sort_column.column}"

                    sort_column.applied = True
                    break

        spaces = client.get_spaces(
            ids=ids,
            keys=keys,
            space_type=space_type,
            status=status,
            sort_condition=sort_condition,
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

        sort_condition = None
        if sort:
            for sort_column in sort:
                if sort_column.column in ["id", "title", "createdAt"]:
                    sort_condition = sort_column.column if sort_column != "createdAt" else "created-date"
                    if not sort_column.ascending:
                        sort_condition = f"-{sort_condition}"

                    sort_column.applied = True
                    break

        pages = client.get_pages(
            page_ids=page_ids,
            space_ids=space_ids,
            statuses=statuses,
            title=title,
            sort_condition=sort_condition,
            limit=limit
        )

        pages_df = pd.json_normalize(pages, sep="_")
        pages_df = pages_df[self.get_columns()]

        return pages_df

    def get_columns(self) -> List[str]:
        """
        Retrieves the attributes (columns) of the 'pages' resource.

        Returns:
            List[Text]: A list of attributes (columns) of the 'pages' resource.
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

            if condition.column == "spaceId":
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

        sort_condition = None
        if sort:
            for sort_column in sort:
                if sort_column.column in ["id", "title", "createdAt"]:
                    sort_condition = sort_column.column if sort_column != "createdAt" else "created-date"
                    if not sort_column.ascending:
                        sort_condition = f"-{sort_condition}"

                    sort_column.applied = True
                    break

        blogposts = client.get_blogposts(
            post_ids=post_ids,
            space_ids=space_ids,
            statuses=statuses,
            title=title,
            sort_condition=sort_condition,
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


class ConfluenceWhiteboardsTable(APIResource):
    """
    The table abstraction for the 'whiteboards' resource of the Confluence API.
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
        Executes a parsed SELECT SQL query on the 'whiteboards' resource of the Confluence API.

        Args:
            conditions (List[FilterCondition]): The list of parsed filter conditions.
            limit (int): The maximum number of records to return.
            sort (List[SortColumn]): The list of parsed sort columns.
            targets (List[str]): The list of target columns to return.
        """
        whiteboards = []
        client: ConfluenceAPIClient = self.handler.connect()

        whiteboard_ids = None
        for condition in conditions:
            if condition.column == "id":
                if condition.op == FilterOperator.EQUAL:
                    whiteboard_ids = [condition.value]

                elif condition.op == FilterOperator.IN:
                    whiteboard_ids = condition.value

                else:
                    raise ValueError(
                        f"Unsupported operator '{condition.op}' for column 'id'."
                    )

                condition.applied = True

        if not whiteboard_ids:
            raise ValueError("The 'id' column must be provided in the WHERE clause.")

        whiteboards = [client.get_whiteboard_by_id(whiteboard_id) for whiteboard_id in whiteboard_ids]

        whiteboards_df = pd.json_normalize(whiteboards, sep="_")
        whiteboards_df = whiteboards_df[self.get_columns()]

        return whiteboards_df

    def get_columns(self) -> List[str]:
        """
        Retrieves the attributes (columns) of the 'whiteboards' resource.

        Returns:
            List[Text]: A list of attributes (columns) of the 'whiteboards' resource.
        """
        return [
            "id",
            "type",
            "status",
            "title",
            "parentId",
            "parentType",
            "position",
            "authorId",
            "ownerId",
            "createdAt",
            "version_createdAt",
            "version_message",
            "version_number",
            "version_minorEdit",
            "version_authorId",
        ]


class ConfluenceDatabasesTable(APIResource):
    """
    The table abstraction for the 'databases' resource of the Confluence API.
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
        Executes a parsed SELECT SQL query on the 'databases' resource of the Confluence API.

        Args:
            conditions (List[FilterCondition]): The list of parsed filter conditions.
            limit (int): The maximum number of records to return.
            sort (List[SortColumn]): The list of parsed sort columns.
            targets (List[str]): The list of target columns to return.
        """
        databases = []
        client: ConfluenceAPIClient = self.handler.connect()

        database_ids = None
        for condition in conditions:
            if condition.column == "id":
                if condition.op == FilterOperator.EQUAL:
                    database_ids = [condition.value]

                elif condition.op == FilterOperator.IN:
                    database_ids = condition.value

                else:
                    raise ValueError(
                        f"Unsupported operator '{condition.op}' for column 'id'."
                    )

                condition.applied = True

        if not database_ids:
            raise ValueError("The 'id' column must be provided in the WHERE clause.")

        databases = [client.get_database_by_id(database_id) for database_id in database_ids]

        databases_df = pd.json_normalize(databases, sep="_")
        databases_df = databases_df[self.get_columns()]

        return databases_df

    def get_columns(self) -> List[str]:
        """
        Retrieves the attributes (columns) of the 'databases' resource.

        Returns:
            List[Text]: A list of attributes (columns) of the 'databases' resource.
        """
        return [
            "id",
            "type",
            "status",
            "title",
            "parentId",
            "parentType",
            "position",
            "authorId",
            "ownerId",
            "createdAt",
            "version_createdAt",
            "version_message",
            "version_number",
            "version_minorEdit",
            "version_authorId",
        ]


class ConfluenceTasksTable(APIResource):
    """
    The table abstraction for the 'tasks' resource of the Confluence API.
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
        Executes a parsed SELECT SQL query on the 'tasks' resource of the Confluence API.

        Args:
            conditions (List[FilterCondition]): The list of parsed filter conditions.
            limit (int): The maximum number of records to return.
            sort (List[SortColumn]): The list of parsed sort columns.
            targets (List[str]): The list of target columns to return.
        """
        tasks = []
        client: ConfluenceAPIClient = self.handler.connect()

        task_ids = None
        space_ids = None
        page_ids = None
        blogpost_ids = None
        created_by_ids = None
        assigned_to_ids = None
        completed_by_ids = None
        status = None

        for condition in conditions:
            if condition.column == "id":
                if condition.op == FilterOperator.EQUAL:
                    task_ids = [condition.value]

                elif condition.op == FilterOperator.IN:
                    task_ids = condition.value

                else:
                    raise ValueError(
                        f"Unsupported operator '{condition.op}' for column 'id'."
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

            if condition.column == "pageId":
                if condition.op == FilterOperator.EQUAL:
                    page_ids = [condition.value]

                elif condition.op == FilterOperator.IN:
                    page_ids = condition.value

                else:
                    raise ValueError(
                        f"Unsupported operator '{condition.op}' for column 'pageId'."
                    )

                condition.applied = True

            if condition.column == "blogPostId":
                if condition.op == FilterOperator.EQUAL:
                    blogpost_ids = [condition.value]

                elif condition.op == FilterOperator.IN:
                    blogpost_ids = condition.value

                else:
                    raise ValueError(
                        f"Unsupported operator '{condition.op}' for column 'blogPostId'."
                    )

                condition.applied = True

            if condition.column == "createdBy":
                if condition.op == FilterOperator.EQUAL:
                    created_by_ids = [condition.value]

                elif condition.op == FilterOperator.IN:
                    created_by_ids = condition.value

                else:
                    raise ValueError(
                        f"Unsupported operator '{condition.op}' for column 'createdBy'."
                    )

                condition.applied = True

            if condition.column == "assignedTo":
                if condition.op == FilterOperator.EQUAL:
                    assigned_to_ids = [condition.value]

                elif condition.op == FilterOperator.IN:
                    assigned_to_ids = condition.value

                else:
                    raise ValueError(
                        f"Unsupported operator '{condition.op}' for column 'assignedTo'."
                    )

                condition.applied = True

            if condition.column == "completedBy":
                if condition.op == FilterOperator.EQUAL:
                    completed_by_ids = [condition.value]

                elif condition.op == FilterOperator.IN:
                    completed_by_ids = condition.value

                else:
                    raise ValueError(
                        f"Unsupported operator '{condition.op}' for column 'completedBy'."
                    )

                condition.applied = True

            if condition.column == "status":
                if condition.op == FilterOperator.EQUAL:
                    status = condition.value

                else:
                    raise ValueError(
                        f"Unsupported operator '{condition.op}' for column 'status'."
                    )

                condition.applied = True

        tasks = client.get_tasks(
            task_ids=task_ids,
            space_ids=space_ids,
            page_ids=page_ids,
            blogpost_ids=blogpost_ids,
            created_by_ids=created_by_ids,
            assigned_to_ids=assigned_to_ids,
            completed_by_ids=completed_by_ids,
            status=status,
            limit=limit
        )
        tasks_df = pd.json_normalize(tasks, sep="_")

        # Each task will have either a 'pageId' or 'blogPostId' but not both.
        # In situations where they are all from the same resource, the other column will be empty.
        # We will fill the empty column with None to ensure consistency.
        for column in ["pageId", "blogPostId"]:
            if column not in tasks_df.columns:
                tasks_df[column] = None

        tasks_df = tasks_df[self.get_columns()]

        return tasks_df

    def get_columns(self) -> List[str]:
        """
        Retrieves the attributes (columns) of the 'tasks' resource.

        Returns:
            List[Text]: A list of attributes (columns) of the 'tasks' resource.
        """
        return [
            "id",
            "localId",
            "spaceId",
            "pageId",
            "blogPostId",
            "status",
            "body_storage_representation",
            "body_storage_value",
            "createdBy",
            "assignedTo",
            "completedBy",
            "createdAt",
            "updatedAt",
            "dueAt",
            "completedAt",
        ]
