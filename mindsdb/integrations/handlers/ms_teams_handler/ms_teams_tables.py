from typing import List

import pandas as pd

from mindsdb.integrations.handlers.ms_teams_handler.ms_graph_api_teams_client import MSGraphAPITeamsDelegatedPermissionsClient
from mindsdb.integrations.libs.api_handler import APIResource
from mindsdb.integrations.utilities.sql_utils import (
    FilterCondition,
    FilterOperator,
    SortColumn
)


class TeamsTable(APIResource):
    """
    The table abstraction for the 'teams' resource of the Microsoft Graph API.
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
        Executes a parsed SELECT SQL query on the 'teams' resource of the Microsoft Graph API.

        Args:
            conditions (List[FilterCondition]): The list of parsed filter conditions.
            limit (int): The maximum number of records to return.
            sort (List[SortColumn]): The list of parsed sort columns.
            targets (List[str]): The list of target columns to return.
        """
        client: MSGraphAPITeamsDelegatedPermissionsClient = self.handler.connect()
        teams = client.get_teams()

        teams_df = pd.json_normalize(teams, sep="_")
        teams_df = teams_df.reindex(columns=self.get_columns(), fill_value=None)

        return teams_df

    def get_columns(self) -> List[str]:
        """
        Retrieves the attributes (columns) of the 'teams' resource.

        Returns:
            List[Text]: A list of attributes (columns) of the 'teams' resource.
        """
        return [
            "id",
            "createdDateTime",
            "displayName",
            "description",
            "internalId",
            "classification",
            "specialization",
            "visibility",
            "webUrl",
            "isArchived",
            "tenantId",
            "isMembershipLimitedToOwners",
        ]


class ChannelsTable(APIResource):
    """
    The table abstraction for the 'channels' resource of the Microsoft Graph API.
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
        Executes a parsed SELECT SQL query on the 'channels' resource of the Microsoft Graph API.

        Args:
            conditions (List[FilterCondition]): The list of parsed filter conditions.
            limit (int): The maximum number of records to return.
            sort (List[SortColumn]): The list of parsed sort columns.
            targets (List[str]): The list of target columns to return.
        """
        client: MSGraphAPITeamsDelegatedPermissionsClient = self.handler.connect()
        channels = []

        team_id, channel_ids = None, None
        for condition in conditions:
            if condition.column == "teamId":
                if condition.op == FilterOperator.EQUAL:
                    team_id = condition.value

                else:
                    raise ValueError(
                        f"Unsupported operator '{condition.op}' for column 'teamId'."
                    )

                condition.applied = True

            if condition.column == "id":
                if condition.op == FilterOperator.EQUAL:
                    channel_ids = [condition.value]

                elif condition.op == FilterOperator.IN:
                    channel_ids = condition.value

                else:
                    raise ValueError(
                        f"Unsupported operator '{condition.op}' for column 'id'."
                    )

                condition.applied = True

        channels = client.get_channels(team_id, channel_ids)

        channels_df = pd.json_normalize(channels, sep="_")
        channels_df = channels_df[self.get_columns()]

        return channels_df

    def get_columns(self) -> List[str]:
        """
        Retrieves the attributes (columns) of the 'chats' resource.

        Returns:
            List[Text]: A list of attributes (columns) of the 'chats' resource.
        """
        return [
            "id",
            "createdDateTime",
            "displayName",
            "description",
            "isFavoriteByDefault",
            "email",
            "tenantId",
            "webUrl",
            "membershipType",
            "teamId",
        ]


class ChannelMessagesTable(APIResource):
    """
    The table abstraction for the 'channel messages' resource of the Microsoft Graph API.
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
        Executes a parsed SELECT SQL query on the 'channel messages' resource of the Microsoft Graph API.

        Args:
            conditions (List[FilterCondition]): The list of parsed filter conditions.
            limit (int): The maximum number of records to return.
            sort (List[SortColumn]): The list of parsed sort columns.
            targets (List[str]): The list of target columns to return.
        """
        client: MSGraphAPITeamsDelegatedPermissionsClient = self.handler.connect()
        messages = []

        group_id, channel_id, message_ids = None, None, None
        for condition in conditions:
            if condition.column == "channelIdentity_teamId":
                if condition.op == FilterOperator.EQUAL:
                    group_id = condition.value

                else:
                    raise ValueError(
                        f"Unsupported operator '{condition.op}' for column 'channelIdentity_teamId'."
                    )

                condition.applied = True

            if condition.column == "channelIdentity_channelId":
                if condition.op == FilterOperator.EQUAL:
                    channel_id = condition.value

                else:
                    raise ValueError(
                        f"Unsupported operator '{condition.op}' for column 'channelIdentity_channelId'."
                    )

                condition.applied = True

            if condition.column == "id":
                if condition.op == FilterOperator.EQUAL:
                    message_ids = [condition.value]

                elif condition.op == FilterOperator.IN:
                    message_ids = condition.value

                else:
                    raise ValueError(
                        f"Unsupported operator '{condition.op}' for column 'id'."
                    )

                condition.applied = True

        if not group_id or not channel_id:
            raise ValueError("The 'channelIdentity_teamId' and 'channelIdentity_channelId' columns are required.")

        messages = client.get_channel_messages(group_id, channel_id, message_ids)

        messages_df = pd.json_normalize(messages, sep="_")
        messages_df = messages_df[self.get_columns()]

        return messages_df

    def get_columns(self) -> List[str]:
        """
        Retrieves the attributes (columns) of the 'chat messages' resource.

        Returns:
            List[Text]: A list of attributes (columns) of the 'chat messages' resource.
        """
        return [
            "id",
            "replyToId",
            "etag",
            "messageType",
            "createdDateTime",
            "lastModifiedDateTime",
            "lastEditedDateTime",
            "deletedDateTime",
            "subject",
            "summary",
            "chatId",
            "importance",
            "locale",
            "webUrl",
            "policyViolation",
            "from_application",
            "from_device",
            "from_user_id",
            "from_user_displayName",
            "from_user_userIdentityType",
            "body_contentType",
            "body_content",
            "channelIdentity_teamId",
            "channelIdentity_channelId",
        ]


class ChatsTable(APIResource):
    """
    The table abstraction for the 'chats' resource of the Microsoft Graph API.
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
        Executes a parsed SELECT SQL query on the 'chats' resource of the Microsoft Graph API.

        Args:
            conditions (List[FilterCondition]): The list of parsed filter conditions.
            limit (int): The maximum number of records to return.
            sort (List[SortColumn]): The list of parsed sort columns.
            targets (List[str]): The list of target columns to return.
        """
        client: MSGraphAPITeamsDelegatedPermissionsClient = self.handler.connect()
        chats = []

        chat_ids = None
        for condition in conditions:
            if condition.column == "id":
                if condition.op == FilterOperator.EQUAL:
                    chat_ids = [condition.value]

                elif condition.op == FilterOperator.IN:
                    chat_ids = condition.value

                else:
                    raise ValueError(
                        f"Unsupported operator '{condition.op}' for column 'id'."
                    )

                condition.applied = True

        chats = client.get_chats(chat_ids)

        chats_df = pd.json_normalize(chats, sep="_")
        chats_df = chats_df[self.get_columns()]

        return chats_df

    def get_columns(self) -> List[str]:
        """
        Retrieves the attributes (columns) of the 'chats' resource.

        Returns:
            List[Text]: A list of attributes (columns) of the 'chats' resource.
        """
        return [
            "id",
            "topic",
            "createdDateTime",
            "lastUpdatedDateTime",
            "chatType",
            "webUrl",
            "isHiddenForAllMembers"
        ]


class ChatMessagesTable(APIResource):
    """
    The table abstraction for the 'chat messages' resource of the Microsoft Graph API.
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
        Executes a parsed SELECT SQL query on the 'chat messages' resource of the Microsoft Graph API.

        Args:
            conditions (List[FilterCondition]): The list of parsed filter conditions.
            limit (int): The maximum number of records to return.
            sort (List[SortColumn]): The list of parsed sort columns.
            targets (List[str]): The list of target columns to return.
        """
        client: MSGraphAPITeamsDelegatedPermissionsClient = self.handler.connect()
        messages = []

        chat_id, message_ids = None, None
        for condition in conditions:
            if condition.column == "chatId":
                if condition.op == FilterOperator.EQUAL:
                    chat_id = condition.value

                else:
                    raise ValueError(
                        f"Unsupported operator '{condition.op}' for column 'chatId'."
                    )

                condition.applied = True

            if condition.column == "id":
                if condition.op == FilterOperator.EQUAL:
                    message_ids = [condition.value]

                elif condition.op == FilterOperator.IN:
                    message_ids = condition.value

                else:
                    raise ValueError(
                        f"Unsupported operator '{condition.op}' for column 'id'."
                    )

                condition.applied = True

        if not chat_id:
            raise ValueError("The 'chatId' column is required.")

        messages = client.get_chat_messages(chat_id, message_ids)

        messages_df = pd.json_normalize(messages, sep="_")
        messages_df = messages_df[self.get_columns()]

        return messages_df

    def get_columns(self) -> List[str]:
        """
        Retrieves the attributes (columns) of the 'chat messages' resource.

        Returns:
            List[Text]: A list of attributes (columns) of the 'chat messages' resource.
        """
        return [
            "id",
            "replyToId",
            "etag",
            "messageType",
            "createdDateTime",
            "lastModifiedDateTime",
            "lastEditedDateTime",
            "deletedDateTime",
            "subject",
            "summary",
            "chatId",
            "importance",
            "locale",
            "webUrl",
            "policyViolation",
            "from_application",
            "from_device",
            "from_user_id",
            "from_user_displayName",
            "from_user_userIdentityType",
            "body_contentType",
            "body_content",
        ]
