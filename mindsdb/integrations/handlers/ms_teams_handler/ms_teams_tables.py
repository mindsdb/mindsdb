from typing import List

import pandas as pd

from mindsdb.integrations.handlers.ms_teams_handler.ms_graph_api_teams_client import MSGraphAPITeamsClient
from mindsdb.integrations.libs.api_handler import APIResource
from mindsdb.integrations.utilities.sql_utils import (
    FilterCondition,
    FilterOperator,
    SortColumn
)


class ChannelsTable(APIResource):
    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[str] = None,
        **kwargs
    ):
        client: MSGraphAPITeamsClient = self.handler.connect()
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

        if team_id:
            if channel_ids:
                channels = client.get_channels_in_group_by_ids(team_id, channel_ids)

            else:
                channels = client.get_all_channels_in_group(team_id)

        elif channel_ids:
            channels = client.get_channels_across_all_groups_by_ids(channel_ids)

        else:
            channels = client.get_all_channels_across_all_groups()

        channels_df = pd.json_normalize(channels, sep="_")
        channels_df = channels_df[self.get_columns()]

        return channels_df

    def get_columns(self) -> List[str]:
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
    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[str] = None,
        **kwargs
    ):
        client: MSGraphAPITeamsClient = self.handler.connect()
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
        
        if message_ids:
            messages = client.get_messages_in_channel_by_ids(group_id, channel_id, message_ids)

        else:
            messages = client.get_all_messages_in_channel(group_id, channel_id)

        messages_df = pd.json_normalize(messages, sep="_")
        messages_df = messages_df[self.get_columns()]

        return messages_df

    def get_columns(self) -> List[str]:
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