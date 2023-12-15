from typing import List
from pydantic import BaseSettings


class MSTeamsHandlerConfig(BaseSettings):
    """
    Settings for the Microsoft Teams handler.

    Attributes
    ----------
    DEFAULT_SCOPES: List
        Default scopes for querying the Microsoft Graph API.

    CHATS_TABLE_COLUMNS: List
        Columns for the chats table. 

    CHAT_MESSAGES_TABLE_COLUMNS: List
        Columns for the chat messages table.

    CHANNELS_TABLE_COLUMNS: List
        Columns for the channels table.

    CHANNEL_MESSAGES_TABLE_COLUMNS: List
        Columns for the channel messages table.   
    """

    DEFAULT_SCOPES: List = [
        'https://graph.microsoft.com/User.Read',
        'https://graph.microsoft.com/Group.Read.All',
        'https://graph.microsoft.com/ChannelMessage.Send',
        'https://graph.microsoft.com/Chat.Read',
        'https://graph.microsoft.com/ChatMessage.Send',
    ]

    CHATS_TABLE_COLUMNS: List = [
        "id",
        "topic",
        "createdDateTime",
        "lastUpdatedDateTime",
        "chatType",
        "webUrl",
        "tenantId",
        "onlineMeetingInfo",
        "viewpoint_isHidden",
        "viewpoint_lastMessageReadDateTime",
        "lastMessagePreview_id",
        "lastMessagePreview_createdDateTime",
    ]

    CHAT_MESSAGES_TABLE_COLUMNS: List = [
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
        "channelIdentity",
        "policyViolation",
        "attachments",
        "mentions",
        "reactions",
        "from_application",
        "from_device",
        "from_user_@odata.type",
        "from_user_id",
        "from_user_displayName",
        "from_user_userIdentityType",
        "from_user_tenantId",
        "body_contentType",
        "body_content",
        "eventDetail_@odata.type",
        "eventDetail_visibleHistoryStartDateTime",
        "eventDetail_members",
        "eventDetail_initiator_application",
        "eventDetail_initiator_device",
        "eventDetail_initiator_user_@odata.type",
        "eventDetail_initiator_user_id",
        "eventDetail_initiator_user_displayName",
        "eventDetail_initiator_user_userIdentityType",
        "eventDetail_initiator_user_tenantId",
    ]

    CHANNELS_TABLE_COLUMNS: List = [
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

    CHANNEL_MESSAGES_TABLE_COLUMNS: List = [
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
        "eventDetail",
        "attachments",
        "mentions",
        "reactions",
        "from_application",
        "from_device",
        "from_user_@odata.type",
        "from_user_id",
        "from_user_displayName",
        "from_user_userIdentityType",
        "from_user_tenantId",
        "body_contentType",
        "body_content",
        "channelIdentity_teamId",
        "channelIdentity_channelId",
        "from",
        "eventDetail_@odata.type",
        "eventDetail_visibleHistoryStartDateTime",
        "eventDetail_members",
        "eventDetail_initiator_device",
        "eventDetail_initiator_user",
        "eventDetail_initiator_application_@odata.type",
        "eventDetail_initiator_application_id",
        "eventDetail_initiator_application_displayName",
        "eventDetail_initiator_application_applicationIdentityType",
        "eventDetail_channelId",
        "eventDetail_channelDisplayName",
        "eventDetail_initiator_application",
        "eventDetail_initiator_user_@odata.type",
        "eventDetail_initiator_user_id",
        "eventDetail_initiator_user_displayName",
        "eventDetail_initiator_user_userIdentityType",
        "eventDetail_initiator_user_tenantId"
    ]

ms_teams_handler_config = MSTeamsHandlerConfig()