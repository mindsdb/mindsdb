from typing import List
from pydantic_settings import BaseSettings


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
    
    TEST_CHAT_DATA: Dict
        Test data for the chats table.

    TEST_CHATS_DATA: List[Dict]
        Test data for the chats table.

    TEST_CHAT_MESSAGE_DATA: Dict
        Test data for the chat messages table.

    TEST_CHAT_MESSAGES_DATA: List[Dict]
        Test data for the chat messages table.

    TEST_CHANNEL_DATA: Dict
        Test data for the channels table.

    TEST_CHANNELS_DATA: List[Dict]
        Test data for the channels table.

    TEST_CHANNEL_MESSAGE_DATA: Dict
        Test data for the channel messages table.  

    TEST_CHANNEL_MESSAGES_DATA: List[Dict]
        Test data for the channel messages table.
    
    TEST_GROUP_DATA: dict

    TEST_CHANNEL_ID_DATA: dict
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
        "body_content"
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
        "eventDetail_@odata.type",
        "eventDetail_visibleHistoryStartDateTime",
        "eventDetail_members",
        "eventDetail_initiator_device",
        "eventDetail_initiator_application_@odata.type",
        "eventDetail_initiator_application_id",
        "eventDetail_initiator_application_displayName",
        "eventDetail_initiator_application_applicationIdentityType",
        "eventDetail_channelId",
        "eventDetail_channelDisplayName",
        "eventDetail_initiator_user_@odata.type",
        "eventDetail_initiator_user_id",
        "eventDetail_initiator_user_displayName",
        "eventDetail_initiator_user_userIdentityType",
        "eventDetail_initiator_user_tenantId"
    ]

    TEST_CHAT_DATA: dict = {
        '@odata.context': 'test_context',
        'id': 'test_id',
        'topic': None,
        'createdDateTime': '2023-11-20T10:25:19.553Z',
        'lastUpdatedDateTime': '2023-11-20T10:25:19.553Z',
        'chatType': 'oneOnOne',
        'webUrl': 'https://teams.test',
        'tenantId': 'test_tenant_id',
        'onlineMeetingInfo': None,
        'viewpoint': {
            'isHidden': False,
            'lastMessageReadDateTime': '2023-12-08T17:09:34.214Z'
        },
        'lastMessagePreview@odata.context': 'https://graph.test',
        'lastMessagePreview': {
            'id': '1702055374214',
            'createdDateTime': '2023-12-08T17:09:34.214Z',
            'isDeleted': False,
            'messageType': 'message',
            'eventDetail': None,
            'body': {
                'contentType': 'text',
                'content': '\n\nTest message.'
            },
            'from': {
                'application': None,
                'device': None,
                'user': {}
            }
        }
    }

    TEST_CHATS_DATA: List[dict] = [TEST_CHAT_DATA]

    TEST_CHAT_MESSAGE_DATA: dict = {
        '@odata.context': 'test_context', 
        'id': 'test_id', 
        'replyToId': None, 
        'etag': 'test_etag', 
        'messageType': 'message', 
        'createdDateTime': '2023-12-08T17:09:22.241Z', 
        'lastModifiedDateTime': '2023-12-08T17:09:22.241Z', 
        'lastEditedDateTime': None, 
        'deletedDateTime': None, 
        'subject': None, 
        'summary': None, 
        'chatId': 'test_chat_id', 
        'importance': 'normal', 
        'locale': 'en-us',
        'webUrl': None,
        'channelIdentity': None,
        'policyViolation': None,
        'attachments': [],
        'mentions': [],
        'reactions': [],
        'from': {
            'application': None,
            'device': None,
            'user': {
                '@odata.type': 'test_type',
                'id': 'test_user_id',
                'displayName': 'test_user_display_name',
                'userIdentityType': 'aadUser',
                'tenantId': 'test_tenant_id'
            }
        },
        'body': {
            'contentType': 'text',
            'content': '\n\nTest message.'
        },
        'eventDetail': {
            '@odata.type': 'test_type',
            'visibleHistoryStartDateTime': '2023-12-08T17:09:22.241Z',
            'members': [],
            'initiator': {
                'device': None,
                'application': None,
                'user': {
                    '@odata.type': 'test_type',
                    'id': 'test_user_id',
                    'displayName': 'test_user_display_name',
                    'userIdentityType': 'aadUser',
                    'tenantId': 'test_tenant_id'
                }
            },
        }
    }

    TEST_CHAT_MESSAGES_DATA: List[dict] = [TEST_CHAT_MESSAGE_DATA]

    TEST_CHANNEL_DATA: dict = {
        '@odata.context': 'test_context', 
        'id': 'test_id', 
        'createdDateTime': '2023-11-17T22:54:33.055Z', 
        'displayName': 'test_display_name', 
        'description': None, 
        'isFavoriteByDefault': None, 
        'email': 'test@test.com', 
        'tenantId': 'test_tenant_id', 
        'webUrl': 'https://teams.test', 
        'membershipType': 'standard', 
        'teamId': 'test_team_id'
    }

    TEST_CHANNELS_DATA: List[dict] = [TEST_CHANNEL_DATA]

    TEST_CHANNEL_MESSAGE_DATA: dict = {
        '@odata.context': 'test_context', 
        'id': 'test_id', 
        'replyToId': None, 
        'etag': 'test_etag', 
        'messageType': 'message', 
        'createdDateTime': '2023-11-30T16:52:50.18Z', 
        'lastModifiedDateTime': '2023-11-30T16:52:50.18Z', 
        'lastEditedDateTime': None, 
        'deletedDateTime': None, 
        'subject': 'Test Subject', 
        'summary': None, 
        'chatId': None, 
        'importance': 
        'normal', 
        'locale': 'en-us',
        'webUrl': 'https://teams.test',
        'policyViolation': None,
        'attachments': [],
        'mentions': [],
        'reactions': [],
        'from': {
            'application': None,
            'device': None,
            'user': {
                '@odata.type': 'test_type',
                'id': 'test_user_id',
                'displayName': 'test_user_display_name',
                'userIdentityType': 'aadUser',
                'tenantId': 'test_tenant_id'
            }
        },
        'body': {
            'contentType': 'text',
            'content': '\n\nTest message.'
        },
        'channelIdentity': {
            'teamId': 'test_team_id',
            'channelId': 'test_channel_id'
        },
        'eventDetail': {
            '@odata.type': 'test_type',
            'visibleHistoryStartDateTime': '2023-11-30T16:52:50.18Z',
            'members': [],
            'initiator': {
                'device': None,
                'application': {
                    '@odata.type': 'test_type',
                    'id': 'test_app_id',
                    'displayName': 'test_app_display_name',
                    'applicationIdentityType': 'bot'
                },
                'user': {
                    '@odata.type': 'test_type',
                    'id': 'test_user_id',
                    'displayName': 'test_user_display_name',
                    'userIdentityType': 'aadUser',
                    'tenantId': 'test_tenant_id'
                }
            },
            'channelId': 'test_channel_id',
            'channelDisplayName': 'test_channel_display_name'
        }
    }

    TEST_CHANNEL_MESSAGES_DATA: List[dict] = [TEST_CHANNEL_MESSAGE_DATA]

    TEST_GROUP_DATA: dict = {
        '@odata.context': 'test_context', 
        'value': [
            {
                'id': 'test_team_id',
                'resourceProvisioningOptions': ['Team'],
            }
        ]
    }

    TEST_CHANNEL_ID_DATA: dict = {
        '@odata.context': 'test_context', 
        '@odata.count': 1,
        'value': [
            {
                'id': 'test_channel_id',
            }
        ]
    }

ms_teams_handler_config = MSTeamsHandlerConfig()