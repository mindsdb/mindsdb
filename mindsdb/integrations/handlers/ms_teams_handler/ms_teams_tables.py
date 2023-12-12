import pandas as pd
from typing import Text, List, Dict, Any

from mindsdb.utilities import log
from mindsdb_sql.parser import ast
from mindsdb_sql.parser.ast import ASTNode
from mindsdb.integrations.libs.api_handler import APITable

from mindsdb.integrations.handlers.ms_teams_handler.settings import ms_teams_handler_config

from mindsdb.integrations.handlers.utilities.query_utilities.insert_query_utilities import INSERTQueryParser
from mindsdb.integrations.handlers.utilities.query_utilities.select_query_utilities import SELECTQueryParser, SELECTQueryExecutor

logger = log.getLogger(__name__)


class ChannelsTable(APITable):
    """
    The Microsoft Channels Table implementation.
    """

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Pulls data from the "GET /teams/{group_id}/channels" and "GET teams/{group_id}/channels/{channel_id}" Microsoft Graph API endpoints.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query.

        Returns
        -------
        pd.DataFrame
            Microsoft Teams Channels matching the query.

        Raises
        ------
        ValueError
            If the query contains an unsupported target (column).

        NotImplementedError
            If the query contains an unsupported condition.
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'channels',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        channel_id, team_id = None, None
        for op, arg1, arg2 in where_conditions:
            if arg1 == 'id':
                if op == "=":
                    channel_id = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for id column.")
                
            if arg1 == 'teamId':
                if op == "=":
                    team_id = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for teamId column.")
                
        if channel_id and team_id:
            channels_df = pd.json_normalize(self.get_channels(channel_id, team_id))
            where_conditions = [where_condition for where_condition in where_conditions if where_condition[1] not in ['id', 'teamId']]
        else:
            channels_df = pd.json_normalize(self.get_channels())

        select_statement_executor = SELECTQueryExecutor(
            channels_df,
            selected_columns,
            where_conditions,
            order_by_conditions,
            result_limit if query.limit else None
        )

        channels_df = select_statement_executor.execute_query()

        return channels_df

    def get_channels(self, channel_id: Text = None, team_id: Text = None) -> List[Dict[Text, Any]]:
        """
        Calls the API client to get the channels from the Microsoft Graph API.
        If all parameters are None, it will return all the channels from all the teams.

        Parameters
        ----------
        channel_id: Text
            The channel id to get the channel from.

        team_id: Text
            The team id to get the channels from.

        Returns
        -------
        List[Dict[Text, Any]]
            The channels from the Microsoft Graph API.
        """

        api_client = self.handler.connect()

        if channel_id and team_id:
            return [api_client.get_channel(team_id, channel_id)]
        else:
            return api_client.get_channels()
    
    def get_columns(self) -> List[Text]:
        """
        Returns the columns of the Channels Table.

        Returns
        -------
        List[Text]
            The columns of the Channels Table.
        """
        return ms_teams_handler_config.CHANNELS_TABLE_COLUMNS

class ChannelMessagesTable(APITable):
    """
    The Microsoft Teams Channel Messages Table implementation.       
    """
     
    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Pulls data from the "GET /teams/{group_id}/channels/{channel_id}/messages" and "GET /teams/{group_id}/channels/{channel_id}/messages/{message_id}" Microsoft Graph API endpoints.

        Parameters
        ----------
        query: ast.Select
            Given SQL SELECT query.

        Returns
        -------
        pd.DataFrame
            Microsoft Teams Channel Messages matching the query.

        Raises
        ------
        ValueError
            If the query contains an unsupported target (column).

        NotImplementedError
            If the query contains an unsupported condition.
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'channel_messages',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        team_id, channel_id, message_id = None, None, None
        for op, arg1, arg2 in where_conditions:
            if arg1 == 'id':
                if op == "=":
                    message_id = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for id column.")

            if arg1 == 'channelIdentity_teamId':
                if op == "=":
                    team_id = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for id column.")
                
            if arg1 == 'channelIdentity_channelId':
                if op == "=":
                    team_id = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for teamId column.")
                
        if message_id and channel_id and team_id:
            messages_df = pd.json_normalize(self.get_messages(team_id, channel_id, message_id), sep='_')
            where_conditions = [where_condition for where_condition in where_conditions if where_condition[1] not in ['id', 'channelIdentity_teamId', 'channelIdentity_channelId']]
        else:
            messages_df = pd.json_normalize(self.get_messages(), sep='_')

        select_statement_executor = SELECTQueryExecutor(
            messages_df,
            selected_columns,
            where_conditions,
            order_by_conditions,
            result_limit if query.limit else None
        )

        messages_df = select_statement_executor.execute_query()

        return messages_df
    
    def get_messages(self, team_id: Text = None, channel_id: Text = None, message_id: Text = None) -> List[Dict[Text, Any]]:
        """
        Calls the API client to get the messages from the Microsoft Graph API.
        If all parameters are None, it will return all the messages from all the channels from all the teams.

        Parameters
        ----------
        team_id: Text
            The team id to get the messages from.

        channel_id: Text
            The channel id to get the messages from.

        message_id: Text
            The message id to get the message from.
        """

        api_client = self.handler.connect()
        # TODO: Should these records be filtered somehow?
        if message_id and channel_id and team_id:
            return [api_client.get_channel_message(team_id, channel_id, message_id)]
        else:
            return api_client.get_channel_messages()
    
    def insert(self, query: ast.Insert) -> None:
        """
        Inserts data into the "POST /teams/{group_id}/channels/{channel_id}/messages" Microsoft Graph API endpoint.

        Parameters
        ----------
        query : ast.Insert
           Given SQL INSERT query.

        Returns
        -------
        None

        Raises
        ------
        UnsupportedColumnException
            If the query contains an unsupported column.

        MandatoryColumnException
            If the query is missing a mandatory column.

        ColumnCountMismatchException
            If the number of columns does not match the number of values.
        """

        insert_statement_parser = INSERTQueryParser(
            query,
            supported_columns=["channelIdentity_teamId", "channelIdentity_channelId", "subject", "body_content"],
            mandatory_columns=["channelIdentity_teamId", "channelIdentity_channelId", "body_content"],
        )

        messages_to_send = insert_statement_parser.parse_query()

        self.send_messages(messages_to_send)

    def send_messages(self, messages_to_send: List[Dict[Text, Any]]) -> None:
        """
        Calls the API client to send the messages to the Microsoft Graph API.
        
        Parameters
        ----------
        messages_to_send: List[Dict[Text, Any]]
            The messages to send to the Microsoft Graph API.

        Returns
        -------
        None

        Raises
        ------
        None
        """

        api_client = self.handler.connect()
        for message in messages_to_send:
            api_client.send_channel_message(
                group_id=message["channelIdentity_teamId"],
                channel_id=message["channelIdentity_channelId"],
                message=message["body_content"],
                subject=message.get("subject")
            )
    
    def get_columns(self) -> List[Text]:
        """
        Returns the columns of the Channel Messages Table.

        Returns
        -------
        list
            The columns of the Channel Messages Table.
        """

        return ms_teams_handler_config.CHANNEL_MESSAGES_TABLE_COLUMNSS

class ChannelMessageRepliesTable(APITable):
    """
    The Microsoft Teams Channel Message Replies Table implementation.
    """
    pass
    
class ChatsTable(APITable):
    """The Microsoft Chats Table implementation"""
    
    def select(self, query: ASTNode) -> pd.DataFrame:
        """Pulls data from the Microsoft Teams "GET /chats" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Microsoft Teams Chats matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        select_statement_parser = SELECTQueryParser(
            query,
            'chats',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        id = None
        for op, arg1, arg2 in where_conditions:
            if arg1 == 'id':
                if op == "=":
                    id = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for id column.")
                
        if id:
            chats_df = pd.json_normalize(self.get_chats(id), sep='_')
            where_conditions = [where_condition for where_condition in where_conditions if where_condition[1] not in ['id']]
        else:
            chats_df = pd.json_normalize(self.get_chats(), sep='_')

        select_statement_executor = SELECTQueryExecutor(
            chats_df,
            selected_columns,
            where_conditions,
            order_by_conditions,
            result_limit if query.limit else None
        )

        chats_df = select_statement_executor.execute_query()

        return chats_df
    
    def get_chats(self, chat_id = None) -> List[Dict[Text, Any]]:
        api_client = self.handler.connect()

        if chat_id:
            chats = [api_client.get_chat(chat_id)]
        else:
            chats = api_client.get_chats()

        for chat in chats:
            last_message_preview = chat.get("lastMessagePreview")
            # keep only the lastMessagePreview_id and lastMessagePreview_createdDateTime columns
            if last_message_preview:
                chat["lastMessagePreview_id"] = last_message_preview.get("id")
                chat["lastMessagePreview_createdDateTime"] = last_message_preview.get("createdDateTime")
                del chat["lastMessagePreview"]
                del chat["lastMessagePreview@odata.context"]

        return chats

    def get_columns(self) -> list:
        return [
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
    
class ChatMessagesTable(APITable):
    """The Microsoft Chat Messages Table implementation"""
    
    def select(self, query: ASTNode) -> pd.DataFrame:
        """Pulls data from the Microsoft Teams "GET /chats/{chat_id}/messages" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Microsoft Teams Chat Messages matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        select_statement_parser = SELECTQueryParser(
            query,
            'chat_messages',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        chat_id, message_id = None, None
        for op, arg1, arg2 in where_conditions:
            if arg1 == 'id':
                if op == "=":
                    message_id = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for id column.")
                
            if arg1 == 'chatId':
                if op == "=":
                    chat_id = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for chatId column.")
                
        if message_id and chat_id:
            messages_df = pd.json_normalize(self.get_messages(chat_id, message_id), sep='_')
            where_conditions = [where_condition for where_condition in where_conditions if where_condition[1] not in ['id', 'chatId']]
        else:
            messages_df = pd.json_normalize(self.get_messages(chat_id), sep='_')

        select_statement_executor = SELECTQueryExecutor(
            messages_df,
            selected_columns,
            where_conditions,
            order_by_conditions,
            result_limit if query.limit else None
        )

        messages_df = select_statement_executor.execute_query()

        return messages_df
    
    def get_messages(self, chat_id = None, message_id = None) -> List[Dict[Text, Any]]:
        api_client = self.handler.connect()

        if message_id and chat_id:
            return [api_client.get_chat_message(chat_id, message_id)]
        elif chat_id:
            return api_client.get_chat_messages(chat_id)
        else:
            return api_client.get_all_chat_messages()
        
    def get_columns(self) -> list:
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
            "channelIdentity",
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
            "from",
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

    def insert(self, query: ASTNode) -> None:
        """Inserts data into the Microsoft Teams "POST /chats/{chat_id}/messages" API endpoint.

        Parameters
        ----------
        query : ast.Insert
           Given SQL INSERT query

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        insert_statement_parser = INSERTQueryParser(
            query,
            supported_columns=["chatId", "subject", "body_content"],
            mandatory_columns=["chatId", "body_content"],
        )

        messages_to_send = insert_statement_parser.parse_query()

        self.send_messages(messages_to_send)

    def send_messages(self, messages_to_send: List[Dict[Text, Any]]) -> None:
        api_client = self.handler.connect()
        for message in messages_to_send:
            api_client.send_chat_message(
                chat_id=message["chatId"],
                message=message["body_content"],
                subject=message.get("subject")
            )

    class ChatMessageRepliesTable(APITable):
        """The Microsoft Chat Message Replies Table implementation"""
        pass