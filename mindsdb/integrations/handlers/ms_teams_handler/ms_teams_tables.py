import pandas as pd
from typing import Text, List, Dict, Any

from mindsdb.utilities import log
from mindsdb_sql.parser import ast
from mindsdb.integrations.libs.api_handler import APITable

from mindsdb.integrations.handlers.ms_teams_handler.settings import ms_teams_handler_config

from mindsdb.integrations.utilities.handlers.query_utilities.insert_query_utilities import INSERTQueryParser
from mindsdb.integrations.utilities.handlers.query_utilities.select_query_utilities import SELECTQueryParser, SELECTQueryExecutor

logger = log.getLogger(__name__)


class ChatsTable(APITable):
    """
    The Microsoft Teams Chats Table implementation.
    """
    
    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Pulls data from the "GET /chats" and "GET /chats/{chat_id} Microsoft Graph API endpoints.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query.

        Returns
        -------
        pd.DataFrame
            Microsoft Teams Chats matching the query.

        Raises
        ------
        ValueError
            If the query contains an unsupported target (column).

        NotImplementedError
            If the query contains an unsupported condition.
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'chats',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        # only the = operator is supported for the id column
        id = None
        for op, arg1, arg2 in where_conditions:
            if arg1 == 'id':
                if op == "=":
                    id = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for id column.")
        
        chats_df = pd.json_normalize(self.get_chats(id), sep='_')

        # if the id is given, remove the id column from the where conditions as it has already been evaluated in the API call (get_chats)
        if id:
            where_conditions = [where_condition for where_condition in where_conditions if where_condition[1] not in ['id']]

        select_statement_executor = SELECTQueryExecutor(
            chats_df,
            selected_columns,
            where_conditions,
            order_by_conditions,
            result_limit if query.limit else None
        )

        chats_df = select_statement_executor.execute_query()

        return chats_df
    
    def get_chats(self, chat_id: Text = None) -> List[Dict[Text, Any]]:
        """
        Calls the API client to get the chats from the Microsoft Graph API.

        Parameters
        ----------
        chat_id: Text
            The chat id to get the chat from.

        Returns
        -------
        List[Dict[Text, Any]]
            The chats from the Microsoft Graph API.
        """

        api_client = self.handler.connect()

        # if the chat_id is given, get the chat with that id from the API
        if chat_id:
            chats = [api_client.get_chat(chat_id)]
        # if the chat_id is not given, get all the chats
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

    def get_columns(self) -> List[Text]:
        """
        Returns the columns of the Chats Table.

        Returns
        -------
        List[Text]
            The columns of the Chats Table.
        """

        return ms_teams_handler_config.CHATS_TABLE_COLUMNS
    
class ChatMessagesTable(APITable):
    """
    The Microsoft Teams Chat Messages Table implementation.
    """
    
    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Pulls data from the "GET /chats/{chat_id}/messages" and "GET /chats/{chat_id}/messages/{message_id}" Microsoft Graph API endpoints.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query.

        Returns
        -------
        pd.DataFrame
            Microsoft Teams Chat Messages matching the query.

        Raises
        ------
        ValueError
            If the query contains an unsupported target (column).

        NotImplementedError
            If the query contains an unsupported condition.
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'chat_messages',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        # only the = operator is supported for the id and chatId columns
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
        
        messages_df = pd.json_normalize(self.get_messages(chat_id, message_id), sep='_')
    
        # if both chat_id and message_id are given, remove the id and chatId columns from the where conditions as they have already been evaluated in the API call (get_messages)
        if chat_id and message_id:
            where_conditions = [where_condition for where_condition in where_conditions if where_condition[1] not in ['id', 'chatId']]
        # if only the chat_id is given, remove the chatId column from the where conditions as it has already been evaluated in the API call (get_messages)
        elif chat_id:
            where_conditions = [where_condition for where_condition in where_conditions if where_condition[1] not in ['chatId']]

        select_statement_executor = SELECTQueryExecutor(
            messages_df,
            selected_columns,
            where_conditions,
            order_by_conditions,
            result_limit if query.limit else None
        )

        messages_df = select_statement_executor.execute_query()

        return messages_df
    
    def get_messages(self, chat_id: Text = None, message_id: Text = None) -> List[Dict[Text, Any]]:
        """
        Calls the API client to get the messages from the Microsoft Graph API.
        If all parameters are None, it will return all the messages from all the chats.
        If only the chat_id is given, it will return all the messages from that chat.

        Parameters
        ----------
        chat_id: Text
            The chat id to get the messages from.

        message_id: Text
            The message id to get the message from.

        Returns
        -------
        List[Dict[Text, Any]]
            The messages from the Microsoft Graph API.
        """

        api_client = self.handler.connect()

        # if both chat_id and message_id are given, get the message with that id from the API
        if message_id and chat_id:
            chat_messages = [api_client.get_chat_message(chat_id, message_id)]
        # if only the chat_id is given, get all the messages from that chat
        elif chat_id:
            chat_messages = api_client.get_chat_messages(chat_id)
        # if no parameters are given or only the message_id is given, get all the messages from all the chats
        else:
            chat_messages = api_client.get_all_chat_messages()

        return chat_messages

    def insert(self, query: ast.Insert) -> None:
        """
        Inserts data into the "POST /chats/{chat_id}/messages" Microsoft Graph API endpoint.

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

        # only the chatId, subject and body_content columns are supported
        # chatId and body_content are mandatory
        insert_statement_parser = INSERTQueryParser(
            query,
            supported_columns=["chatId", "subject", "body_content"],
            mandatory_columns=["chatId", "body_content"],
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
        """

        api_client = self.handler.connect()

        # send each message through the API to the chat with the given id
        for message in messages_to_send:
            api_client.send_chat_message(
                chat_id=message["chatId"],
                message=message["body_content"],
                subject=message.get("subject")
            )

    def get_columns(self) -> List[Text]:
        """
        Returns the columns of the Chat Messages Table.

        Returns
        -------
        List[Text]
            The columns of the Chat Messages Table.
        """

        return ms_teams_handler_config.CHAT_MESSAGES_TABLE_COLUMNS

    class ChatMessageRepliesTable(APITable):
        """
        The Microsoft Chat Message Replies Table implementation.
        """
        pass

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

        # only the = operator is supported for the id and teamId columns
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
                
        channels_df = pd.json_normalize(self.get_channels(channel_id, team_id))

        # if both channel_id and team_id are given, remove the id and teamId columns from the where conditions as they have already been evaluated in the API call (get_channels)
        if channel_id and team_id:
            where_conditions = [where_condition for where_condition in where_conditions if where_condition[1] not in ['id', 'teamId']]

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

        # if both channel_id and team_id are given, get the channel with that id from the API
        if channel_id and team_id:
            return [api_client.get_channel(team_id, channel_id)]
        # if no parameter are given or only the team_id is given, get all the channels
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

        # only the = operator is supported for the id, channelIdentity_teamId and channelIdentity_channelId columns
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
                    channel_id = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for teamId column.")
                
        messages_df = pd.json_normalize(self.get_messages(team_id, channel_id, message_id), sep='_')

        # if all parameters are given, remove the id, channelIdentity_teamId and channelIdentity_channelId columns from the where conditions as they have already been evaluated in the API call (get_messages)
        if team_id and channel_id and message_id:
            where_conditions = [where_condition for where_condition in where_conditions if where_condition[1] not in ['id', 'channelIdentity_teamId', 'channelIdentity_channelId']]

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

        # if all parameters are given, get the message with that id from that channel from that team from the API
        if message_id and channel_id and team_id:
            channel_message = api_client.get_channel_message(team_id, channel_id, message_id)
            # add the missing eventDetail attribute to the channel message
            channel_message['eventDetail'] = {
                '@odata.type': None, 
                'visibleHistoryStartDateTime': None, 
                'members': None,
                'channelId': None,
                'channelDisplayName': None,
                'initiator': {
                    'application': {
                        '@odata.type': None, 
                        'id': None,
                        'displayName': None,
                        'applicationIdentityType': None
                    },
                    'device': None, 
                    'user': {
                        '@odata.type': None, 
                        'id': None,
                        'displayName': None,
                        'userIdentityType': None,
                        'tenantId': None
                    }
                }
            }

            return [channel_message]
        # for any other combination of parameters, get all the messages from all the channels from all the teams
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

        # only the channelIdentity_teamId, channelIdentity_channelId, subject and body_content columns are supported
        # channelIdentity_teamId and channelIdentity_channelId are mandatory
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

        # send each message through the API to the channel with the given id from the team with the given id
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

        return ms_teams_handler_config.CHANNEL_MESSAGES_TABLE_COLUMNS

class ChannelMessageRepliesTable(APITable):
    """
    The Microsoft Teams Channel Message Replies Table implementation.
    """
    pass
