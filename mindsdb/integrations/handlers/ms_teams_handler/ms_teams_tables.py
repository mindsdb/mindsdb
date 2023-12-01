from mindsdb_sql.parser.ast import ASTNode
import pandas as pd
from typing import Text, List, Dict, Any

from mindsdb_sql.parser import ast
from mindsdb.integrations.libs.api_handler import APITable

from mindsdb.integrations.handlers.utilities.query_utilities.insert_query_utilities import INSERTQueryParser
from mindsdb.integrations.handlers.utilities.query_utilities.select_query_utilities import SELECTQueryParser, SELECTQueryExecutor

from mindsdb.utilities import log

logger = log.getLogger(__name__)


class ChannelMessagesTable(APITable):
    """The Microsoft Teams Messages Table implementation"""
     
    def select(self, query: ASTNode) -> pd.DataFrame:
        """Pulls data from the Microsoft Teams "GET /teams/{group_id}/channels/{channel_id}/messages" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Microsoft Teams Messages matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        select_statement_parser = SELECTQueryParser(
            query,
            'messages',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        messages_df = pd.json_normalize(self.get_messages(), sep='_')

        return messages_df
    
    def get_messages(self):
        api_client = self.handler.connect()
        # TODO: Should these records be filtered somehow?
        return api_client.get_channel_messages()
    
    def insert(self, query: ASTNode) -> None:
        """Inserts data into the Microsoft Teams "POST /teams/{group_id}/channels/{channel_id}/messages" API endpoint.

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
            supported_columns=["channelIdentity_teamId", "channelIdentity_channelId", "subject", "body_content"],
            mandatory_columns=["channelIdentity_teamId", "channelIdentity_channelId", "body_content"],
        )

        messages_to_send = insert_statement_parser.parse_query()

        self.send_messages(messages_to_send)

    def send_messages(self, messages_to_send: List[Dict[Text, Any]]) -> None:
        api_client = self.handler.connect()
        for message in messages_to_send:
            api_client.send_channel_message(
                group_id=message["channelIdentity_teamId"],
                channel_id=message["channelIdentity_channelId"],
                message=message["body_content"],
                subject=message.get("subject")
            )
    
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

class ChannelMessageRepliesTable(APITable):
    """The Microsoft Teams Message Replies Table implementation"""
    pass
            
class ChannelsTable(APITable):
    """The Microsoft Channels Table implementation"""

    def select(self, query: ASTNode) -> pd.DataFrame:
        """Pulls data from the Microsoft Teams "GET /teams/{group_id}/channels" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Microsoft Teams Channels matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
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

    def get_channels(self, channel_id = None, team_id: str = None) -> List[Dict[Text, Any]]:
        api_client = self.handler.connect()

        if channel_id and team_id:
            return [api_client.get_channel(team_id, channel_id)]
        else:
            return api_client.get_channels()
    
    def get_columns(self) -> List[Text]:
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