import datetime as dt
from typing import Any, Dict, List, Text

from mindsdb_sql_parser.ast import Delete, Insert, Update
import pandas as pd
from slack_sdk.errors import SlackApiError

from mindsdb.integrations.libs.api_handler import APIResource
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions, FilterCondition, FilterOperator
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class SlackConversationsTable(APIResource):
    """
    This is the table abstraction for interacting with conversations via the Slack API.
    """

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        **kwargs: Any
    ) -> pd.DataFrame:
        """
        Retrieves a list of Slack conversations based on the specified conditions.

        Args:
            conditions (List[FilterCondition]): The conditions to filter the conversations.
            limit (int): The limit of the conversations to return.
            kwargs(Any): Arbitrary keyword arguments.

        Returns:
            pd.DataFrame: The list of conversations.
        """
        channels = []
        for condition in conditions:
            value = condition.value
            op = condition.op

            # If a conversation id is provided, get the channel data for the ID(s).
            if condition.column == 'id':
                if op not in [FilterOperator.EQUAL, FilterOperator.IN]:
                    raise ValueError(f"Unsupported operator '{op}' for column 'id'")
                
                if op == FilterOperator.EQUAL:
                    try:
                        channels = [self.get_channel(value)]
                        condition.applied = True
                    except ValueError:
                        raise
                    
                if op == FilterOperator.IN:
                    try:
                        channels = self._get_channels(
                            value if isinstance(value, list) else [value]
                        )
                        condition.applied = True
                    except ValueError:
                        raise

        # If no channel ID(s) are provided, get all channels with the specified limit.
        if not channels:
            channels = self.get_all_channels(limit)

        for channel in channels:
            channel['created_at'] = dt.datetime.fromtimestamp(channel['created'])
            channel['updated_at'] = dt.datetime.fromtimestamp(channel['updated'] / 1000)

        return pd.DataFrame(channels, columns=self.get_columns())
    
    def get_channel(self, channel_id: Text) -> Dict:
        """
        Get the channel data for the specified channel id.

        Args:
            channel_id (Text): The channel id.

        Returns:
            Dict: The channel data.
        """
        client = self.connect()

        try:
            response = client.conversations_info(channel=channel_id)
        except SlackApiError as slack_error:
            logger.error(f"Error getting channel '{channel_id}': {slack_error.response['error']}")
            raise ValueError(f"Channel '{channel_id}' not found")

        return response['channel']
    
    def get_channels(self, channel_ids: List[Text]) -> List[Dict]:
        """
        Get the channel data for multiple channel ids.
        As it is unlikely that a large number of channels will be provided, the API rate limits are ignored here.

        Args:
            channel_ids (List[Text]): The list of channel ids.

        Returns:
            List[Dict]: The list of channel data.
        """
        channels = []
        for channel_id in channel_ids:
            try:
                channel = self.get_channel(channel_id)
                channels.append(channel)
            except SlackApiError as slack_error:
                logger.error(f"Error getting channel '{channel_id}': {slack_error.response['error']}")
                raise ValueError(f"Channel '{channel_id}' not found")
                
        return channels

    def get_all_channels(self, limit: int = None) -> List[Dict]:
        """
        Get the list of channels with a limit.
        If the provided limit is greater than 1000, provide no limit to the API call and paginate the results until the limit is reached.
        Otherwise, provide the limit or a default limit of 1000 to the API call.

        Args:
            limit (int): The limit of channels to return.

        Returns:
            List[Dict]: The list of channels.
        """
        client = self.connect()

        try:
            if limit and limit > 1000:
                response = client.conversations_list()
                channels = response['channels']

                while response['response_metadata']['next_cursor']:
                    response = client.conversations_list(cursor=response['response_metadata']['next_cursor'])
                    channels.extend(response['channels'])
                    if len(channels) >= limit:
                        break

                channels = channels[:limit]
            else:
                response = client.conversations_list(limit=limit if limit else 1000)
                channels = response['channels']
        except SlackApiError as slack_error:
            logger.error(f"Error getting channels: {slack_error.response['error']}")
            raise ValueError(f"Error getting channels: {slack_error.response['error']}")

        return channels

    def get_columns(self) -> List[str]:
        """
        Retrieves the attributes (columns) of the Slack conversations.

        Returns:
            List[str]: The list of columns.
        """
        return [
            'id',
            'name',
            'is_channel',
            'is_group',
            'is_im',
            'is_mpim',
            'is_private',
            'is_archived',
            'is_general',
            'is_shared',
            'is_ext_shared',
            'is_org_shared',
            'creator',
            'created_at',
            'updated_at',
        ]


class SlackMessagesTable(APIResource):
    """
    This is the table abstraction for interacting with messages via the Slack API.
    """

    def list(self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        **kwargs: Any
    ) -> pd.DataFrame:
        """
        Retrieves a list of messages from Slack conversations based on the specified conditions.

        Args:
            conditions (List[FilterCondition]): The conditions to filter the messages.
            limit (int): The limit of the messages to return.
            kwargs (Any): Arbitrary keyword arguments.

        Returns:
            pd.DataFrame: The list of messages.
        """
        client = self.handler.connect()

        # Build the parameters for the call to the Slack API.
        params = {}
        for condition in conditions:
            value = condition.value
            op = condition.op

            if condition.column == 'channel_id':
                if op != FilterOperator.EQUAL:
                    raise ValueError(f"Unsupported operator '{op}' for column 'channel_id'")

                # Check if the provided channel exists.
                try:
                    channel = SlackConversationsTable(self.handler).get_channel(value)
                    params['channel'] = value
                    condition.applied = True
                except SlackApiError:
                    raise ValueError(f"Channel '{value}' not found")

            # Handle the column 'created_at'.
            elif condition.column == 'created_at' and value is not None:
                date = dt.datetime.fromisoformat(value).replace(tzinfo=dt.timezone.utc)
                if op == FilterOperator.GREATER_THAN:
                    params['oldest'] = date.timestamp() + 1
                elif op == FilterOperator.GREATER_THAN_OR_EQUAL:
                    params['oldest'] = date.timestamp()
                elif op == FilterOperator.LESS_THAN_OR_EQUAL:
                    params['latest'] = date.timestamp()
                else:
                    continue
                condition.applied = True

        if limit:
            params['limit'] = limit

        if 'channel' not in params:
            raise Exception("To retrieve data from Slack, you need to provide the 'channel_id' parameter.")

        # Retrieve the conversation history.
        # TODO: Add support for pagination.
        result = client.conversations_history(**params)

        # Convert the SlackResponse object to a DataFrame.
        result = pd.DataFrame(result['messages'], columns=self.get_columns())

        # Remove null rows from the result.
        result = result[result['text'].notnull()]

        # Add the selected channel name to the response.
        result['channel_id'] = params['channel']
        result['channel_name'] = channel['name'] if 'name' in channel else None

        # Translate the time stamp into a 'created_at' field.
        result['created_at'] = pd.to_datetime(result['ts'].astype(float), unit='s').dt.strftime('%Y-%m-%d %H:%M:%S')

        return result

    def insert(self, query: Insert):
        """
        Executes an INSERT SQL query represented by an ASTNode object and posts a message to a Slack channel.

        Args:
            query (Insert): An ASTNode object representing the SQL query to be executed.
        """
        client = self.handler.connect()
    
        # Get column names and values from the query.
        columns = [col.name for col in query.columns]
        for row in query.values:
            params = dict(zip(columns, row))

            # Check if required parameters are provided.
            if 'channel_id' not in params or 'text' not in params:
                raise ValueError("To insert data into Slack, you need to provide the 'channel_id' and 'text' parameters.")

            try:
                response = client.chat_postMessage(
                    channel=params['channel_id'],
                    text=params['text']
                )
            except SlackApiError as slack_error:
                logger.error(f"Error posting message to Slack channel '{params['channel_id']}': {slack_error.response['error']}")
                raise Exception(f"Error posting message to Slack channel '{params['channel_id']}': {slack_error.response['error']}")
            
            # TODO: Is this necessary?
            inserted_id = response['ts']
            params['ts'] = inserted_id

    def update(self, query: Update):
        """
        Executes an UPDATE SQL query represented by an ASTNode object and updates a message in a Slack channel.

        Args:
            query (Update): An ASTNode object representing the SQL query to be executed.
        """
        client = self.handler.connect()

        conditions = extract_comparison_conditions(query.where)

        # Build the parameters for the call to the Slack API.
        params = {}
        for op, arg1, arg2 in conditions:
            if arg1 == 'channel_id':
                # Check if the provided channel exists.
                try:
                    SlackConversationsTable(self.handler).get_channel(arg2)
                    params['channel'] = arg2
                except SlackApiError as slack_error:
                    logger.error(f"Error getting channel '{arg2}': {slack_error.response['error']}")
                    raise ValueError(f"Channel '{arg2}' not found")

            if arg1 in ['text', 'ts']:
                if op == '=':
                    params[arg1] = arg2
                else:
                    raise ValueError(f"Unsupported operator '{op}' for column '{arg1}'")
                
            else:
                raise ValueError(f"Unsupported column '{arg1}'")

        # Check if required parameters are provided.
        if 'channel' not in params or 'ts' not in params or 'text' not in params:
            raise Exception("To update a message in Slack, you need to provide the 'channel', 'ts', and 'text' parameters.")

        try:
            client.chat_update(
                channel=params['channel'],
                ts=str(params['ts']),
                text=params['text'].strip()
            )
        except SlackApiError as slack_error:
            logger.error(f"Error updating message in Slack channel '{params['channel']}' with timestamp '{params['ts']}': {slack_error.response['error']}")
            raise Exception(f"Error updating message in Slack channel '{params['channel']}' with timestamp '{params['ts']}': {slack_error.response['error']}")

    def delete(self, query: Delete):
        """
        Executes a DELETE SQL query represented by an ASTNode object and deletes a message from a Slack channel.

        Args:
            query (Delete): An ASTNode object representing the SQL query to be executed.
        """
        client = self.handler.connect()

        conditions = extract_comparison_conditions(query.where)

        # Build the parameters for the call to the Slack API.
        params = {}
        for op, arg1, arg2 in conditions:
            if arg1 == 'channel_id':
                # Check if the provided channel exists.
                try:
                    SlackConversationsTable(self.handler).get_channel(arg2)
                    params['channel'] = arg2
                except SlackApiError as slack_error:
                    logger.error(f"Error getting channel '{arg2}': {slack_error.response['error']}")
                    raise ValueError(f"Channel '{arg2}' not found")

            if arg1 == 'ts':
                if op == '=':
                    params['ts'] = float(arg2)
                else:
                    raise NotImplementedError(f'Unknown op: {op}')

            else:
                raise ValueError(f"Unsupported column '{arg1}'")

        # Check if required parameters are provided.
        if 'channel' not in params or 'ts' not in params:
            raise Exception("To delete a message from Slack, you need to provide the 'channel' and 'ts' parameters.")

        try:
            client.chat_delete(
                channel=params['channel'],
                ts=params['ts']
            )
            
        except SlackApiError as slack_error:
            logger.error(f"Error deleting message in Slack channel '{params['channel']}' with timestamp '{params['ts']}': {slack_error.response['error']}")
            raise Exception(f"Error deleting message in Slack channel '{params['channel']}' with timestamp '{params['ts']}': {slack_error.response['error']}")
        
    def get_columns(self) -> List[Text]:
        """
        Retrieves the attributes (columns) of the Slack messages.

        Returns:
            List[str]: The list of columns.
        """
        return [
            'channel_id',
            'channel',
            'client_msg_id',
            'type',
            'subtype',
            'ts',
            'created_at',
            'user',
            'text',
            'attachments',
            'files',
            'reactions',
            'thread_ts',
            'reply_count',
            'reply_users_count',
            'latest_reply',
            'reply_users'
        ]
        

class SlackThreadsTable(APIResource):
    """
    This is the table abstraction for interacting with threads in Slack conversations.
    """

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        **kwargs: Any
    ) -> pd.DataFrame:
        """
        Retrieves a list of messages in a thread based on the specified conditions.

        Args:
            conditions (List[FilterCondition]): The conditions to filter the messages.
            limit (int): The limit of the messages to return.
            kwargs (Any): Arbitrary keyword arguments.

        Returns:
            pd.DataFrame: The messages in the thread.
        """
        client = self.handler.connect()

        # Build the parameters for the call to the Slack API.
        params = {}
        for condition in conditions:
            value = condition.value
            op = condition.op

            # Handle the column 'channel_id'.
            if condition.column == 'channel_id':
                if op != FilterOperator.EQUAL:
                    raise ValueError(f"Unsupported operator '{op}' for column 'channel_id'")

                # Check if the provided channel exists.
                try:
                    channel = SlackConversationsTable(self.handler).get_channel(value)
                    params['channel'] = value
                    condition.applied = True
                except SlackApiError as slack_error:
                    logger.error(f"Error getting channel '{value}': {slack_error.response['error']}")
                    raise ValueError(f"Channel '{value}' not found")

            # Handle the column 'thread_ts'.
            elif condition.column == 'thread_ts':
                if op != FilterOperator.EQUAL:
                    raise ValueError(f"Unsupported operator '{op}' for column 'thread_ts'")

                params['ts'] = value

        if limit:
            params['limit'] = limit

        if 'channel' not in params:
            raise Exception("To retrieve data from Slack, you need to provide the 'channel_id' parameter.")

        # Retrieve the replies in the thread.
        # TODO: Add support for pagination.
        result = client.conversations_replies(**params)

        # Convert the result to a DataFrame.
        result = pd.DataFrame(result['messages'], columns=self.get_columns())

        # Remove null rows from the result.
        result = result[result['text'].notnull()]

        # Add the selected channel to the DataFrame.
        result['channel_id'] = params['channel']
        result['channel_name'] = channel['name'] if 'name' in channel else None

        return result
    
    def insert(self, query: Insert):
        """
        Executes an INSERT SQL query represented by an ASTNode object and posts a message to a Slack thread.

        Args:
            query (Insert): An ASTNode object representing the SQL query to be executed.

        Raises:
            ValueError: If the 'channel_id', 'text', or 'thread_ts' parameters are not provided.
        """
        client = self.handler.connect()
    
        # Get column names and values from the query.
        columns = [col.name for col in query.columns]
        for row in query.values:
            params = dict(zip(columns, row))

            # Check if required parameters are provided.
            if 'channel_id' not in params or 'text' not in params or 'thread_ts' not in params:
                raise ValueError("To insert data into Slack, you need to provide the 'channel_id', 'text', and 'thread_ts' parameters.")

            try:
                client.chat_postMessage(
                    channel=params['channel_id'],
                    text=params['text'],
                    thread_ts=params['thread_ts']
                )
            except SlackApiError as slack_error:
                logger.error(f"Error posting message to Slack channel '{params['channel_id']}': {slack_error.response['error']}")
                raise Exception(f"Error posting message to Slack channel '{params['channel_id']}': {slack_error.response['error']}")

    def get_columns(self) -> List[Text]:
        """
        Retrieves the attributes (columns) of the Slack threads.

        Returns:
            List[Text]: The list of columns.
        """
        return [
            "channel_id",
            "channel_name",
            "type",
            "user",
            "text",
            "ts",
            "client_msg_id",
            "thread_ts",
            "parent_user_id",
            "reply_count",
            "reply_users_count",
            "latest_reply",
            "reply_users"
        ]
    

class SlackUsersTable(APIResource):
    """
    This is the table abstraction for interacting with users in Slack.
    """

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        **kwargs: Any
    ) -> pd.DataFrame:
        """
        Retrieves a list of users based on the specified conditions.

        Args:
            conditions (List[FilterCondition]): The conditions to filter the users.
            limit (int): The limit of the users to return.
            kwargs (Any): Arbitrary keyword arguments.
        """
        client = self.handler.connect()

        # TODO: Add support for pagination.
        users = client.users_list(limit=limit).data['members']

        return pd.DataFrame(users, columns=self.get_columns())

    def get_columns(self) -> List[Text]:
        """
        Retrieves the attributes (columns) of the Slack users.

        Returns:
            List[Text]: The list of columns.
        """
        return [
            'id',
            'name',
            'real_name'
        ]