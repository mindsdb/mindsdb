import datetime as dt
from typing import List
import pandas as pd
from slack_sdk.errors import SlackApiError

from mindsdb.utilities import log

from mindsdb_sql_parser import ast
from mindsdb_sql_parser.ast import ASTNode
from mindsdb.integrations.libs.api_handler import APIResource
from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator

from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions

logger = log.getLogger(__name__)


class SlackConversationsTable(APIResource):

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        **kwargs
    ) -> pd.DataFrame:
        channels = []
        for condition in conditions:
            value = condition.value
            op = condition.op

            if condition.column == 'id':
                if op not in [FilterOperator.EQUAL, FilterOperator.IN]:
                    raise ValueError(f"Unsupported operator '{op}' for column 'id'")
                
                if op == FilterOperator.EQUAL:
                    try:
                        channels = [self.handler.get_channel(value)]
                        condition.applied = True
                    except ValueError:
                        raise
                    
                if op == FilterOperator.IN:
                    try:
                        channels = self.handler.get_channels(
                            value if isinstance(value, list) else [value]
                        )
                        condition.applied = True
                    except ValueError:
                        raise

        if not channels:
            channels = self.handler.get_limited_channels(limit)

        for channel in channels:
            channel['created_at'] = dt.datetime.fromtimestamp(channel['created'])
            channel['updated_at'] = dt.datetime.fromtimestamp(channel['updated'] / 1000)

        return pd.DataFrame(channels, columns=self.get_columns())

    def get_columns(self) -> List[str]:
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


class SlackUsersTable(APIResource):

    def list(self, **kwargs) -> pd.DataFrame:
        """
        Retrieves list of users

        The "users.list" call is used in slack api

        :return: pd.DataFrame
        """

        client = self.handler.connect()

        users = client.users_list().data['members']

        return pd.DataFrame(users, columns=self.get_columns())

    def get_columns(self) -> List[str]:
        return [
            'id',
            'name',
            'real_name'
        ]


class SlackMessagesTable(APIResource):

    def list(self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Retrieves the data from the channel using SlackAPI

        Returns:
            conversation_history, pd.DataFrame
        """

        client = self.handler.connect()

        # override the default function
        def parse_utc_date(date_str):
            date_obj = dt.datetime.fromisoformat(date_str).replace(tzinfo=dt.timezone.utc)
            return date_obj

        # Build the filters and parameters for the query
        params = {}

        for condition in conditions:
            value = condition.value
            op = condition.op

            if condition.column == 'channel_id':
                if op != FilterOperator.EQUAL:
                    raise ValueError(f"Unsupported operator '{op}' for column 'channel_id'")

                # Check if the channel exists
                try:
                    channel = self.handler.get_channel(value)
                    params['channel'] = value
                    condition.applied = True
                except SlackApiError as e:
                    raise ValueError(f"Channel '{value}' not found")

            elif condition.column == 'created_at' and value is not None:
                date = parse_utc_date(value)
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

        # Retrieve the conversation history
        result = client.conversations_history(**params)

        # convert SlackResponse object to pandas DataFrame
        result = pd.DataFrame(result['messages'], columns=self.get_columns())

        # Remove null rows from the result
        result = result[result['text'].notnull()]

        # Add the selected channel to the dataframe
        result['channel_id'] = params['channel']
        result['channel_name'] = channel['name'] if 'name' in channel else None

        # translate the time stamp into a 'created_at' field
        result['created_at'] = pd.to_datetime(result['ts'].astype(float), unit='s').dt.strftime('%Y-%m-%d %H:%M:%S')

        return result

    def get_columns(self) -> List[str]:
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

    def insert(self, query):
        """
        Inserts the message in the Slack Channel

        Args:
            channel_name
            message
        """
        client = self.handler.connect()
    
        # get column names and values from the query
        columns = [col.name for col in query.columns]
        for row in query.values:
            params = dict(zip(columns, row))

            # check if required parameters are provided
            if 'channel_id' not in params or 'text' not in params:
                raise Exception("To insert data into Slack, you need to provide the 'channel_id' and 'text' parameters.")

            # post message to Slack channel
            try:
                response = client.chat_postMessage(
                    channel=params['channel_id'],
                    text=params['text']
                )
            except SlackApiError as e:
                raise Exception(f"Error posting message to Slack channel '{params['channel']}': {e.response['error']}")
            
            inserted_id = response['ts']
            params['ts'] = inserted_id

    def update(self, query: ast.Update):
        """
        Updates the message in the Slack Channel

        Args:
            updated message
            channel_name
            ts  [TimeStamp -> Can be found by running select command, the entire result will be printed in the terminal]
        """
        client = self.handler.connect()

        # Extract comparison conditions from the query
        conditions = extract_comparison_conditions(query.where)

        filters = []
        params = {}

        keys = list(query.update_columns.keys())

        # Build the filters and parameters for the query
        for op, arg1, arg2 in conditions:
            if arg1 == 'channel_id':
                # Check if the channel exists
                try:
                    self.handler.get_channel(arg2)
                    params['channel'] = arg2
                except SlackApiError as e:
                    raise ValueError(f"Channel '{arg2}' not found")

            if keys[0] == 'text':
                params['text'] =  str(query.update_columns['text'])
            else:
                raise ValueError(f"Message '{arg2}' not found")

            if arg1 == 'ts':
                if op == '=':
                    params['ts'] = float(arg2)
                else:
                    raise NotImplementedError(f'Unknown op: {op}')
            else:
                filters.append([op, arg1, arg2])

        # check if required parameters are provided
        if 'channel' not in params or 'ts' not in params or 'text' not in params:
            raise Exception("To update a message in Slack, you need to provide the 'channel', 'ts', and 'text' parameters.")

        # update message in Slack channel
        try:
            response = client.chat_update(
                channel=params['channel'],
                ts=str(params['ts']),
                text=params['text'].strip()
            )
        except SlackApiError as e:
            raise Exception(f"Error updating message in Slack channel '{params['channel']}' with timestamp '{params['ts']}' and message '{params['text']}': {e.response['error']}")

    def delete(self, query: ASTNode):
        """
        Deletes the message in the Slack Channel

        Args:
            channel_name
            ts  [TimeStamp -> Can be found by running select command, the entire result will be printed in the terminal]
        """
        client = self.handler.connect()

        # Extract comparison conditions from the query
        conditions = extract_comparison_conditions(query.where)

        filters = []
        params = {}

        # Build the filters and parameters for the query
        for op, arg1, arg2 in conditions:
            if arg1 == 'channel_id':
                # Check if the channel exists
                try:
                    self.handler.get_channel(arg2)
                    params['channel'] = arg2
                except SlackApiError as e:
                    raise ValueError(f"Channel '{arg2}' not found")

            if arg1 == 'ts':
                if op == '=':
                    params['ts'] = float(arg2)
                else:
                    raise NotImplementedError(f'Unknown op: {op}')

            else:
                filters.append([op, arg1, arg2])

        # check if required parameters are provided
        if 'channel' not in params or 'ts' not in params:
            raise Exception("To delete a message from Slack, you need to provide the 'channel' and 'ts' parameters.")

        # delete message from Slack channel
        try:
            response = client.chat_delete(
                channel=params['channel'],
                ts=params['ts']
            )
            
        except SlackApiError as e:
            raise Exception(f"Error deleting message from Slack channel '{params['channel']}' with timestamp '{params['ts']}': {e.response['error']}")
        

class SlackThreadsTable(APIResource):

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Retrieves the messages from a thread in a Slack conversation.

        Returns:
            pd.DataFrame: The messages in the thread.
        """
        client = self.handler.connect()

        params = {}

        # Parse the conditions.
        for condition in conditions:
            value = condition.value
            op = condition.op

            # Handle the column 'channel_id'.
            if condition.column == 'channel_id':
                if op != FilterOperator.EQUAL:
                    raise ValueError(f"Unsupported operator '{op}' for column 'channel_id'")

                # Check if the channel exists.
                try:
                    channel = self.handler.get_channel(value)
                    params['channel'] = value
                    condition.applied = True
                except SlackApiError as e:
                    raise ValueError(f"Channel '{value}' not found")

            # Handle the column 'thread_ts'.
            elif condition.column == 'thread_ts':
                if op != FilterOperator.EQUAL:
                    raise ValueError(f"Unsupported operator '{op}' for column 'thread_ts'")

                params['ts'] = value

        # Set the limit.
        if limit:
            params['limit'] = limit

        if 'channel' not in params:
            raise Exception("To retrieve data from Slack, you need to provide the 'channel_id' parameter.")

        # Retrieve the replies in the thread.
        result = client.conversations_replies(**params)

        # Convert the result to a DataFrame.
        result = pd.DataFrame(result['messages'], columns=self.get_columns())

        # Remove null rows from the result.
        result = result[result['text'].notnull()]

        # Add the selected channel to the DataFrame.
        result['channel_id'] = params['channel']
        result['channel_name'] = channel['name'] if 'name' in channel else None

        return result
    
    def insert(self, query):
        """
        Inserts a message into a thread in a Slack conversation.
        """
        client = self.handler.connect()
    
        # Get column names and values from the query.
        columns = [col.name for col in query.columns]
        for row in query.values:
            params = dict(zip(columns, row))

            # Check if required parameters are provided.
            if 'channel_id' not in params or 'text' not in params or 'thread_ts' not in params:
                raise Exception("To insert data into Slack, you need to provide the 'channel_id', 'text', and 'thread_ts' parameters.")

            # Post message to Slack thread.
            try:
                response = client.chat_postMessage(
                    channel=params['channel_id'],
                    text=params['text'],
                    thread_ts=params['thread_ts']
                )
            except SlackApiError as e:
                raise Exception(f"Error posting message to Slack channel '{params['channel']}': {e.response['error']}")

    def get_columns(self) -> List[str]:
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