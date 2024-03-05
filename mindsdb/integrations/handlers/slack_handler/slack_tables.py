from mindsdb.utilities import log
from datetime import datetime as datetime, timezone
from mindsdb_sql.parser import ast
import pandas as pd
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from mindsdb_sql.parser.ast import ASTNode
from mindsdb.integrations.libs.api_handler import APITable

from mindsdb.integrations.utilities.sql_utils import (
    extract_comparison_conditions,
    filter_dataframe
)

from mindsdb.integrations.libs.response import HandlerResponse as Response

logger = log.getLogger(__name__)


class SlackChannelsTable(APITable):
    def __init__(self, handler):
        """
        Checks the connection is active
        """
        super().__init__(handler)
        self.client = WebClient(token=self.handler.connection_args['token'])

    def select(self, query: ast.Select) -> Response:
        """
        Retrieves the data from the channel using SlackAPI

        Args:
            channel_name

        Returns:
            conversation_history
        """
        # override the default function
        def parse_utc_date(date_str):
            date_obj = datetime.fromisoformat(date_str).replace(
                tzinfo=timezone.utc
            )
            return date_obj.strftime('%Y-%m-%d %H:%M:%S')

        # Get the channels list and ids
        channels = self.client.conversations_list(types="public_channel,\
            private_channel")['channels']
        channel_ids = {c['name']: c['id'] for c in channels}

        # Extract comparison conditions from the query
        conditions = extract_comparison_conditions(query.where)
        channel_name = conditions[0][2]
        filters = []
        params = {}
        order_by_conditions = {}

        # Build the filters and parameters for the query
        for op, arg1, arg2 in conditions:
            if arg1 == 'channel':
                if arg2 in channel_ids:
                    params['channel'] = channel_ids[arg2]
                    channel_name = arg2
                else:
                    raise ValueError(f"Channel '{arg2}' not found")

            elif arg1 == 'limit':
                if op == '=': 
                    params['limit'] = int(arg2)
                else:
                    raise NotImplementedError(f'Unknown op: {op}')
             

            elif arg1 == 'created_at':
                date = parse_utc_date(arg2)
                if op == '>':
                    params['start_time'] = date
                elif op == '<':
                    params['end_time'] = date
                else:
                    raise NotImplementedError

            else:
                filters.append([op, arg1, arg2])

        if query.limit:
            params['limit'] = int(query.limit.value)

        if query.order_by and len(query.order_by) > 0:
            order_by_conditions["columns"] = []
            order_by_conditions["ascending"] = []

            for an_order in query.order_by:
                if an_order.field.parts[1] == "messages":
                    order_by_conditions["columns"].append("messages")

                    if an_order.direction == "ASC":
                        order_by_conditions["ascending"].append(True)
                    else:
                        order_by_conditions["ascending"].append(False)
                else:
                    raise ValueError(
                        f"Order by unknown column {an_order.field.parts[1]}"
                    )

        # Retrieve the conversation history
        try:
            result = self.client.conversations_history(channel=params['channel'])
        except SlackApiError as e:
            logger.error("Error creating conversation: {}".format(e))
            raise e

        # Get columns for the query and convert SlackResponse object to pandas DataFrame
        columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                columns = [
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
                    'reply_users',
                    'threads'
                ]
                break
            elif isinstance(target, ast.Identifier):
                columns.append(target.parts[-1])
                logger.warning(target)
            else:
                raise NotImplementedError

        # columns to lower case
        columns = [name.lower() for name in columns]

        # convert SlackResponse object to pandas DataFrame
        result = pd.DataFrame(result['messages'])

         
        # Remove null rows from the result
        result = result[result['text'].notnull()]

        # add absent columns
        for col in set(columns) & set(result.columns) ^ set(columns):
            result[col] = None

        # Add the selected channel to the dataframe
        result['channel'] = channel_name

        # translate the time stamp into a 'created_at' field
        result['ts_datetime'] = pd.to_datetime(result['ts'].astype(float), unit='s')
        result['created_at'] = result['ts_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')

        # if we have 'created_at' attribute, then execute this
        if 'start_time' in params:
            result = result[result['ts_datetime'] > datetime.strptime(params['start_time'], '%Y-%m-%d %H:%M:%S')]
        elif 'end_time' in params:
            result = result[result['ts_datetime'] < datetime.strptime(params['end_time'], '%Y-%m-%d %H:%M:%S')]

        # filter by columns to be returned
        result = result[columns]

        # Sort the data based on order_by_conditions
        if len(order_by_conditions.get("columns", [])) > 0:
            result = result.sort_values(
                by=order_by_conditions["columns"],
                ascending=order_by_conditions["ascending"],
            )

        # Limit the result based on the query limit
        if query.limit:
            result = result.head(query.limit.value)

        # Alias the target column based on the query
        for target in query.targets:
            if target.alias:
                result.rename(
                    columns={target.parts[-1]: str(target.alias)},
                    inplace=True
                )

        result = filter_dataframe(result, filters)

        # ensure the data in the table is of string type
        return result.astype(str)

    def insert(self, query):
        """
        Inserts the message in the Slack Channel

        Args:
            channel_name
            message
        """

        # get column names and values from the query
        columns = [col.name for col in query.columns]
        for row in query.values:
            params = dict(zip(columns, row))

            # check if required parameters are provided
            if 'channel' not in params or 'text' not in params:
                raise Exception("To insert data into Slack, you need to \
                    provide the 'channel' and 'text' parameters.")

            # post message to Slack channel
            try:
                response = self.client.chat_postMessage(
                    channel=params['channel'],
                    text=params['text']
                )
            except SlackApiError as e:
                raise Exception(f"Error posting message to Slack channel \
                    '{params['channel']}': {e.response['error']}")

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

        # Get the channels list and ids
        channels = self.client.conversations_list(types="public_channel,\
            private_channel")['channels']
        channel_ids = {c['name']: c['id'] for c in channels}

        # Extract comparison conditions from the query
        conditions = extract_comparison_conditions(query.where)

        filters = []
        params = {}

        keys = list(query.update_columns.keys())

        # Build the filters and parameters for the query
        for op, arg1, arg2 in conditions:
            if arg1 == 'channel':
                if arg2 in channel_ids:
                    params['channel'] = channel_ids[arg2]
                else:
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
            self.client.chat_update(
                channel=params['channel'],
                ts=str(params['ts']),
                text=params['text'].strip()
            )
        except SlackApiError as e:
            raise Exception(f"Error updating message in Slack channel \
                '{params['channel']}' with timestamp '{params['ts']}' and \
                    message '{params['text']}': {e.response['error']}")

    def delete(self, query: ASTNode):
        """
        Deletes the message in the Slack Channel

        Args:
            channel_name
            ts  [TimeStamp -> Can be found by running select command, the \
                entire result will be printed in the terminal]
        """
        # Get the channels list and ids
        channels = self.client.conversations_list(types="public_channel,\
            private_channel")['channels']
        channel_ids = {c['name']: c['id'] for c in channels}
        # Extract comparison conditions from the query
        conditions = extract_comparison_conditions(query.where)

        filters = []
        params = {}

        # Build the filters and parameters for the query
        for op, arg1, arg2 in conditions:
            if arg1 == 'channel':
                if arg2 in channel_ids:
                    params['channel'] = channel_ids[arg2]
                else:
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
            raise Exception("To delete a message from Slack, you need to \
                provide the 'channel' and 'ts' parameters.")

        # delete message from Slack channel
        try:
            self.client.chat_delete(
                channel=params['channel'],
                ts=params['ts']
            )

        except SlackApiError as e:
            raise Exception(f"Error deleting message from Slack channel \
            '{params['channel']}' with timestamp '{params['ts']}': \
            {e.response['error']}")


class SlackThreadsTable(SlackChannelsTable):
    def __init__(self, handler):
        """
        Checks the connection is active
        """
        super().__init__(handler)
        self.client = WebClient(token=self.handler.connection_args['token'])

    def select(self, query: ast.Select) -> Response:
        """
        Retrieves the data from the channel using SlackAPI

        Args:
            channel_name
            ts

        Returns:
            conversation_replies
        """
        # Get the channels list and ids
        channels = self.client.conversations_list(types="public_channel,private_channel")['channels']
        channel_ids = {c['name']: c['id'] for c in channels}

        # Extract comparison conditions from the query
        conditions = extract_comparison_conditions(query.where)
        channel_name = None
        filters = []
        params = {}
        order_by_conditions = {}

        # Build the filters and parameters for the query
        for op, arg1, arg2 in conditions:
            if arg1 == 'ts':
                if op == '=': 
                    params['ts'] = arg2
                else:
                    raise NotImplementedError(f'Unknown op: {op}')

            if arg1 == 'channel':
                if arg2 in channel_ids:
                    params['channel'] = channel_ids[arg2]
                    channel_name = arg2
                else:
                    raise ValueError(f"Channel '{arg2}' not found")

            elif arg1 == 'limit':
                if op == '=':
                    params['limit'] = int(arg2)
                else:
                    raise NotImplementedError(f'Unknown op: {op}')

            else:
                filters.append([op, arg1, arg2])

        if query.limit:
            params['limit'] = int(query.limit.value)

        if query.order_by and len(query.order_by) > 0:
            order_by_conditions["columns"] = []
            order_by_conditions["ascending"] = []

            for an_order in query.order_by:
                if an_order.field.parts[1] == "messages":
                    order_by_conditions["columns"].append("messages")

                    if an_order.direction == "ASC":
                        order_by_conditions["ascending"].append(True)
                    else:
                        order_by_conditions["ascending"].append(False)
                else:
                    raise ValueError(
                        f"Order by unknown column {an_order.field.parts[1]}"
                    )

        # Retrieve the conversation history
        try:
            result = self.client.conversations_replies(
                channel=params['channel'],
                ts=params['ts']
            )
        except SlackApiError as e:
            logger.error("Error creating conversation: {}".format(e))
            raise e

        # Get columns for the query and convert SlackResponse object to pandas DataFrame
        columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                columns = [
                    'client_msg_id',
                    'type',
                    'text',
                    'user',
                    'ts',
                    'team',
                    'thread_ts',
                    'reply_count',
                    'reply_users_count',
                    'latest_reply',
                    'reply_users',
                    'is_locked',
                    'subscribed'
                    ]
                break
            elif isinstance(target, ast.Identifier):
                columns.append(target.parts[-1])
                logger.warning(target)
            else:
                raise NotImplementedError

        # columns to lower case
        columns = [name.lower() for name in columns]

        # convert SlackResponse object to pandas DataFrame
        result = pd.DataFrame(result['messages'])

        # Remove null rows from the result
        result = result[result['text'].notnull()]

        # add absent columns
        for col in set(columns) & set(result.columns) ^ set(columns):
            result[col] = None

        # Add the selected channel to the dataframe
        result['channel'] = channel_name

        # filter by columns to be returned
        result = result[columns]

        # Sort the data based on order_by_conditions
        if len(order_by_conditions.get("columns", [])) > 0:
            result = result.sort_values(
                by=order_by_conditions["columns"],
                ascending=order_by_conditions["ascending"],
            )

        # Limit the result based on the query limit
        if query.limit:
            result = result.head(query.limit.value)

        # Alias the target column based on the query
        for target in query.targets:
            if target.alias:
                result.rename(columns={target.parts[-1]: str(target.alias)}, inplace=True)

        # ensure the data in the table is of string type
        return result.astype(str)

    def insert(self, query):
        """
        Inserts the message in the Slack Channel

        Args:
            channel_name
            ts
            message
        """

        # get column names and values from the query
        columns = [col.name for col in query.columns]
        for row in query.values:
            params = dict(zip(columns, row))

            # check if required parameters are provided
            if 'channel' not in params or 'text' not in params or 'ts' not in params:
                raise Exception("To insert data into Slack, you need to provide the 'channel' and 'text' and 'ts' parameters.")

            # post message to Slack channel
            try:
                response = self.client.chat_postMessage(
                    channel=params['channel'],
                    thread_ts=params['ts'],
                    text=params['text']
                )
            except SlackApiError as e:
                raise Exception(f"Error posting message to Slack channel '{params['channel']}': {e.response['error']}")

            inserted_id = response['ts']
            params['ts'] = inserted_id
