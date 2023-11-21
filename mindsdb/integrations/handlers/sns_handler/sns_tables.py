from typing import List

import pandas as pd
from mindsdb_sql.parser import ast
from mindsdb.integrations.handlers.utilities.query_utilities.insert_query_utilities import INSERTQueryParser
from mindsdb.integrations.libs.api_handler import APITable, APIHandler


class MessageTable(APITable):
    name: str = "messages"
    columns: List[str] = ['message', 'topic_arn', 'id','subject','message_deduplication_id', 'message_group_id']

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses

        Returns
        -------
        List[str]
            List of columns
        """
        return [
            "message"
        ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.handler.connect()

    def insert(self, query: ast.Insert) -> pd.DataFrame:    
        supported_columns={'id','subject','message_deduplication_id', 'message_group_id', 'message', 'topic_arn', 'message_attributes'}
        insert_statement_parser = INSERTQueryParser(
            query,
            mandatory_columns=['message', 'topic_arn'],
            supported_columns=list(supported_columns),
            all_mandatory=False
        )
        columns = [col.name for col in query.columns]
        if not set(columns).issubset(supported_columns):
            unsupported_columns = set(columns).difference(supported_columns)
            raise ValueError(
                "Unsupported columns for publish message: "
                + ", ".join(unsupported_columns)
            )
    
        message_rows = insert_statement_parser.parse_query()
        if 'id' not in  message_rows[0]:
           for  message_row in message_rows:
               message = message_row['message']
               topic_arn = message_row['topic_arn']
               param = {"message": message_row['message'], "topic_arn": message_row['topic_arn']  }
               self.handler.call_sns_api("publish_message", param)
               #self.handler.publish_message(topic_arn=topic_arn, message=message)
        else:
            request_entries = []
            topic_arn = ""
            message_ids_set = set()
            for message_row in message_rows:
                message = message_row['message']
                topic_arn = message_row['topic_arn']
                message_id = message_row['id']
                subject = str()
                message_group_id = str()
                message_deduplication_id = str()
                message_attributes = {}
                if 'subject' in message_row:
                   subject = message_row['subject']
                if 'message_deduplication_id' in message_row:
                   message_deduplication_id = message_row['message_deduplication_id']
                if 'message_group_id' in message_row:
                   message_group_id = message_row['message_group_id']
                if 'message_attributes' in message_row:
                    message_attributes = message_row['message_attributes']
                # check uniq ids ?
                if message_id in message_ids_set:
                    raise ValueError("Two or more batch entries in the request have the same Id")
                message_ids_set.add(message_id)                      
                request_entry={'Id': message_id, 'Message': message, 'Subject': subject, 'MessageDeduplicationId': message_deduplication_id, 'MessageGroupId': message_group_id, 'MessageAttributes': message_attributes }
                request_entries.append(request_entry)             
            self.handler.publish_batch(topic_arn=topic_arn, batch_request_entries=request_entries)
        


class TopicTable(APITable):
    name: str = "topics"
    columns: List[str] = ["TopicArn"]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        """triggered at the SELECT query

        Args:
            query (ast.Select): user's entered query

        Returns:
            pd.DataFrame: the queried information
        """
        topics = self.handler.topic_list()
        ids_list = []
        topics_arn_list = []
        id = 1
        for topic in topics:
            ids_list.append(id)
            topics_arn_list.append(topic["TopicArn"])
            id += 1
        data = {'id': ids_list, 'topic_arn': topics_arn_list}
        df = pd.DataFrame(data=data)
        return df

    def insert(self, query: ast.Insert) -> pd.DataFrame:
        mandatory_columns = {'name'}
        insert_statement_parser = INSERTQueryParser(
            query,
            mandatory_columns=list(mandatory_columns),
            all_mandatory=True
        )
        columns = [col.name for col in query.columns]
        if not set(columns).issubset(mandatory_columns):
            unsupported_columns = set(columns).difference(mandatory_columns)
            raise ValueError(
                "Unsupported columns for create topic: "
                + ", ".join(unsupported_columns)
            )
        topic_names = insert_statement_parser.parse_values()
        for topic_name in topic_names:
            self.handler.create_topic(name=topic_name[0])

    def get_columns(self, ignore: List[str] = []) -> List[str]:
        """columns

        Args:
            ignore (List[str], optional): exclusion items. Defaults to [].

        Returns:
            List[str]: available columns with `ignore` items removed from the list.
        """

        return [item for item in self.columns if item not in ignore]
