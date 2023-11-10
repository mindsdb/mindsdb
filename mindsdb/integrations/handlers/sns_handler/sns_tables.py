from typing import List

import pandas as pd
from mindsdb_sql.parser import ast
from mindsdb.integrations.handlers.utilities.query_utilities.insert_query_utilities import INSERTQueryParser

from mindsdb.integrations.libs.api_handler import APITable, APIHandler


class MessageTable(APITable):
    name: str = "messages"
    columns: List[str] = ["TopicArn", "Message"]

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
    
        insert_statement_parser = INSERTQueryParser(
            query,
            mandatory_columns=['message', 'TopicArn'],
            all_mandatory=True
        )
        message_rows = insert_statement_parser.parse_query()
        #message = message_data[0]['message']
        #topic_arn = message_data[0]['TopicArn']
        for  message_row in message_rows:
            message = message_row['message']
            topic_arn = message_row['TopicArn']
            self.handler.publish_message(topic_arn=topic_arn, message=message)
        # todo check insert in mysql how works currently return 0
        


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
        index_list = []
        topics_arn_list = []
        i = 1
        for topic in topics:
            index_list.append(i)
            topics_arn_list.append(topic["TopicArn"])
            i += 1
        data = {'index': index_list, 'topic_arn': topics_arn_list}
        df = pd.DataFrame(data=data)
        return df

    def insert(self, query: ast.Insert) -> pd.DataFrame:
        insert_statement_parser = INSERTQueryParser(
            query,
            mandatory_columns=['Name'],
            all_mandatory=True
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
