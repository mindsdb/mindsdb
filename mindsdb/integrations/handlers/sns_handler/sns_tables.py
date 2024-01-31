from typing import List
import json as JSON
import pandas as pd
from mindsdb_sql.parser import ast
from mindsdb.integrations.handlers.utilities.query_utilities.insert_query_utilities import INSERTQueryParser
from mindsdb.integrations.libs.api_handler import APITable, APIHandler
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions


class SubscriptionTable(APITable):
    """The SNS subscriptions Table implementation for create and read subscription"""
    name: str = "subscriptions"
    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses

        Returns
        -------
        List[str]
            List of columns
        """
        return [
            "SubscriptionArn"
        ]
    
    
    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.handler.connect()
    
    def insert(self, query: ast.Insert) -> pd.DataFrame:
        mandatory_columns = ('TopicArn','Endpoint','Protocol')
        supported_columns = tuple()
        supported_columns += mandatory_columns
        insert_statement_parser = INSERTQueryParser(
            query,
            mandatory_columns=list(mandatory_columns),
            supported_columns=list(supported_columns),
            all_mandatory=True
        )
        subscription_rows= insert_statement_parser.parse_query()
        for subscription_row in subscription_rows:
            topic_arn=subscription_row['TopicArn']
            protocol=subscription_row['Protocol']
            endpoint=subscription_row['Endpoint']
            response=self.handler.connection.subscribe(TopicArn=topic_arn,Protocol=protocol,Endpoint=endpoint)
            return pd.DataFrame(response)
        
    def select(self, query: ast.Select) -> pd.DataFrame:
        conditions = extract_comparison_conditions(query.where)
        params = {}
        accepted_params = ['TopicArn', 'SubscriptionArn','Protocol','Endpoint']
        for op, arg1, arg2 in conditions:
            if arg1 in accepted_params:
                if op != '=':
                    raise NotImplementedError
                params[arg1] = arg2
        response = self.handler.connection.list_subscriptions()        
        if bool(params) is False:
            print(response['Subscriptions'])           
            return pd.DataFrame(response['Subscriptions'])    
        else:
            for subscription in response['Subscriptions']:
                for key,value in params.items():
                    if value==subscription[key]:
                        print(subscription) 
                        return pd.DataFrame.from_dict([subscription])
            return pd.DataFrame(columns=accepted_params)
                
                
            

class MessageTable(APITable):
    """The SNS message Table implementation for publish (insert) messages"""
    name: str = "messages"

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
    
    def select(self, query: ast.Select) -> pd.DataFrame:
        params = {}
        conditions=extract_comparison_conditions(query.where)
        accepted_params = ['QueueUrl']
        for op, arg1, arg2 in conditions:
            if arg1 in accepted_params:
                if op != '=':
                    raise NotImplementedError
                params[arg1] = arg2
        if bool(params) is False:
            return pd.DataFrame(columns = ['text'])
        response=""
        #try:
        #response = self.handler.sqs_client.receive_message(QueueUrl='http://localstack:4566/000000000000/test')
        print(params)
        response = self.handler.sqs_client.receive_message(QueueUrl=params['QueueUrl'])
        #except:
        #raise ConnectionError
        if response !="" and 'Messages' in response:    
            text_list = list()
            for message in response['Messages']:
                body=message['Body']
                json_obj=JSON.loads(body)
                text = json_obj['Message']
                text_list.append(text)
            df = pd.DataFrame({'text': text_list})
            return df
        else:
            df = pd.DataFrame(columns = ['text'])
            return df
            

    def insert(self, query: ast.Insert) -> pd.DataFrame:
        """  Args: query (ast.Insert): SQL query to parse.
        """
        mandatory_columns = tuple()
        mandatory_columns=('message', 'topic_arn')
        supported_columns = ('id', 'subject', 'message_deduplication_id', 'message_group_id',
                         'message_attributes')
        supported_columns+=mandatory_columns
        columns: List[str] = list(supported_columns)
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
        if 'id' not in message_rows[0]:
            for message_row in message_rows:
                message = message_row['message']
                topic_arn = message_row['topic_arn']
                self.handler.connection.publish(TopicArn=message_row['topic_arn'], Message=message_row['message'])
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
                if message_id in message_ids_set:
                    raise ValueError("Two or more batch entries in the request have the same Id")
                message_ids_set.add(message_id)
                request_entry = {'Id': message_id, 'Message': message, 'Subject': subject,
                                 'MessageDeduplicationId': message_deduplication_id, 'MessageGroupId': message_group_id,
                                 'MessageAttributes': message_attributes}
                request_entries.append(request_entry)
            self.handler.connection.publish_batch(TopicArn=topic_arn,
                                                 PublishBatchRequestEntries=request_entries)


class TopicTable(APITable):
    """
    class for view and insert sns topics
    """
    name: str = "topics"
    columns: List[str] = ["topic_arn", "name"]

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
        conditions = extract_comparison_conditions(query.where)
        params = {}
        accepted_params = ['TopicArn']
        for op, arg1, arg2 in conditions:
            if arg1 in accepted_params:
                if op != '=':
                    raise NotImplementedError
                params[arg1] = arg2
            else:
                raise NotImplementedError  
        if bool(params) is False:
            return self.topic_list()
        else:              
            return self.topic_list(topic_arn=params["TopicArn"])

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
            self.handler.connection.create_topic(Name =topic_name[0])
    
    def topic_list(self, topic_arn=None) -> pd.DataFrame:
        """
        Returns topic arns as a pandas DataFrame.
        Args:
            topic_arn (str): topic TopicArn (str)
        """
        response = self.handler.connection.list_topics()
        if len(response['Topics']) == 0:
            return pd.DataFrame({'TopicArn': []})
        json_response = str(response)
        if topic_arn is not None:
            for topic_arn_row in response['Topics']:
                topic_arn_response_name = topic_arn_row['TopicArn']
                if topic_arn == topic_arn_response_name:
                    return pd.DataFrame([{'TopicArn': topic_arn_response_name}])
        if topic_arn is not None:
            return pd.DataFrame({'TopicArn': []})
        json_response = json_response.replace("\'", "\"")
        data = JSON.loads(str(json_response))
        return pd.DataFrame(data["Topics"])
    
    def get_columns(self, ignore: List[str] = []) -> List[str]:
        """columns

        Args:
            ignore (List[str], optional): exclusion items. Defaults to [].

        Returns:
            List[str]: available columns with `ignore` items removed from the list.
        """

        return [item for item in self.columns if item not in ignore]
