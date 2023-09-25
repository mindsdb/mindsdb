import pandas as pd
from typing import Text, List, Dict, Any

from mindsdb_sql.parser import ast
from mindsdb.integrations.libs.api_handler import APITable

from mindsdb.integrations.handlers.utilities.query_utilities.insert_query_utilities import INSERTQueryParser

from mindsdb.utilities.log import get_log

logger = get_log("integrations.ms_teams_handler")


class MessagesTable(APITable):
    """The Microsoft Teams Messages Table implementation"""

    def insert(self, query: ast.Insert) -> None:
        """Sends messages to a Microsoft Teams Channel through a configured webhook.

        Parameters
        ----------
        query : ast.Insert
           Given SQL INSERT query.

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
            supported_columns=['text'],
            mandatory_columns=['text'],
            all_mandatory=False
        )
        message_data = insert_statement_parser.parse_query()
        self.send_message(message_data)

    def send_message(self, message_data: List[Dict[Text, Any]]) -> None:
        """Sends messages to a Microsoft Teams Channel using the parsed message data.

        Parameters
        ----------
        message_data : List[Dict[Text, Any]]
           List of dictionaries containing the messages to send.

        Returns
        -------
        None
        """
        teams = self.handler.connect() 
        for message in message_data:
            try:
                teams.text(message['text'])
                teams.send()
                logger.info(f"Message sent to Microsoft Teams channel successfully.")
            except Exception as e:
                logger.error(f"Error sending message to Microsoft Teams channel: {e}")
                raise e