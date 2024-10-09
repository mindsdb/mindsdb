from typing import Dict, List, Text

from mindsdb_sql.parser.ast import Select
import pandas as pd

from mindsdb.integrations.libs.api_handler import APIResource
from mindsdb.utilities import log


logger = log.getLogger(__name__)


class ContactsTable(APIResource):
    """
    This is the table abstraction for the Contacts resource of the Salesforce API.
    """

    def select(self, query: Select) -> pd.DataFrame:
        """
        Executes a SELECT SQL query represented by an ASTNode object on the Salesforce Contacts resource and retrieves the data (if any).

        Args:
            query (ASTNode): An ASTNode object representing the SQL query to be executed.

        Returns:
            pd.DataFrame: A DataFrame containing the data retrieved from the Salesforce Contacts resource.
        """
        query.from_table = "Contact"

        client = self.handler.connect()
        results = client.sobjects.query(query.to_string())

        for result in results:
            del result['attributes']

        return pd.DataFrame(results)

    def add(self, contact: Dict) -> None:
        """
        Adds a new contact to the Salesforce Contacts resource.

        Args:
            contact (Dict): The data to be inserted into the Salesforce Contacts resource.
        """
        pass

    def get_columns(self) -> List[Text]:
        """
        Retrieves the attributes (columns) of the Salesforce Contacts resource.

        Returns:
            List[Text]: A list of Attributes (columns) of the Salesforce Contacts resource.
        """
        client = self.handler.connect()
        return [field['name'] for field in client.sobjects.Contact.describe()['fields']]