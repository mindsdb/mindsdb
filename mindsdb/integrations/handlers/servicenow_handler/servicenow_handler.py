from mindsdb.integrations.base import IntegrationHandler # type: ignore
from .servicenow_utils import ServiceNowClient

class ServiceNowHandler(IntegrationHandler):
    def __init__(self, config):
        super().__init__(config)
        self.client = ServiceNowClient(config)

    def validate_connection(self):
        """
        Test the connection to ServiceNow.
        """
        return self.client.ping()

    def fetch_data(self, query):
        """
        Fetch data from ServiceNow based on the provided query.
        """
        return self.client.query_records(query)

    def push_data(self, table, data):
        """
        Push data into a specific table in ServiceNow.
        """
        return self.client.insert_record(table, data)
