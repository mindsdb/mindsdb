from pysnc import ServiceNowClient as SNC

class ServiceNowClient:
    def __init__(self, config):
        self.client = SNC(instance=config['instance'], user=config['user'], password=config['password'])

    def ping(self):
        """
        Check if the ServiceNow connection is active.
        """
        try:
            return self.client.table('incident').get(limit=1) is not None
        except Exception as e:
            raise ConnectionError(f"Failed to connect to ServiceNow: {e}")

    def query_records(self, query):
        """
        Query records from ServiceNow.
        """
        try:
            return self.client.table(query['table']).get(query['conditions'])
        except Exception as e:
            raise ValueError(f"Failed to fetch data from ServiceNow: {e}")

    def insert_record(self, table, data):
        """
        Insert a new record into a ServiceNow table.
        """
        try:
            return self.client.table(table).insert(data)
        except Exception as e:
            raise ValueError(f"Failed to insert data into ServiceNow: {e}")
