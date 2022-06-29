from ..postgres_handler import Handler as PostgresHandler


class QuestDBHandler(PostgresHandler):
    """
    This handler handles connection and execution of the QuestDB statements. 
    TODO: check the dialect for questdb
    """
    name = 'questdb'

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)

    def get_tables(self):
        """
        List all tabels in QuestDB
        """
        query = "SHOW TABLES"
        response = super().native_query(query)
        return response

    def get_columns(self, table_name):
        """
        List information about the table
        """
        query = f"SELECT * FROM tables() WHERE name='{table_name}';"
        response = super().native_query(query)
        return response
