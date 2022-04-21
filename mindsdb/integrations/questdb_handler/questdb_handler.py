from mindsdb.integrations.postgres_handler.postgres_handler import PostgresHandler

class QuestDBHandler(PostgresHandler):
    """
    This handler handles connection and execution of the QuestDB statements. 
    TODO: check the dialect for questdb
    """

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)

    def get_tables(self):
        """
        List all tabels in QuestDB
        """
        query = "SHOW TABLES"
        res = super().native_query(query)
        return res

    def describe_table(self, table_name):
        """
        List information about the table
        """
        query = f"SELECT * FROM tables() WHERE name='{table_name}';"
        result = super().native_query(query)
        print('RESULTTT ', result)
        return result
    
    def get_views(self):
        """
        QuestDB doesn't support views
        """
        raise NotImplementedError()
