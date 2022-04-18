import unittest

from mindsdb.utilities.config import Config
from mindsdb.integrations.postgres_handler.postgres_handler import PostgresHandler


class PostgresHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = Config()
        cls.kwargs = {
            "host": "localhost",
            "port": "5432",
            "user": "mindsdb",
            "password": "mindsdb",
            "database": "postgres"
        }
        cls.handler = PostgresHandler('test_postgres_handler', **cls.kwargs)


    def test_0_check_status(self):
        assert self.handler.check_status()
    
    def test_1_describe_table(self):
        described = self.handler.describe_table("test_mdb")
        assert isinstance(described, list)
    
    def test_2_get_tables(self):
        tables = self.handler.get_tables()
        assert isinstance(tables, list)
    
    def test_3_get_views(self):
        views = self.handler.get_views()
        assert isinstance(views, list)

    def test_4_select_query(self):
        query = "SELECT * FROM data.test_mdb WHERE 'id'='1'"
        parsed = self.handler.parser(query, dialect=self.handler.dialect)
        targets = parsed.targets
        from_stmt = parsed.from_table
        where_stmt = parsed.where
        result = self.handler.select_query(targets, from_stmt, where_stmt)
        assert len(result) > 0

