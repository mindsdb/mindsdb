import unittest

from mindsdb_sql_parser import parse_sql

from mindsdb.integrations.handlers.notion_handler.notion_handler import NotionHandler
from mindsdb.integrations.handlers.notion_handler.notion_table import (
    NotionDatabaseTable,
    NotionBlocksTable,
    NotionCommentsTable,
    NotionPagesTable,
)


class NotionHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_data": {
                "api_token": "secret_KHTlOzUN5fIwVlb1euOpBa4lwcJA7jEALoBGDRrlATx"
            }
        }
        cls.handler = NotionHandler("test_notion_handler", **cls.kwargs)
        cls.db_table = NotionDatabaseTable(cls.handler)
        cls.pages_table = NotionPagesTable(cls.handler)
        cls.blocks_table = NotionBlocksTable(cls.handler)
        cls.comments_table = NotionCommentsTable(cls.handler)

    def test_check_connection(self):
        status = self.handler.check_connection()
        self.assertTrue(status.success)

    def test_select_database(self):
        query = "SELECT * FROM database WHERE database_id = '21510b8a953c4d62958c9907f3cf9f87'"
        ast = parse_sql(query)
        res = self.db_table.select(ast)
        self.assertFalse(res.empty)

    def test_select_page(self):
        query = "SELECT * FROM pages WHERE page_id = '70f28e55416b4dfe8588aa175ecae63a'"
        ast = parse_sql(query)
        res = self.pages_table.select(ast)
        self.assertFalse(res.empty)

    def test_select_blocks(self):
        query = (
            "SELECT * FROM blocks WHERE block_id = '6d1480e0bf4b46e1a71be093f105d654'"
        )
        ast = parse_sql(query)
        res = self.blocks_table.select(ast)
        self.assertFalse(res.empty)

    def test_select_comment(self):
        query = (
            "SELECT * FROM comments where block_id = '169fa742a8374fe9a516caecfb33432a'"
        )
        ast = parse_sql(query)
        res = self.comments_table.select(ast)
        self.assertFalse(res.empty)


if __name__ == "__main__":
    unittest.main()
