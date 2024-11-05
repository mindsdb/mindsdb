import unittest
from mindsdb.integrations.handlers.dropbox_handler.dropbox_handler import DropboxHandler


class DropboxHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "access_token": "ai.L-wqp3eP6r4cSWVklkKAdTNZ3VAuQjWuZMvIs1BzKvZNVW07rKbVNi5HbxvLc9q9D6qSfsf5VTsqYsNPGUkqSJBlpkr88gNboUNuhITmJG9mVw-Olniu4MO3BWVbEIphVxXxxxCd677Y",
        }
        cls.handler = DropboxHandler("test_dropbox_handler", cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()


if __name__ == "__main__":
    unittest.main()
