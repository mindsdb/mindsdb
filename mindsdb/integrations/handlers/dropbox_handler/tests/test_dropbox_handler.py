import unittest
from unittest.mock import patch, MagicMock

from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from mindsdb.integrations.handlers.dropbox_handler.dropbox_handler import DropboxHandler


class DropboxHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "access_token": "ai.L-wqp3eP6r4cSWVklkKAdTNZ3VAuQjWuZMvIs1BzKvZNVW07rKbVNi5HbxvLc9q9D6qSfsf5VTsqYsNPGUkqSJBlpkr88gNboUNuhITmJG9mVw-Olniu4MO3BWVbEIphVxXxxxCd677Y",
        }
        cls.handler = DropboxHandler("test_dropbox_handler", cls.kwargs)

    @patch("dropbox.Dropbox")
    def test_0_check_connection(self, mock_dropbox_class):
        mock_dropbox_instance = MagicMock()
        mock_dropbox_class.return_value = mock_dropbox_instance

        mock_dropbox_instance.users_get_current_account.return_value = MagicMock()

        status = self.handler.check_connection()
        self.assertTrue(status.success)

    @patch("dropbox.Dropbox")
    def test_1_get_tables(self, mock_dropbox_class):
        mock_dropbox_instance = MagicMock()
        mock_dropbox_class.return_value = mock_dropbox_instance

        mock_files_list_folder_result = MagicMock()
        mock_files_list_folder_result.entries = [
            MagicMock(name="file1.csv", path_lower="/file1.csv"),
            MagicMock(name="file2.csv", path_lower="/file2.csv"),
        ]
        mock_files_list_folder_result.has_more = False
        mock_dropbox_instance.files_list_folder.return_value = (
            mock_files_list_folder_result
        )

        tables = self.handler.get_tables()
        self.assertIsNotNone(tables)
        self.assertNotEqual(tables.type, RESPONSE_TYPE.ERROR)
