import unittest
import os
import pandas as pd
from mindsdb.integrations.handlers.raindrop_handler.raindrop_handler import RaindropHandler


class TestRaindropHandlerIntegration(unittest.TestCase):
    """Integration tests for RaindropHandler (requires valid API key)"""

    @classmethod
    def setUpClass(cls):
        """Set up the test environment"""
        cls.api_key = os.environ.get('RAINDROP_API_KEY')
        if not cls.api_key:
            raise unittest.SkipTest("RAINDROP_API_KEY environment variable not set")
        
        cls.handler = RaindropHandler('test_raindrop_handler')
        cls.handler.connection_data = {'api_key': cls.api_key}

    def test_check_connection(self):
        """Test that we can connect to the Raindrop.io API"""
        response = self.handler.check_connection()
        self.assertTrue(response.success, f"Connection failed: {response.error_message}")

    def test_get_tables(self):
        """Test that tables are properly registered"""
        tables = self.handler.get_tables()
        table_names = [table.data[0] for table in tables.data]
        
        self.assertIn('raindrops', table_names)
        self.assertIn('bookmarks', table_names)
        self.assertIn('collections', table_names)

    def test_raindrops_table_select(self):
        """Test selecting from raindrops table"""
        # Test basic select
        query = "SELECT * FROM raindrops LIMIT 5"
        result = self.handler.native_query(query)
        self.assertTrue(result.success, f"Query failed: {result.error_message}")
        
        # Check that we get a DataFrame
        if hasattr(result, 'data_frame') and result.data_frame is not None:
            self.assertIsInstance(result.data_frame, pd.DataFrame)

    def test_collections_table_select(self):
        """Test selecting from collections table"""
        query = "SELECT * FROM collections LIMIT 5"
        result = self.handler.native_query(query)
        self.assertTrue(result.success, f"Query failed: {result.error_message}")
        
        # Check that we get a DataFrame
        if hasattr(result, 'data_frame') and result.data_frame is not None:
            self.assertIsInstance(result.data_frame, pd.DataFrame)

    def test_raindrops_table_columns(self):
        """Test that raindrops table has expected columns"""
        raindrops_table = self.handler.get_table('raindrops')
        columns = raindrops_table.get_columns()
        
        expected_columns = [
            "_id", "link", "title", "excerpt", "note", "type", "cover", "tags",
            "important", "reminder", "removed", "created", "lastUpdate",
            "domain", "collection.id", "collection.title", "user.id", "broken",
            "cache", "file.name", "file.size", "file.type"
        ]
        
        for col in expected_columns:
            self.assertIn(col, columns, f"Column {col} not found in raindrops table")

    def test_collections_table_columns(self):
        """Test that collections table has expected columns"""
        collections_table = self.handler.get_table('collections')
        columns = collections_table.get_columns()
        
        expected_columns = [
            "_id", "title", "description", "color", "view", "public", "sort",
            "count", "created", "lastUpdate", "expanded", "parent.id", "user.id",
            "cover", "access.level", "access.draggable"
        ]
        
        for col in expected_columns:
            self.assertIn(col, columns, f"Column {col} not found in collections table")

    def test_create_and_delete_bookmark(self):
        """Test creating and deleting a bookmark (if API key has write permissions)"""
        try:
            # Create a test bookmark
            insert_query = """
            INSERT INTO raindrops (link, title, note, tags) 
            VALUES ('https://example.com/test', 'Test Bookmark', 'Test note', 'test,automated')
            """
            result = self.handler.native_query(insert_query)
            
            if not result.success:
                # Skip if we don't have write permissions
                self.skipTest(f"Cannot create bookmarks: {result.error_message}")
            
            # Try to find the bookmark we just created
            select_query = "SELECT * FROM raindrops WHERE title = 'Test Bookmark' LIMIT 1"
            result = self.handler.native_query(select_query)
            self.assertTrue(result.success)
            
            if hasattr(result, 'data_frame') and result.data_frame is not None and not result.data_frame.empty:
                bookmark_id = result.data_frame['_id'].iloc[0]
                
                # Delete the test bookmark
                delete_query = f"DELETE FROM raindrops WHERE _id = {bookmark_id}"
                result = self.handler.native_query(delete_query)
                self.assertTrue(result.success)
                
        except Exception as e:
            self.fail(f"Create/delete test failed: {e}")

    def test_create_and_delete_collection(self):
        """Test creating and deleting a collection (if API key has write permissions)"""
        try:
            # Create a test collection
            insert_query = """
            INSERT INTO collections (title, description, color) 
            VALUES ('Test Collection', 'Automated test collection', '#FF0000')
            """
            result = self.handler.native_query(insert_query)
            
            if not result.success:
                # Skip if we don't have write permissions
                self.skipTest(f"Cannot create collections: {result.error_message}")
            
            # Try to find the collection we just created
            select_query = "SELECT * FROM collections WHERE title = 'Test Collection' LIMIT 1"
            result = self.handler.native_query(select_query)
            self.assertTrue(result.success)
            
            if hasattr(result, 'data_frame') and result.data_frame is not None and not result.data_frame.empty:
                collection_id = result.data_frame['_id'].iloc[0]
                
                # Delete the test collection
                delete_query = f"DELETE FROM collections WHERE _id = {collection_id}"
                result = self.handler.native_query(delete_query)
                self.assertTrue(result.success)
                
        except Exception as e:
            self.fail(f"Create/delete collection test failed: {e}")


if __name__ == '__main__':
    # Run integration tests only if API key is available
    unittest.main()
