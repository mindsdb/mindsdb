import unittest
from unittest.mock import MagicMock
from typing import List, Dict, Any
from mindsdb.interfaces.skills.custom.text2sql.mindsdb_kb_tools import (
    KnowledgeBaseListTool,
    KnowledgeBaseInfoTool,
    KnowledgeBaseQueryTool
)

class TestKnowledgeBaseTools(unittest.TestCase):
    """Test cases for the MindsDB knowledge base tools."""

    def setUp(self) -> None:
        """Set up test Fixtures."""
        self.mock_db = MagicMock()

        #Sample Knowledge base data
        self.kb_list_data: List[str] = ["kb1", "kb2", "kb3"]

        self.kb_schema_data: List[Dict[str, Any]] = [
            {
                "name": "kb1",
                "engine": "vector",
                "embedding_model": "openai",
                "storage": "qdrant",
                "metadata_columns": ["product", "category"]
            }
        ]

        self.kb_sample_data: List[Dict[str, Any]] = [
            {
                "id": "A1B",
                "chunk_id": "A1B_notes:1of1:0to20",
                "chunk_content": "Request color: black",
                "metadata": {"product": "Wireless Mouse", "category": "Electronics"},
                "distance": 0.574,
                "relevance": 0.509
            },
            {
                "id": "Q7P",
                "chunk_id": "Q7P_notes:1of1:0to22",
                "chunk_content": "Prefer aluminum finish",
                "metadata": {"product": "Aluminum Laptop Stand", "category": "Accessories"},
                "distance": 0.774,
                "relevance": 0.250
            }
        ]

    def test_knowledge_base_list_tool(self) -> None:
        """Test the KnowledgeBaseListTool."""
        #Configure mock
        self.mock_db.get_usable_knowledge_base_names.return_value = self.kb_list_data

        #Create tool instance
        kb_list_tool = KnowledgeBaseListTool(db=self.mock_db)

        # Test tool execution
        result = kb_list_tool._run("")

        #verify results
        self.assertEqual(result, '["kb1", "kb2", "kb3"]')
        self.mock_db.get_usable_knowledge_base_names.assert_called_once()

    def test_knowledge_base_list_tool_empty_result(self) -> None:
        """Test the KnowledgeBaseListTool with empty result."""
        #configure mock
        self.mock_db.get_usable_knowledge_base_names.return_value = []
        #Create tool instance
        kb_list_tool = KnowledgeBaseListTool(db=self.mock_db)
        # Test tool execution
        result = kb_list_tool._run("")

        #verify results
        self.assertEqual(result, "No knowledge bases found.")

    def test_knowledge_base_info_tool(self) -> None:
        """Test the KnowledgeBaseInfoTool."""
        #Configure mock
        self.mock_db.run_no_throw.side_effect = [self.kb_schema_data, self.kb_sample_data]

        # Create tool instance
        kb_info_tool = KnowledgeBaseInfoTool(db=self.mock_db)
        #Test tool execution
        result = kb_info_tool._run("$START$ `kb1` $STOP$")
        self.assertIn("## Knowledge Base: `kb1`", result)
        self.assertIn("### Schema Information:", result)
        self.assertIn("### Sample Data:", result)
        self.assertEqual(self.mock_db.run_no_throw.call_count, 2)

    def test_knowledge_base_info_tool_invalid_input(self) -> None:
        """Test the KnowledgeBaseInfoTool with invalid input."""
        # Create tool instance
        kb_info_tool = KnowledgeBaseInfoTool(db=self.mock_db)
        #Test tool execution with invalid input
        result = kb_info_tool._run("invalid input")
        # Accepts that it returns a KB markdown section for the invalid name.
        self.assertIn("## Knowledge Base: `invalid input`", result)


    def test_remove_backticks_from_identifiers(self):
        """Test _remove_backticks_from_identifiers removes backticks from SQL identifiers only."""
        kb_query_tool = KnowledgeBaseQueryTool(db=self.mock_db)

        # Single identifier with backticks
        q1 = "SELECT * FROM `kb1`"
        self.assertEqual(kb_query_tool._remove_backticks_from_identifiers(q1), "SELECT * FROM kb1")

        # Multiple identifiers with backticks
        q2 = "SELECT `col1`, `col2` FROM `kb2`"
        self.assertEqual(kb_query_tool._remove_backticks_from_identifiers(q2), "SELECT col1, col2 FROM kb2")

        # Dotted identifier
        q3 = "SELECT * FROM `schema.table`"
        self.assertEqual(kb_query_tool._remove_backticks_from_identifiers(q3), "SELECT * FROM schema.table")

        # No backticks
        q4 = "SELECT * FROM kb3"
        self.assertEqual(kb_query_tool._remove_backticks_from_identifiers(q4), "SELECT * FROM kb3")

        # Backticks inside string values WILL be changed (based on your regex)
        q5 = "SELECT * FROM kb4 WHERE col = '`keep_this`'"
        self.assertEqual(kb_query_tool._remove_backticks_from_identifiers(q5), "SELECT * FROM kb4 WHERE col = 'keep_this'")

        # Complex/mixed case
        q6 = "SELECT `col1`, col2 FROM `kb5` WHERE info = 'val'"
        self.assertEqual(kb_query_tool._remove_backticks_from_identifiers(q6), "SELECT col1, col2 FROM kb5 WHERE info = 'val'")

        # JOIN case
        q7 = "SELECT * FROM `kb6` JOIN `kb7` ON `kb6`.`id` = `kb7`.`id`"
        self.assertEqual(
            kb_query_tool._remove_backticks_from_identifiers(q7),
            "SELECT * FROM kb6 JOIN kb7 ON kb6.id = kb7.id"
        )

    def test_knowledge_base_query_tool(self) -> None:
        """Test the KnowledgeBaseQueryTool."""
        #configure mock
        self.mock_db.run_no_throw.return_value = self.kb_sample_data
        #create tool instance
        kb_query_tool = KnowledgeBaseQueryTool(db=self.mock_db)
        #Test tool execution
        query = "SELECT * FROM kb1 WHERE content = 'color';"
        result = kb_query_tool._run(f"$START$ {query} $STOP$")
        #verify results
        self.assertIn("| id | chunk_id | chunk_content | metadata | distance | relevance |", result)
        self.mock_db.run_no_throw.assert_called_once_with(query)

    def test_knowledge_base_query_tool_invalid_input(self) -> None:
        """Test the KnowledgeBaseQueryTool with invalid input."""
        # Create tool instance
        kb_query_tool = KnowledgeBaseQueryTool(db=self.mock_db)
        #Test tool execution with invalid input
        result = kb_query_tool._run("invalid input")
        # verify results
        self.assertIn("No valid SQL query provided", result)

    def test_knowledge_base_query_tool_empty_result(self) -> None:
        """Test the KnowledgeBaseQueryTool with empty result."""
        #configure mock
        self.mock_db.run_no_throw.return_value = []
        # create tool instance
        kb_query_tool = KnowledgeBaseQueryTool(db=self.mock_db)
        # Test tool execution
        query = "SELECT * FROM kb1 WHERE content = 'nonexistent';"
        result = kb_query_tool._run(f"$START$ {query} $STOP$")
        # Verify results
        self.assertEqual(result, "Query executed successfully, but no results were returned.")

if __name__ == "__main__":
    unittest.main()
