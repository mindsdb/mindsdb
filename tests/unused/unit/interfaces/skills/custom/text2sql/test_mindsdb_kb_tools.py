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
        """Set up test fixtures."""
        self.mock_db = MagicMock()

        # Sample knowledge base data
        self.kb_list_data: List[Dict[str, str]] = [
            {"name": "kb1"},
            {"name": "kb2"},
            {"name": "kb3"}
        ]

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
        # Configure mock
        self.mock_db.run.return_value = self.kb_list_data

        # Create tool instance
        kb_list_tool = KnowledgeBaseListTool(db=self.mock_db)

        # Test tool execution
        result = kb_list_tool._run("")

        # Verify results
        self.assertEqual(result, "`kb1`, `kb2`, `kb3`")
        self.mock_db.run.assert_called_once_with("SHOW KNOWLEDGE_BASES;")

    def test_knowledge_base_list_tool_empty_result(self) -> None:
        """Test the KnowledgeBaseListTool with empty result."""
        # Configure mock
        self.mock_db.run.return_value = []

        # Create tool instance
        kb_list_tool = KnowledgeBaseListTool(db=self.mock_db)

        # Test tool execution
        result = kb_list_tool._run("")

        # Verify results
        self.assertEqual(result, "No knowledge bases found.")

    def test_knowledge_base_info_tool(self) -> None:
        """Test the KnowledgeBaseInfoTool."""
        # Configure mock
        self.mock_db.run.side_effect = [self.kb_schema_data, self.kb_sample_data]

        # Create tool instance
        kb_info_tool = KnowledgeBaseInfoTool(db=self.mock_db)

        # Test tool execution
        result = kb_info_tool._run("$START$ `kb1` $STOP$")

        # Verify results
        self.assertIn("## Knowledge Base: `kb1`", result)
        self.assertIn("### Schema Information:", result)
        self.assertIn("### Sample Data:", result)
        self.assertEqual(self.mock_db.run.call_count, 2)

    def test_knowledge_base_info_tool_invalid_input(self) -> None:
        """Test the KnowledgeBaseInfoTool with invalid input."""
        # Create tool instance
        kb_info_tool = KnowledgeBaseInfoTool(db=self.mock_db)

        # Test tool execution with invalid input
        result = kb_info_tool._run("invalid input")

        # Verify results
        self.assertEqual(
            result,
            "No valid knowledge base names provided. Please provide names enclosed in backticks between $START$ and $STOP$."
        )

    def test_knowledge_base_query_tool(self) -> None:
        """Test the KnowledgeBaseQueryTool."""
        # Configure mock
        self.mock_db.run.return_value = self.kb_sample_data

        # Create tool instance
        kb_query_tool = KnowledgeBaseQueryTool(db=self.mock_db)

        # Test tool execution
        query = "SELECT * FROM kb1 WHERE content = 'color';"
        result = kb_query_tool._run(f"$START$ {query} $STOP$")

        # Verify results
        self.assertIn("| id | chunk_id | chunk_content | metadata | distance | relevance |", result)
        self.mock_db.run.assert_called_once_with(query)

    def test_knowledge_base_query_tool_invalid_input(self) -> None:
        """Test the KnowledgeBaseQueryTool with invalid input."""
        # Create tool instance
        kb_query_tool = KnowledgeBaseQueryTool(db=self.mock_db)

        # Test tool execution with invalid input
        result = kb_query_tool._run("invalid input")

        # Verify results
        self.assertEqual(
            result,
            "No valid SQL query provided. Please provide a query between $START$ and $STOP$."
        )

    def test_knowledge_base_query_tool_empty_result(self) -> None:
        """Test the KnowledgeBaseQueryTool with empty result."""
        # Configure mock
        self.mock_db.run.return_value = []

        # Create tool instance
        kb_query_tool = KnowledgeBaseQueryTool(db=self.mock_db)

        # Test tool execution
        query = "SELECT * FROM kb1 WHERE content = 'nonexistent';"
        result = kb_query_tool._run(f"$START$ {query} $STOP$")

        # Verify results
        self.assertEqual(result, "Query executed successfully, but no results were returned.")


if __name__ == "__main__":
    unittest.main()
