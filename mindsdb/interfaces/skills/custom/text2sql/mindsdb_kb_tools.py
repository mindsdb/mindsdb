from typing import Type, List, Any
import re
import json
from pydantic import BaseModel, Field
from langchain_core.tools import BaseTool


class KnowledgeBaseListToolInput(BaseModel):
    tool_input: str = Field("", description="An empty string to list all knowledge bases.")


class KnowledgeBaseListTool(BaseTool):
    """Tool for listing knowledge bases in MindsDB."""

    name: str = "kb_list_tool"
    description: str = "List all knowledge bases in MindsDB."
    args_schema: Type[BaseModel] = KnowledgeBaseListToolInput
    db: Any = None

    def _run(self, tool_input: str) -> str:
        """List all knowledge bases."""
        try:
            # Query to get all knowledge bases
            result = self.db.run("SHOW KNOWLEDGE_BASES;")

            if not result or len(result) == 0:
                return "No knowledge bases found."

            # Format the knowledge bases as a comma-separated list with backticks
            kb_names = [f"`{row['name']}`" for row in result]
            return ", ".join(kb_names)
        except Exception as e:
            return f"Error listing knowledge bases: {str(e)}"


class KnowledgeBaseInfoToolInput(BaseModel):
    tool_input: str = Field(
        ...,
        description="A comma-separated list of knowledge base names enclosed between $START$ and $STOP$."
    )


class KnowledgeBaseInfoTool(BaseTool):
    """Tool for getting information about knowledge bases in MindsDB."""

    name: str = "kb_info_tool"
    description: str = "Get information about knowledge bases in MindsDB."
    args_schema: Type[BaseModel] = KnowledgeBaseInfoToolInput
    db: Any = None

    def _extract_kb_names(self, tool_input: str) -> List[str]:
        """Extract knowledge base names from the tool input."""
        match = re.search(r'\$START\$(.*?)\$STOP\$', tool_input, re.DOTALL)
        if not match:
            return []

        # Extract and clean the knowledge base names
        kb_names_str = match.group(1).strip()
        kb_names = re.findall(r'`([^`]+)`', kb_names_str)
        return kb_names

    def _run(self, tool_input: str) -> str:
        """Get information about specified knowledge bases."""
        kb_names = self._extract_kb_names(tool_input)

        if not kb_names:
            return "No valid knowledge base names provided. Please provide names enclosed in backticks between $START$ and $STOP$."

        results = []

        for kb_name in kb_names:
            try:
                # Get knowledge base schema
                schema_result = self.db.run(f"DESCRIBE KNOWLEDGE_BASE `{kb_name}`;")

                if not schema_result:
                    results.append(f"Knowledge base `{kb_name}` not found or has no schema information.")
                    continue

                # Get sample data
                sample_data = self.db.run(f"SELECT * FROM `{kb_name}` LIMIT 3;")

                # Format the results
                kb_info = f"## Knowledge Base: `{kb_name}`\n\n"

                # Schema information
                kb_info += "### Schema Information:\n"
                kb_info += "```\n"
                for row in schema_result:
                    kb_info += f"{json.dumps(row, indent=2)}\n"
                kb_info += "```\n\n"

                # Sample data
                kb_info += "### Sample Data:\n"
                if sample_data:
                    # Extract column names
                    columns = list(sample_data[0].keys())

                    # Create markdown table header
                    kb_info += "| " + " | ".join(columns) + " |\n"
                    kb_info += "| " + " | ".join(["---" for _ in columns]) + " |\n"

                    # Add rows
                    for row in sample_data:
                        formatted_row = []
                        for col in columns:
                            cell_value = row[col]
                            if isinstance(cell_value, dict):
                                cell_value = json.dumps(cell_value, ensure_ascii=False)
                            formatted_row.append(str(cell_value).replace("|", "\\|"))
                        kb_info += "| " + " | ".join(formatted_row) + " |\n"
                else:
                    kb_info += "No sample data available.\n"

                results.append(kb_info)

            except Exception as e:
                results.append(f"Error getting information for knowledge base `{kb_name}`: {str(e)}")

        return "\n\n".join(results)


class KnowledgeBaseQueryToolInput(BaseModel):
    tool_input: str = Field(
        ...,
        description="A SQL query for knowledge bases enclosed between $START$ and $STOP$."
    )


class KnowledgeBaseQueryTool(BaseTool):
    """Tool for querying knowledge bases in MindsDB."""

    name: str = "kb_query_tool"
    description: str = "Query knowledge bases in MindsDB."
    args_schema: Type[BaseModel] = KnowledgeBaseQueryToolInput
    db: Any = None

    def _extract_query(self, tool_input: str) -> str:
        """Extract the SQL query from the tool input."""
        match = re.search(r'\$START\$(.*?)\$STOP\$', tool_input, re.DOTALL)
        if not match:
            return ""

        query = match.group(1).strip()
        return query

    def _run(self, tool_input: str) -> str:
        """Execute a knowledge base query."""
        query = self._extract_query(tool_input)

        if not query:
            return "No valid SQL query provided. Please provide a query between $START$ and $STOP$."

        try:
            # Execute the query
            result = self.db.run(query)

            if not result:
                return "Query executed successfully, but no results were returned."

            # Format the results as a markdown table
            columns = list(result[0].keys())

            # Create markdown table header
            output = "| " + " | ".join(columns) + " |\n"
            output += "| " + " | ".join(["---" for _ in columns]) + " |\n"

            # Add rows
            for row in result:
                formatted_row = []
                for col in columns:
                    cell_value = row[col]
                    if isinstance(cell_value, dict):
                        cell_value = json.dumps(cell_value, ensure_ascii=False)
                    formatted_row.append(str(cell_value).replace("|", "\\|"))
                output += "| " + " | ".join(formatted_row) + " |\n"

            return output
        except Exception as e:
            return f"Error executing query: {str(e)}"
