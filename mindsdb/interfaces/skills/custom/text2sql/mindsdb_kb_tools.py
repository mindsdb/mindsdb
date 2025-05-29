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
            # First try using get_usable_knowledge_base_names method
            try:
                kb_names = self.db.get_usable_knowledge_base_names()

                # Handle the case where the result is not iterable (ExecuteAnswer object)
                if not hasattr(kb_names, "__iter__") or isinstance(kb_names, str):
                    # Try to extract data from ExecuteAnswer object if possible
                    if hasattr(kb_names, "data"):
                        kb_names = kb_names.data
                    else:
                        # If we can't extract data, try direct SQL query instead
                        raise ValueError("Non-iterable result from get_usable_knowledge_base_names")
            except Exception as e:
                # If the first method fails, try direct SQL query
                try:
                    # Try to query knowledge bases directly from the database
                    result = self.db.run_no_throw("SHOW KNOWLEDGE_BASES FROM mindsdb;")
                    if result and hasattr(result, "__iter__"):
                        if isinstance(result[0], dict) and "name" in result[0]:
                            kb_names = [kb["name"] for kb in result]
                        else:
                            kb_names = result
                    else:
                        # If that fails too, return a helpful message
                        return "No knowledge bases found or unable to retrieve knowledge base list."
                except Exception as inner_e:
                    # Log both errors for debugging
                    return f"Error listing knowledge bases: {str(e)}. Direct query error: {str(inner_e)}"

            # Format the result as a markdown table
            if kb_names and len(kb_names) > 0:
                if isinstance(kb_names[0], dict) and "name" in kb_names[0]:
                    # If we have a list of dictionaries with 'name' key
                    kb_names = [kb["name"] for kb in kb_names]

                # Create a markdown table
                table = "| Knowledge Base Name |\n"
                table += "| ----------------- |\n"
                for name in kb_names:
                    table += f"| `{name}` |\n"

                return table
            else:
                return "No knowledge bases found."
        except Exception as e:
            return f"Error listing knowledge bases: {str(e)}"


class KnowledgeBaseInfoToolInput(BaseModel):
    tool_input: str = Field(
        ...,
        description="A comma-separated list of knowledge base names enclosed between $START$ and $STOP$.",
    )


class KnowledgeBaseInfoTool(BaseTool):
    """Tool for getting information about knowledge bases in MindsDB."""

    name: str = "kb_info_tool"
    description: str = "Get information about knowledge bases in MindsDB."
    args_schema: Type[BaseModel] = KnowledgeBaseInfoToolInput
    db: Any = None

    def _extract_kb_names(self, tool_input: str) -> List[str]:
        """Extract knowledge base names from the tool input."""
        match = re.search(r"\$START\$(.*?)\$STOP\$", tool_input, re.DOTALL)
        if not match:
            return []

        # Extract and clean the knowledge base names
        kb_names_str = match.group(1).strip()
        kb_names = re.findall(r"`([^`]+)`", kb_names_str)
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
                schema_result = self.db.run_no_throw(f"DESCRIBE KNOWLEDGE_BASE `{kb_name}`;")

                if not schema_result:
                    results.append(f"Knowledge base `{kb_name}` not found or has no schema information.")
                    continue

                # Get sample data
                sample_data = self.db.run_no_throw(f"SELECT * FROM `{kb_name}` LIMIT 10;")

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
        description="A SQL query for knowledge bases. Can be provided directly or enclosed between $START$ and $STOP$.",
    )


class KnowledgeBaseQueryTool(BaseTool):
    """Tool for querying knowledge bases in MindsDB."""

    name: str = "kb_query_tool"
    description: str = "Query knowledge bases in MindsDB."
    args_schema: Type[BaseModel] = KnowledgeBaseQueryToolInput
    db: Any = None

    def _extract_query(self, tool_input: str) -> str:
        """Extract the SQL query from the tool input."""
        # First check if the input is wrapped in $START$ and $STOP$
        match = re.search(r"\$START\$(.*?)\$STOP\$", tool_input, re.DOTALL)
        if match:
            return match.group(1).strip()

        # If not wrapped in delimiters, use the input directly
        # Check for SQL keywords to validate it's likely a query
        if re.search(r"\b(SELECT|FROM|WHERE|LIMIT|ORDER BY)\b", tool_input, re.IGNORECASE):
            return tool_input.strip()

        return ""

    def _run(self, tool_input: str) -> str:
        """Execute a knowledge base query."""
        query = self._extract_query(tool_input)

        if not query:
            return "No valid SQL query provided. Please provide a SQL query that includes SELECT, FROM, or other SQL keywords."

        try:
            # Execute the query
            result = self.db.run_no_throw(query)

            if not result:
                return "Query executed successfully, but no results were returned."

            # Format the results as a markdown table
            if isinstance(result, list) and len(result) > 0:
                # Extract column names
                columns = list(result[0].keys())

                # Create markdown table header
                table = "| " + " | ".join(columns) + " |\n"
                table += "| " + " | ".join(["---" for _ in columns]) + " |\n"

                # Add rows
                for row in result:
                    formatted_row = []
                    for col in columns:
                        cell_value = row[col]
                        if isinstance(cell_value, dict):
                            cell_value = json.dumps(cell_value, ensure_ascii=False)
                        formatted_row.append(str(cell_value).replace("|", "\\|"))
                    table += "| " + " | ".join(formatted_row) + " |\n"

                return table

            return result
        except Exception as e:
            return f"Error executing query: {str(e)}"
