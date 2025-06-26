from typing import Type, List, Any
import re
import json
from pydantic import BaseModel, Field
from langchain_core.tools import BaseTool
from mindsdb_sql_parser.ast import Describe, Select, Identifier, Constant, Star


def llm_str_strip(s):
    length = -1
    while length != len(s):
        length = len(s)

        # remove ```
        if s.startswith("```"):
            s = s[3:]
        if s.endswith("```"):
            s = s[:-3]

        # remove trailing new lines
        s = s.strip("\n")

        # remove extra quotes
        for q in ('"', "'", "`"):
            if s.count(q) == 1:
                s = s.strip(q)
    return s


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
        kb_names = self.db.get_usable_knowledge_base_names()
        # Convert list to a formatted string for better readability
        if not kb_names:
            return "No knowledge bases found."
        return json.dumps(kb_names)


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
        # First, check if the input is already a list (passed directly from include_knowledge_bases)
        if isinstance(tool_input, list):
            return tool_input

        # Next, try to parse it as JSON in case it was serialized as a JSON string
        try:
            parsed_input = json.loads(tool_input)
            if isinstance(parsed_input, list):
                return parsed_input
        except (json.JSONDecodeError, TypeError):
            pass

        # Finally, try the original regex pattern for $START$ and $STOP$ markers
        match = re.search(r"\$START\$(.*?)\$STOP\$", tool_input, re.DOTALL)
        if not match:
            # If no markers found, check if it's a simple comma-separated string
            if "," in tool_input:
                return [kb.strip() for kb in tool_input.split(",")]
            # If it's just a single string without formatting, return it as a single item
            if tool_input.strip():
                return [llm_str_strip(tool_input)]
            return []

        # Extract and clean the knowledge base names
        kb_names_str = match.group(1).strip()
        kb_names = re.findall(r"`([^`]+)`", kb_names_str)

        kb_names = [llm_str_strip(n) for n in kb_names]
        return kb_names

    def _run(self, tool_input: str) -> str:
        """Get information about specified knowledge bases."""
        kb_names = self._extract_kb_names(tool_input)

        if not kb_names:
            return "No valid knowledge base names provided. Please provide knowledge base names as a list, comma-separated string, or enclosed in backticks between $START$ and $STOP$."

        results = []

        for kb_name in kb_names:
            try:
                self.db.check_knowledge_base_permission(Identifier(kb_name))

                # Get knowledge base schema
                schema_result = self.db.run_no_throw(str(Describe(kb_name, type="knowledge_base")))

                if not schema_result:
                    results.append(f"Knowledge base `{kb_name}` not found or has no schema information.")
                    continue

                # Format the results
                kb_info = f"## Knowledge Base: `{kb_name}`\n\n"

                # Schema information
                kb_info += "### Schema Information:\n"
                kb_info += "```\n"

                # Handle different return types for schema_result
                if isinstance(schema_result, str):
                    kb_info += f"{schema_result}\n"
                elif isinstance(schema_result, list):
                    for row in schema_result:
                        if isinstance(row, dict):
                            kb_info += f"{json.dumps(row, indent=2)}\n"
                        else:
                            kb_info += f"{str(row)}\n"
                else:
                    kb_info += f"{str(schema_result)}\n"

                kb_info += "```\n\n"

                # Get sample data
                sample_data = self.db.run_no_throw(
                    str(Select(targets=[Star()], from_table=Identifier(kb_name), limit=Constant(20)))
                )

                # Sample data
                kb_info += "### Sample Data:\n"

                # Handle different return types for sample_data
                if not sample_data:
                    kb_info += "No sample data available.\n"
                elif isinstance(sample_data, str):
                    kb_info += f"```\n{sample_data}\n```\n"
                elif isinstance(sample_data, list) and len(sample_data) > 0:
                    # Only try to extract columns if we have a list of dictionaries
                    if isinstance(sample_data[0], dict):
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
                        # If it's a list but not of dictionaries, just format as text
                        kb_info += "```\n"
                        for item in sample_data:
                            kb_info += f"{str(item)}\n"
                        kb_info += "```\n"
                else:
                    # For any other type, just convert to string
                    kb_info += f"```\n{str(sample_data)}\n```\n"

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
            query = llm_str_strip(query)
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

            # Ensure we always return a string
            if isinstance(result, (list, dict)):
                return json.dumps(result, indent=2)
            return str(result)
        except Exception as e:
            return f"Error executing query: {str(e)}"
