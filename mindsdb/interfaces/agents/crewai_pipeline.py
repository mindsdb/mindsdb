import os
from typing import List, Dict, Any, Optional, Union
from crewai import Agent, Task, Crew, Process
from crewai.tools import tool
from crewai import LLM as CrewAILLM
from mindsdb.utilities import log
from mindsdb.interfaces.skills.skill_tool import SkillToolController

logger = log.getLogger(__name__)


class CrewAITextToSQLPipeline:
    """CrewAI implementation for text-to-SQL operations in MindsDB.

    This class creates a pipeline of agents that work together to:
    1. Understand the user's query
    2. Generate appropriate SQL
    3. Execute the SQL
    4. Validate the results

    It can handle both structured data queries and semantic search in knowledge bases.
    """

    def __init__(
        self,
        tables: Optional[List[str]] = None,
        knowledge_bases: Optional[List[str]] = None,
        provider: str = "openai",
        model: str = "gpt-4o",
        temperature: float = 0.0,
        api_key: Optional[str] = None,
        prompt_template: str = None,
        verbose: bool = True,
        max_tokens: int = 4000,
    ):
        """Initialize the CrewAI Text-to-SQL Pipeline.

        Args:
            tables: List of table names in format 'database.table'
            knowledge_bases: List of knowledge base names
            provider: LLM provider ('openai' or 'google')
            model: Model name to use (e.g., 'gpt-4o', 'gemini-2.0-flash')
            temperature: Model temperature
            api_key: API key for the provider (OpenAI API key or Google API key)
            verbose: Whether to output detailed logs
            max_tokens: Maximum tokens for completion
        """
        self.tables = tables or []
        self.knowledge_bases = knowledge_bases or []
        self.provider = provider.lower()
        self.model = model
        self.temperature = temperature
        self.verbose = verbose
        self.max_tokens = max_tokens

        # Validate provider
        if self.provider not in ["openai", "google"]:
            raise ValueError("Provider must be either 'openai' or 'google'")

        # Set up API key and environment variables based on provider
        if self.provider == "openai":
            if api_key:
                self.api_key = api_key
            else:
                self.api_key = os.environ.get("OPENAI_API_KEY")
            if not self.api_key:
                raise ValueError("OpenAI API key must be provided or set as OPENAI_API_KEY environment variable")
            llm_model = self.model

        elif self.provider == "google":
            # Ensure the API key is available and exported for downstream libraries
            if api_key:
                self.api_key = api_key
            else:
                self.api_key = os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")
            if not self.api_key:
                raise ValueError(
                    "Google API key must be provided or set as GOOGLE_API_KEY / GEMINI_API_KEY environment variable"
                )
            llm_model = self._prepare_gemini_model_name(self.model)

        # Initialize the CrewAI LLM which delegates calls to LiteLLM underneath
        self.llm = CrewAILLM(
            model=llm_model, api_key=self.api_key, temperature=self.temperature, max_tokens=self.max_tokens
        )

        # Initialize the skill tool controller from MindsDB
        self.skill_tool = SkillToolController()

        # Set up the tools and agents
        self._setup_tools()
        self._setup_agents()

        # Store the prompt template for later use in task descriptions
        self.prompt_template = prompt_template

    def _execute_sql_safely(self, sql_query: str, return_dict: bool = False) -> Union[str, Dict[str, Any]]:
        """Utility method to safely execute SQL by parsing it first.

        This centralizes SQL parsing and execution to avoid the 'to_string' error.

        Args:
            sql_query: The SQL query string to execute
            return_dict: If True, return the results as a structured dictionary, otherwise a formatted string

        Returns:
            String representation or dictionary representation of the execution result or an error message
        """
        try:
            # Log the query for debugging
            logger.info(f"Executing SQL query: {sql_query}")

            # Parse the SQL string to an AST object first
            from mindsdb_sql_parser import parse_sql

            ast_query = parse_sql(sql_query)

            # Now execute the parsed query
            result = self.skill_tool.get_command_executor().execute_command(ast_query, database_name="mindsdb")

            # Convert ExecuteAnswer to a DataFrame for easier manipulation
            df = None
            if hasattr(result, "data") and hasattr(result.data, "data_frame"):
                df = result.data.data_frame
            else:
                # Fallback to to_df when data_frame attr not available
                try:
                    df = result.data.to_df()
                except Exception:
                    df = None

            # If dictionary output requested
            if return_dict:
                if df is not None:
                    # Build a serialisable structure
                    data_dict = {"columns": df.columns.tolist(), "rows": df.to_dict(orient="records")}
                    return data_dict
                else:
                    # No dataframe – return whatever string representation is available
                    return {"message": str(result)}

            # Default behaviour (string)
            if df is not None:
                if not df.empty:
                    return df.to_string(index=False)
                else:
                    return "Query executed successfully, but returned no data."

            return str(result)
        except Exception as e:
            import traceback

            error_trace = traceback.format_exc()
            logger.error(f"Error executing SQL query: {sql_query}\nError: {str(e)}\nTrace: {error_trace}")

            error_msg = str(e)
            if return_dict:
                return {"error": error_msg}

            # Try to provide more helpful error messages for common issues
            if "Model not found" in error_msg and "knowledge_base" in sql_query.lower():
                return (
                    f"Error: The knowledge base may not exist or the query syntax is incorrect. "
                    f"For knowledge base queries, use simple syntax: SELECT * FROM knowledge_base_name "
                    f"WHERE content = 'search term'; - Original error: {error_msg}"
                )
            elif "table" in error_msg.lower() and "not found" in error_msg.lower():
                return f"Error: The table referenced in the query doesn't exist. Please check available tables using the list_tables tool. - Original error: {error_msg}"

            return f"Error executing SQL query: {error_msg}"

    def _setup_tools(self):
        """Set up the tools needed by the agents."""

        # Tool for listing available tables
        @tool("list_tables_tool")
        def list_tables(query: str) -> str:
            """Lists available tables and databases.

            Args:
                query: A question about what data is available

            Returns:
                List of available tables and databases
            """
            # Check if this is a direct SQL command that should be executed
            direct_sql_commands = ["SHOW DATABASES", "SHOW TABLES", "SHOW AGENTS", "SHOW KNOWLEDGE_BASES"]

            # Clean up the query
            cleaned_query = query.strip()

            # Check if the query is one of the direct SQL commands (case insensitive)
            for cmd in direct_sql_commands:
                if cleaned_query.upper().startswith(cmd):
                    # Execute the command directly
                    return self._execute_sql_safely(cleaned_query)

            # If not a direct command, provide the standard listing
            available_tables = self.tables
            available_kbs = self.knowledge_bases

            result = "Available tables:\n"
            for table in available_tables:
                result += f"- {table}\n"

            result += "\nAvailable knowledge bases:\n"
            for kb in available_kbs:
                result += f"- {kb}\n"

            return result

        # Tool for executing SQL queries
        @tool("execute_sql_tool")
        def execute_sql(sql_query: str) -> Dict[str, Any]:
            """Executes a SQL query and returns the results as a dictionary.

            Args:
                sql_query: The SQL query to execute

            Returns:
                Dictionary containing query results or error message
            """
            return self._execute_sql_safely(sql_query, return_dict=True)

        # Tool for checking SQL syntax
        @tool("check_sql_tool")
        def check_sql(sql_query: str) -> str:
            """Checks SQL syntax without executing the query.

            Args:
                sql_query: The SQL query to check

            Returns:
                Validation result or error message
            """
            try:
                # Just parse the SQL to check syntax, don't execute
                from mindsdb_sql_parser import parse_sql

                _ = parse_sql(sql_query)
                return "SQL syntax is valid."
            except Exception as e:
                return f"SQL syntax error: {str(e)}"

        self.tools = {"list_tables": list_tables, "execute_sql": execute_sql, "check_sql": check_sql}

    def _setup_agents(self):
        """Set up the four agents in the pipeline."""

        # Single Agent
        self.single_agent = Agent(
            role="Question Answering Agent",
            goal="Create precise, efficient SQL queries from user input based on the user's intent, and execute the queries to answer user's question",
            backstory="""You are a SQL expert who can translate user intent into proper SQL queries.
            You understand database schemas and can write complex queries including joins, aggregations,
            and filtering. You adapt your approach based on whether the query needs regular SQL or
            semantic search functionality. You are responsible for running SQL queries and
            returning the complete results exactly as received from the database.
            You understand that queries may contain long string literals that must be
            preserved perfectly for accurate results.""",
            verbose=self.verbose,
            allow_delegation=False,
            tools=[self.tools["list_tables"], self.tools["check_sql"], self.tools["execute_sql"]],
            llm=self.llm,
        )

    def _get_task_description(self, user_query: str) -> str:
        """Get the standard task description for CrewAI agents.

        Args:
            user_query: The user's natural language query

        Returns:
            The formatted task description string
        """
        return f"""
        Interpret user query: "{user_query}"
        Based on the query understanding, generate the appropriate SQL query from the user input.
        You need to generate a SQL query that result of its execution can directly answer user's request, later
        SQL Validation Agent can use this result to answer the users question.

        And here is more information about the database and tables (e.g. columns names and how they are related together) provided by the user: "{self.prompt_template}"

        CRITICAL: NEVER truncate, abbreviate, or modify string literals in any way.
        Always keep quoted values EXACTLY as they appear in the original query, including
        full names, long text strings, and search terms. Even very long strings
        must be preserved completely.

        - If user input pertains to structured data (e.g., databases, tables), then convert the natural
        language question into an SQL query, based on the provided information in the user question.

        For standard database queries:
            - Include all necessary joins, filters, and aggregations
            - Ensure proper syntax and column references
            - Preserve the EXACT text of string literals in WHERE clauses

        -If the user input relates to unstructured data or requires information from documents, articles,
        or general knowledge, then it has to do semantic similarity search in the knowledge base.
        For knowledge base queries for semantic similarity search, generate a query on knowledge base and
        put WHERE condition on "content" column.
        "content" column is the only column that is used for semantic search.
            - This is an example query for knowledge base search:
            SELECT *
            FROM knowledge_base_name
            WHERE content = 'search_term' AND relevance_threshold=0.6 LIMIT 50;

        Note: In this example 'search_term' is the term that is extracted from the user input, and
        "knowledge_base_name" is the name of the knowledge base, and "relevance_threshold" is the
        relevance threshold for the semantic search and it should be between 0 and 1, and always you should use "=" operator for it. You can choose default value for relevance_threshold of 0.6 if not provided in the user query.

        - If the user is asking about available databases, tables, use the list_tables tool to get this information directly.

        Validate your SQL with the check_sql tool before finalizing.

        Note:
        - If the previous output indicates a general conversation or greeting, acknowledge that no SQL query is needed, and answer it based on your general knowledge.
        - Otherwise, generate the appropriate SQL query as per the user's intent.

        Execute the EXACT SQL query using the execute_sql tool WITHOUT modifying it.

        IMPORTANT:
        - Never modify, truncate, or change the SQL query in any way
        - Execute the exact query as provided, even if it contains very long string literals
        - Preserve the complete structure and values of the original query
        - If no SQL query is provided (due to the nature of the user input), acknowledge that execution is skipped.

        - The output MUST be a string with markdown format.
        - If you need to return any table or dataframe in the answer it should be a string in the markdown format.

        - And then as the final answer to the user question, retrun the final dictionary like this:
        Short explanation about the process or explaining the executed query and resulted data or extract information from the result to answer the user's question or any error message.
        And the result received from the SQL Execution as a string in markdown format.

        - NEVER manipulate or transform the contents of the query execution result other than passing it verbatim.

        - If there was an error, replicate the error information in the answer.

        - If the original query was a general greeting or conversational input, respond directly to
        the user to answer the general question based on your general knowledge.

        IMPORTANT:
        - If there was an error from execution of the generated query, collect insight and regernarete the query to address the
          issue, and execute it again to get a correct meaningful answer.

        """

    def _create_crew(self, single_task: Task) -> Crew:
        """Create a Crew with the standard configuration.

        Args:
            single_task: The task to be executed by the crew

        Returns:
            Configured Crew instance
        """
        return Crew(
            agents=[self.single_agent],
            tasks=[single_task],
            verbose=self.verbose,
            process=Process.sequential,
        )

    def process_query_stream(self, user_query: str):
        """Process a natural language query through the CrewAI pipeline with streaming.

        Args:
            user_query: The natural language query from the user

        Yields:
            Dict containing streaming chunks with real thoughts, actions, and results from CrewAI
        """
        import sys

        # Yield initial start message
        yield {"type": "start", "prompt": user_query}

        # Create the task
        single_task = Task(
            description=self._get_task_description(user_query),
            agent=self.single_agent,
            expected_output="String with markdown format.",
        )

        # Create and run the crew with custom logging capture
        crew = self._create_crew(single_task)

        # Create a custom stdout/stderr capturer that parses in real-time
        class StreamingCapture:
            def __init__(self, stream_callback, original_stream):
                self.stream_callback = stream_callback
                self.original_stream = original_stream
                self.buffer = ""
                self.current_tool = ""
                self.current_tool_input = ""
                self.in_tool_input = False
                self.in_tool_output = False
                self.tool_input_lines = []
                self.tool_output_lines = []

            def write(self, text):
                # Write to original stream so logs still appear in terminal
                self.original_stream.write(text)
                self.original_stream.flush()

                # Parse the text for streaming
                self.buffer += text
                self.parse_buffer()

            def flush(self):
                self.original_stream.flush()

            def _strip_ansi_codes(self, text):
                """Remove ANSI escape codes from text"""
                import re

                # Pattern to match ANSI escape codes
                ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
                return ansi_escape.sub("", text)

            def parse_buffer(self):
                lines = self.buffer.split("\n")
                # Keep the last incomplete line in buffer
                self.buffer = lines[-1]

                # Process complete lines
                for line in lines[:-1]:
                    self.parse_line(line.strip())

            def parse_line(self, line):
                """Parse individual lines for CrewAI patterns"""
                if not line:
                    return

                # Strip ANSI escape codes from the line
                clean_line = self._strip_ansi_codes(line)
                if not clean_line.strip():
                    return

                # Check for thought patterns
                if "## Thought:" in clean_line:
                    # End any previous tool input/output collection
                    self._finalize_tool_collection()

                    thought_text = clean_line.split("## Thought:")[-1].strip()
                    if thought_text and thought_text.startswith("Thought:"):
                        thought_text = thought_text[8:].strip()  # Remove "Thought:" prefix
                    if thought_text and thought_text != "Thought:":
                        self.stream_callback(
                            {
                                "actions": [{"tool": "thinking", "tool_input": thought_text, "log": thought_text}],
                                "is_task_complete": False,
                            }
                        )

                # Check for tool usage patterns
                elif "## Using tool:" in clean_line:
                    # End any previous tool input/output collection
                    self._finalize_tool_collection()

                    tool_name = clean_line.split("## Using tool:")[-1].strip()
                    self.current_tool = tool_name

                # Check for tool input start
                elif "## Tool Input:" in clean_line:
                    self.in_tool_input = True
                    self.in_tool_output = False
                    self.tool_input_lines = []
                    # Check if there's content on the same line
                    tool_input_start = clean_line.split("## Tool Input:")[-1].strip()
                    if tool_input_start:
                        self.tool_input_lines.append(tool_input_start)

                # Check for tool output start
                elif "## Tool Output:" in clean_line:
                    # Finalize tool input if we were collecting it
                    if self.in_tool_input:
                        self.current_tool_input = "\n".join(self.tool_input_lines).strip()
                        self.in_tool_input = False

                    self.in_tool_output = True
                    self.tool_output_lines = []
                    # Check if there's content on the same line
                    tool_output_start = clean_line.split("## Tool Output:")[-1].strip()
                    if tool_output_start:
                        self.tool_output_lines.append(tool_output_start)

                # Check for final answer patterns
                elif "## Final Answer:" in clean_line:
                    # End any previous tool input/output collection
                    self._finalize_tool_collection()

                    final_answer = clean_line.split("## Final Answer:")[-1].strip()
                    if final_answer:
                        # Send the final answer as a regular chunk to get thought_process metadata
                        self.stream_callback({"output": final_answer, "type": "answer", "is_task_complete": False})

                # Check if we're in tool input or output collection mode
                elif self.in_tool_input:
                    # Continue collecting tool input lines (strip ANSI codes)
                    self.tool_input_lines.append(clean_line)

                elif self.in_tool_output:
                    # Continue collecting tool output lines (strip ANSI codes)
                    self.tool_output_lines.append(clean_line)

                    # Check if this might be the end of tool output (next section starts)
                    if clean_line.startswith("## ") or clean_line.startswith("# "):
                        self._finalize_tool_collection()
                        # Re-process this line since it's a new section
                        self.parse_line(line)

            def _finalize_tool_collection(self):
                """Finalize and stream any collected tool input/output"""
                if self.in_tool_output and self.tool_output_lines:
                    tool_output = "\n".join(self.tool_output_lines).strip()
                    if self.current_tool and self.current_tool_input:
                        # Stream the observation as a steps chunk (includes both action and observation)
                        self.stream_callback(
                            {
                                "steps": [
                                    {
                                        "action": {
                                            "tool": self.current_tool,
                                            "tool_input": self.current_tool_input,
                                            "log": f"Action: {self.current_tool}\nAction Input: {self.current_tool_input}",
                                        },
                                        "observation": tool_output,
                                    }
                                ],
                                "is_task_complete": False,
                            }
                        )

                    # Reset state
                    self.current_tool = ""
                    self.current_tool_input = ""
                    self.tool_output_lines = []
                    self.in_tool_output = False

                elif self.in_tool_input and self.tool_input_lines:
                    self.current_tool_input = "\n".join(self.tool_input_lines).strip()
                    self.tool_input_lines = []
                    self.in_tool_input = False

        # Create a callback to stream chunks
        streamed_chunks = []

        def stream_chunk(chunk):
            streamed_chunks.append(chunk)

        # Create custom capture streams
        stdout_capture = StreamingCapture(stream_chunk, sys.stdout)
        stderr_capture = StreamingCapture(stream_chunk, sys.stderr)

        try:
            # Replace stdout/stderr to capture CrewAI output in real-time
            original_stdout = sys.stdout
            original_stderr = sys.stderr
            sys.stdout = stdout_capture
            sys.stderr = stderr_capture

            # Run the crew
            result = crew.kickoff()

            # Convert result to string
            final_result = str(result)

            # Finalize any remaining tool collection in the capture streams
            stdout_capture._finalize_tool_collection()
            stderr_capture._finalize_tool_collection()

            # Yield all captured chunks
            for chunk in streamed_chunks:
                yield chunk

            # If no chunks were captured, or ensure we have a final result
            if not streamed_chunks or not any(
                chunk.get("type") == "answer" or "output" in chunk for chunk in streamed_chunks
            ):
                yield {"output": final_result, "type": "answer"}

        finally:
            # Restore original stdout/stderr
            sys.stdout = original_stdout
            sys.stderr = original_stderr

    def process_query(self, user_query: str) -> str:
        """Process a natural language query through the CrewAI pipeline.

        Args:
            user_query: The natural language query from the user

        Returns:
            Dict containing the original query, SQL generated, and results
        """

        single_task = Task(
            description=self._get_task_description(user_query),
            agent=self.single_agent,
            expected_output="String with markdown format.",
        )

        # Create and run the crew
        crew = self._create_crew(single_task)
        result = crew.kickoff()

        return str(result)

    def _prepare_gemini_model_name(self, raw_name: str) -> str:
        """Return a model name with the required `gemini/` prefix for LiteLLM.

        LiteLLM infers the provider from the model prefix. For Google AI Studio
        models the prefix must be `gemini/`. Examples:
            - "gemini-2.5-pro-preview-05-06"  -> "gemini/gemini-2.5-pro-preview-05-06"
            - "models/gemini-1.5-pro-latest" -> "gemini/gemini-1.5-pro-latest"
            - "gemini/gemini-1.5-pro"        -> unchanged
        """
        name = raw_name.strip()
        # Strip optional leading "models/"
        if name.startswith("models/"):
            name = name[len("models/") :]  # remove the prefix
        # Add gemini/ if missing
        if not name.startswith("gemini/"):
            name = f"gemini/{name}"
        return name


class CrewAIAgentManager:
    """Manager class for CrewAI agents in MindsDB."""

    @staticmethod
    def create_crewai_agents(
        name: str,
        tables: List[str] = None,
        knowledge_bases: List[str] = None,
        provider: str = "openai",
        model: str = "gpt-4o",
        prompt_template: str = None,
        verbose: bool = True,
        max_tokens: int = 4000,
        api_key: str = None,
        openai_api_key: str = None,
        google_api_key: str = None,
    ) -> CrewAITextToSQLPipeline:
        """Create a CrewAI pipeline with the specified configuration.

        This mimics the SQL-like CREATE CREWAI_AGENTs syntax in Python.

        Args:
            name: Name for the CrewAI agent group
            tables: List of tables to query (format: 'database.table')
            knowledge_bases: List of knowledge bases to query
            provider: LLM provider ('openai' or 'google')
            model: Model name to use
            prompt_template: Custom prompt template
            verbose: Whether to output detailed logs
            max_tokens: Maximum tokens for completion
            api_key: (deprecated) generic api key parameter – use provider-specific keys below
            openai_api_key: API key to use when provider='openai'
            google_api_key: API key to use when provider='google'

        Returns:
            Configured CrewAITextToSQLPipeline instance
        """
        provider_lc = provider.lower()

        # Select the correct api key parameter
        selected_key: Optional[str] = api_key  # fallback if user still uses api_key
        if provider_lc == "openai":
            if openai_api_key:
                selected_key = openai_api_key
        elif provider_lc == "google":
            if google_api_key:
                selected_key = google_api_key

        return CrewAITextToSQLPipeline(
            tables=tables,
            knowledge_bases=knowledge_bases,
            provider=provider,
            model=model,
            temperature=0.2,  # Default temperature
            api_key=selected_key,
            prompt_template=prompt_template,
            verbose=verbose,
            max_tokens=max_tokens,
        )
