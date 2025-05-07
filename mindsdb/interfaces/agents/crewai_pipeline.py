import os
from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass

from crewai import Agent, Task, Crew, Process
from crewai.tools import tool
from langchain_openai import ChatOpenAI

from mindsdb.utilities import log
from mindsdb.utilities.config import Config
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
        model: str = "gpt-4o",
        temperature: float = 0.0,
        api_key: Optional[str] = None,
        verbose: bool = True,
        max_tokens: int = 4000
    ):
        """Initialize the CrewAI Text-to-SQL Pipeline.
        
        Args:
            tables: List of table names in format 'database.table'
            knowledge_bases: List of knowledge base names
            model: OpenAI model to use
            temperature: Model temperature
            api_key: OpenAI API key (defaults to OPENAI_API_KEY env var)
            verbose: Whether to output detailed logs
            max_tokens: Maximum tokens for completion
        """
        self.tables = tables or []
        self.knowledge_bases = knowledge_bases or []
        self.model = model
        self.temperature = temperature
        self.verbose = verbose
        self.max_tokens = max_tokens
        
        if api_key:
            self.api_key = api_key
            # Set environment variable for CrewAI components that rely on it directly
            os.environ["OPENAI_API_KEY"] = api_key
        else:
            self.api_key = os.environ.get("OPENAI_API_KEY")
            
        if not self.api_key:
            raise ValueError("OpenAI API key must be provided or set as OPENAI_API_KEY environment variable")
        
        # Initialize the LLM
        self.llm = ChatOpenAI(
            model=self.model,
            temperature=self.temperature,
            api_key=self.api_key,
            max_tokens=self.max_tokens
        )
        
        # Initialize the skill tool controller from MindsDB
        self.skill_tool = SkillToolController()
        
        # Set up the tools and agents
        self._setup_tools()
        self._setup_agents()
    
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
            
            command_executor = self.skill_tool.get_command_executor()
            # Parse the SQL string to an AST object first
            from mindsdb_sql_parser import parse_sql
            ast_query = parse_sql(sql_query)
            
            # Now execute the parsed query
            result = command_executor.execute_command(ast_query, database_name="mindsdb")
            
            # Convert ExecuteAnswer to a DataFrame for easier manipulation
            df = None
            if hasattr(result, 'data') and hasattr(result.data, 'data_frame'):
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
                    data_dict = {
                        "columns": df.columns.tolist(),
                        "rows": df.to_dict(orient="records")
                    }
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
                return (f"Error: The knowledge base may not exist or the query syntax is incorrect. "
                        f"For knowledge base queries, use simple syntax: SELECT * FROM knowledge_base_name "
                        f"WHERE content = 'search term'; - Original error: {error_msg}")
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
            direct_sql_commands = [
                "SHOW DATABASES", 
                "SHOW TABLES", 
                "SHOW AGENTS", 
                "SHOW SKILLS", 
                "SHOW KNOWLEDGE_BASES"
            ]
            
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
        
        # Tool for getting schema information
        @tool("get_schema_tool")
        def get_schema(table_name: str) -> str:
            """Gets schema information for a specified table.
            
            Args:
                table_name: Name of the table to get schema for
                
            Returns:
                Schema information including columns and data types
            """
            try:
                command_executor = self.skill_tool.get_command_executor()
                
                # Parse the table name to extract database and table components
                parts = table_name.split('.')
                if len(parts) == 1:
                    # Only table name provided, use a DESCRIBE command
                    sql_command = f"DESCRIBE {table_name};"
                elif len(parts) == 2:
                    # Database and table provided
                    sql_command = f"DESCRIBE {parts[0]}.{parts[1]};"
                else:
                    return f"Invalid table name format: {table_name}. Please use 'database.table' or just 'table'."
                
                # Execute the DESCRIBE command to get schema information
                schema_result = self._execute_sql_safely(sql_command)
                return f"Schema for {table_name}:\n{schema_result}"
            except Exception as e:
                return f"Error getting schema for {table_name}: {str(e)}"
        
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
        
        self.tools = {
            "list_tables": list_tables,
            "get_schema": get_schema,
            "execute_sql": execute_sql,
            "check_sql": check_sql
        }
    
    def _setup_agents(self):
        """Set up the four agents in the pipeline."""
        
        # 1. Query Understanding Agent
        self.query_understanding_agent = Agent(
            role="Query Understanding Agent",
            goal="Analyze natural language questions to identify intent and relevant data sources",
            backstory="""You are an expert at understanding user questions and translating them 
            into structured intent. Your job is to analyze questions, identify what data is being 
            requested, and determine which tables or knowledge bases are relevant.

            Note:
            - If the user input is a general greeting or conversational phrase (e.g., "Hi", "Hello", "How are you?"), respond 
            directly to the user to answer the general question based on your general knowledge.
            - Do NOT proceed to SQL generation or execution for such inputs.
            """,
            verbose=self.verbose,
            allow_delegation=True,
            tools=[self.tools["list_tables"]],
            llm=self.llm
        )
        
        # 2. SQL Generation Agent
        self.sql_generation_agent = Agent(
            role="SQL Query Generation Agent",
            goal="Create precise, efficient SQL queries from the parsed user's intent",
            backstory="""You are a SQL expert who can translate user intent into proper SQL queries.
            You understand database schemas and can write complex queries including joins, aggregations,
            and filtering. You adapt your approach based on whether the query needs regular SQL or 
            semantic search functionality.""",
            verbose=self.verbose,
            allow_delegation=True,
            tools=[self.tools["list_tables"], self.tools["check_sql"]],
            llm=self.llm
        )
        
        # 3. SQL Execution Agent
        self.sql_execution_agent = Agent(
            role="SQL Execution Agent",
            goal="Execute SQL queries accurately and return complete results",
            backstory="""You are responsible for running SQL queries and
            returning the complete results exactly as received from the database. 
            You understand that queries may contain long string literals that must be 
            preserved perfectly for accurate results.""",
            verbose=self.verbose,
            allow_delegation=True,
            tools=[self.tools["execute_sql"]],
            llm=self.llm
        )
        
        # 4. SQL Validation Agent
        self.sql_validation_agent = Agent(
            role="Results Presentation Specialist",
            goal="Present query results clearly and accurately to the user",
            backstory="""You are responsible for formatting and presenting query results 
            to users in a clear, readable format. For knowledge base similarity searches, 
            you preserve the actual content returned from the database and never substitute 
            with placeholder data. You always return the exact data retrieved from the database.
            Note:
            - If the user input is a general greeting or question or conversational phrase (e.g., "Hi", "Hello", "How are you?"), 
            respond directly to the user to answer the general question based on your general knowledge.
            """,
            verbose=self.verbose,
            allow_delegation=True,
            llm=self.llm
        )
    
    def process_query(self, user_query: str) -> Dict[str, Any]:
        """Process a natural language query through the CrewAI pipeline.
        
        Args:
            user_query: The natural language query from the user
            
        Returns:
            Dict containing the original query, SQL generated, and results
        """
        # Task 1: Understand the query
        understand_task = Task(
            description=f"""
            Interpret user query: "{user_query}"
            
            Determine:
            1. What information the user is looking for
            2. Which tables or knowledge bases are relevant
            3. Whether this requires regular SQL or semantic search
            4. Any filters, aggregations, or specific fields needed
            
            - IMPORTANT: NEVER modify, truncate, or abbreviate any string literals, product names, or search terms 
            from the original query. Always preserve the EXACT and COMPLETE text of any quoted strings, 
            especially in filter conditions.
            
            - If user input pertains to structured data (e.g., databases, tables), then convert the natural 
            language question into an SQL query, based on the provided information in the user question.

            -If the user input relates to unstructured data or requires information from documents, articles, 
            or general knowledge, then it has to do semantic similarity search in the knowledge base. For this: 
            convert the natural language question into an SQL query, but in this case it has to generate a sql 
            query on the knowledge base that has a WHERE condition on the "content" column and the condition term should be 
            extracted from the user input.
            
            - If the user is asking about available databases, tables, or schema information,
            use the list_tables tool to get this information directly.
            
            IMPORTANT:
            - If the user input is a general greeting or conversational phrase (e.g., "Hi", "Hello", "How are you?"), respond 
            directly to the user to answer the general question based on your general knowledge.
            - Do NOT proceed to SQL generation or execution for such inputs.
            """,
            agent=self.query_understanding_agent,
            expected_output="Parsed query details, intent and required data sources or direct response for general inputs"
        )
        
        # Task 2: Generate SQL
        generate_sql_task = Task(
            description="""
            Based on the query understanding, generate the appropriate SQL query from the user input.
            You need to generate a SQL query that result of its execution can directly answer user's request, later 
            SQL Validation Agent can use this result to answer the users question.
            
            CRITICAL: NEVER truncate, abbreviate, or modify string literals in any way.
            Always keep quoted values EXACTLY as they appear in the original query, including
            full names, long text strings, and search terms. Even very long strings
            must be preserved completely.
            
            For standard database queries:
            - Include all necessary joins, filters, and aggregations
            - Ensure proper syntax and column references
            - Preserve the EXACT text of string literals in WHERE clauses
            
            For knowledge base queries for semantic similarity search, generate a query on knowledge base and 
            put WHERE condition on "content" column.  
            "content" column is the only column that is used for semantic search. 
            - This is an example query for knowledge base search:
              SELECT *
              FROM knowledge_base_name
              WHERE content = 'search_term' AND relevance_threshold=0.6 LIMIT 50;
            
            Note: In this example 'search_term' is the term that is extracted from the user input, and 
            "knowledge_base_name" is the name of the knowledge base, and "relevance_threshold" is the 
            relevance threshold for the semantic search and it should be between 0 and 1. and always should you "=" operator for it not 
            "<=" or ">=" or ">" or "<". You can choose default value for relevance_threshold of 0.6 if not provided in the user query.
            Also ensure you use single quotes (') not double quotes (") for string literals in SQL.
            
            Validate your SQL with the check_sql tool before finalizing.

            Note: 
            - If the previous output indicates a general conversation or greeting, acknowledge that no SQL query is needed.
            - Otherwise, generate the appropriate SQL query as per the user's intent.
            """,
            agent=self.sql_generation_agent,
            expected_output="A valid SQL query that addresses the user's question with exact string values preserved or acknowledgment of non-requirement",
            context=[understand_task]
        )
        
        # Task 3: Execute SQL
        execute_sql_task = Task(
            description="""
            Execute the EXACT SQL query using the execute_sql tool WITHOUT modifying it.
            
            IMPORTANT: 
            - Never modify, truncate, or change the SQL query in any way
            - Execute the exact query as provided, even if it contains very long string literals
            - Preserve the complete structure and values of the original query
            - If no SQL query is provided (due to the nature of the user input), acknowledge that execution is skipped.
            
            The output MUST be a JSON/dictionary object containing the raw query results.
            Example output format:
            {
                "columns": ["col1", "col2"],
                "rows": [
                    {"col1": "value1", "col2": "value2"},
                    {"col1": "value3", "col2": "value4"}
                ]
            }
            
            Capture and report any execution errors that occur, returning a dictionary like:
            {"error": "<error message>"}
            """,
            agent=self.sql_execution_agent,
            expected_output="Dictionary object with either query results or an error message, or acknowledgment of skipped execution",
            context=[generate_sql_task]
        )
        
        # Task 4: Validate Results
        validate_results_task = Task(
            description="""
            - Review the dictionary from the previous step and return a final dictionary with the following structure:
            {
                "text": "<Short explanation about the process or explaining the executed query and resulted data or extract information from the result to answer the user's question or any error message>",
                "data": <The dictionary received from the SQL Execution Agent>
            }
            
            - NEVER manipulate or transform the contents of the 'data' key other than passing it verbatim.
            - If there was an error, replicate the error information in the 'text' field and keep 'data' empty or as provided.

            - If the original query was a general greeting or conversational input, respond directly to 
            the user to answer the general question based on your general knowledge. This is the user's original 
            question or query: "{user_query}"
            - If SQL execution was performed, format and present the results accordingly.

            - If user asked a question that still needs to extract information or interpretation from the result, 
            then extract and pass it into the 'text' field and still keep 'data' field as it is.
            
            - Based on your evaluation if the result is not enough to answer the user's question, 
            then you can pass your insight as string to Query Understanding Agent to start over a more complimentary solution.
            """,
            agent=self.sql_validation_agent,
            expected_output="Dictionary with 'text' and 'data' keys as described above",
            context=[execute_sql_task, understand_task]
        )
        
        # Create and run the crew
        crew = Crew(
            agents=[
                self.query_understanding_agent,
                self.sql_generation_agent,
                self.sql_execution_agent,
                self.sql_validation_agent
            ],
            tasks=[
                understand_task,
                generate_sql_task,
                execute_sql_task,
                validate_results_task
            ],
            verbose=self.verbose,
            #process=Process.sequential,
            planning=True
        )
        
        result = crew.kickoff()
        
        # Ensure the final output follows the desired dictionary structure
        final_result: Dict[str, Any]
        try:
            # If the crew already returned a dict, use it directly
            if isinstance(result, dict):
                final_result = result
            else:
                # Attempt to parse JSON string to dict
                import json
                final_result = json.loads(result)
                if not isinstance(final_result, dict):
                    raise ValueError
        except Exception:
            # Fallback to wrapping raw output
            final_result = {
                "text": str(result),
                "data": {}
            }
        
        # Optionally include the original user query for traceability
        final_output = {
            "user_query": user_query,
            "result": final_result
        }
        
        return final_output


class CrewAIAgentManager:
    """Manager class for CrewAI agents in MindsDB."""
    
    @staticmethod
    def create_crewai_agents(
        name: str,
        tables: List[str] = None,
        knowledge_bases: List[str] = None,
        provider: str = 'openai',
        model: str = 'gpt-4o',
        prompt_template: str = None,
        verbose: bool = True,
        max_tokens: int = 4000,
        api_key: str = None
    ) -> CrewAITextToSQLPipeline:
        """Create a CrewAI pipeline with the specified configuration.
        
        This mimics the SQL-like CREATE CREWAI_AGENTs syntax in Python.
        
        Args:
            name: Name for the CrewAI agent group
            tables: List of tables to query (format: 'database.table')
            knowledge_bases: List of knowledge bases to query
            provider: LLM provider (only 'openai' supported currently)
            model: Model name to use
            prompt_template: Custom prompt template (not used in current implementation)
            verbose: Whether to output detailed logs
            max_tokens: Maximum tokens for completion
            api_key: API key for the provider
            
        Returns:
            Configured CrewAITextToSQLPipeline instance
        """
        if provider.lower() != 'openai':
            raise ValueError("Only 'openai' provider is currently supported")
        
        return CrewAITextToSQLPipeline(
            tables=tables,
            knowledge_bases=knowledge_bases,
            model=model,
            temperature=0.2,  # Default temperature
            api_key=api_key,
            verbose=verbose,
            max_tokens=max_tokens
        ) 