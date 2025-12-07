"""Pydantic AI tool builders - create tools directly from agent configuration (no skills)"""

from typing import Dict, Any, List, Optional
from mindsdb.utilities import log
from mindsdb.interfaces.agents.utils.sql_agent import MindsDBSQLProxy
from mindsdb.api.executor.command_executor import ExecuteCommands
from mindsdb.utilities.cache import get_cache

logger = log.getLogger(__name__)

_MAX_CACHE_SIZE = 100


def create_text2sql_tool(
    agent_params: Dict[str, Any],
    command_executor: Any,
    llm: Any,
) -> Optional[Any]:
    """
    Create a text2sql tool from agent configuration.
    
    Args:
        agent_params: Agent parameters containing data.tables configuration
        command_executor: Command executor for SQL queries
        llm: LLM instance (not used directly, but needed for MindsDBSQLProxy compatibility)
        
    Returns:
        Async function that can be used as a Pydantic AI tool, or None if no tables configured
    """
    data_config = agent_params.get("data", {})
    tables_list = data_config.get("tables")
    knowledge_bases_list = data_config.get("knowledge_bases")
    
    if not tables_list and not knowledge_bases_list:
        return None
    
    # Extract database information from tables
    databases = []
    databases_struct = {}
    extracted_databases = set()
    
    if tables_list:
        for table in tables_list:
            if isinstance(table, str):
                # Table format: "database.table" or just "table"
                parts = table.split(".", 1)
                if len(parts) == 2:
                    db_name, table_name = parts
                    extracted_databases.add(db_name)
                else:
                    # Default database
                    extracted_databases.add("mindsdb")
    
    all_databases = list(extracted_databases) if extracted_databases else ["mindsdb"]
    knowledge_base_database = "mindsdb"  # Default project for KBs
    
    # Create MindsDBSQLProxy
    sql_agent = MindsDBSQLProxy(
        command_executor=command_executor,
        databases=all_databases,
        databases_struct=databases_struct,
        include_tables=tables_list,
        ignore_tables=None,
        include_knowledge_bases=knowledge_bases_list,
        ignore_knowledge_bases=None,
        knowledge_base_database=knowledge_base_database,
        sample_rows_in_table_info=3,
        cache=get_cache("agent", max_size=_MAX_CACHE_SIZE),
    )
    
    # Get table info for tool description
    table_info = ""
    if tables_list:
        try:
            table_info = sql_agent.get_table_info(tables_list)
        except Exception as e:
            logger.warning(f"Error getting table info: {e}")
            table_info = f"Available tables: {', '.join(tables_list)}"
    
    kb_info = ""
    if knowledge_bases_list:
        try:
            kb_info = sql_agent.get_knowledge_base_info(knowledge_bases_list)
        except Exception as e:
            logger.warning(f"Error getting KB info: {e}")
            kb_info = f"Available knowledge bases: {', '.join(knowledge_bases_list)}"
    
    # Create tool description
    description_parts = [
        "Execute MindsDB SQL queries against database tables and knowledge bases.",
        "Use this tool to answer questions about data in the database or query knowledge bases.",
        "",
        "**Knowledge Base Queries:**",
        "Knowledge bases can be queried using semantic search and metadata filtering:",
        "- Semantic search: SELECT * FROM kb_name WHERE content = 'your query'",
        "- Metadata filtering: SELECT * FROM kb_name WHERE metadata_column = 'value'",
        "- Combined: SELECT * FROM kb_name WHERE content = 'query' AND metadata_column = 'value' AND relevance >= 0.5",
        "- Knowledge bases support standard SQL operations including JOINs",
        "",
        "**Important:**",
        "- Always execute the SQL query using this tool.",
        "- The SQL query string itself is NOT the final answer unless the user specifically asks for the query.",
        "- All SQL commands must be valid MindsDB SQL syntax.",
    ]
    if table_info:
        description_parts.append(f"\n**Available Tables:**\n{table_info}")
    if kb_info:
        description_parts.append(f"\n**Available Knowledge Bases:**\n{kb_info}")
    
    tool_description = "\n".join(description_parts)
    
    # Helper function to clean query input (extracted from mindsdb_database_agent)
    def extract_essential(input: str) -> str:
        """Sometimes LLM include to input unnecessary data. We can't control stochastic nature of LLM, so we need to
        'clean' input somehow. LLM prompt contains instruction to enclose input between '$START$' and '$STOP$'.
        """
        if "$START$" in input:
            input = input.partition("$START$")[-1]
        if "$STOP$" in input:
            input = input.partition("$STOP$")[0]
        return input.strip(" ")
    
    # Create async tool function
    async def sql_db_query(query: str) -> str:
        """
        Execute a SQL query against the database.
        
        Args:
            query: SQL query string to execute
            
        Returns:
            Query results as a formatted string
        """
        try:
            # Clean query input
            query = extract_essential(query)
            
            # Execute query directly using MindsDBSQLProxy
            logger.info(f"Executing SQL query via tool: {query}")
            result = sql_agent.query(query)
            return result
        except Exception as e:
            logger.exception("Error executing SQL command:")
            # If this is a knowledge base query, provide a more helpful error message
            if "knowledge_base" in query.lower() or any(
                kb in query for kb in sql_agent.get_usable_knowledge_base_names()
            ):
                return f"Error executing knowledge base query: {str(e)}. Please check that the knowledge base exists and your query syntax is correct."
            return f"Error: {str(e)}"
    
    # Set tool metadata
    sql_db_query.__name__ = "sql_db_query"
    sql_db_query.__doc__ = tool_description
    
    return sql_db_query


def build_tools_from_agent_config(
    agent_params: Dict[str, Any],
    command_executor: Any,
    llm: Any,
    embedding_model: Any,
    kb_controller: Any,
    project_id: int,
) -> List[Any]:
    """
    Build all tools from agent configuration.
    
    This function creates a single SQL tool that handles all queries including:
    - Database table queries
    - Knowledge base queries (semantic search and metadata filtering)
    
    Args:
        agent_params: Agent parameters containing data configuration
        command_executor: Command executor for SQL queries
        llm: LLM instance (not used directly, kept for compatibility)
        embedding_model: Embedding model instance (not used, kept for compatibility)
        kb_controller: Knowledge base controller (not used directly, kept for compatibility)
        project_id: Project ID (not used directly, kept for compatibility)
        
    Returns:
        List of tool functions ready to be registered with Pydantic AI agent
        (currently returns a single SQL tool that handles everything)
    """
    tools = []
    
    data_config = agent_params.get("data", {})
    tables_list = data_config.get("tables")
    knowledge_bases_list = data_config.get("knowledge_bases")
    
    # Create a single SQL tool that handles both tables and knowledge bases
    # Knowledge bases are queried using MindsDB SQL syntax:
    # - SELECT * FROM kb_name WHERE content = 'query' (semantic search)
    # - SELECT * FROM kb_name WHERE metadata_column = 'value' (metadata filtering)
    if tables_list or knowledge_bases_list:
        sql_tool = create_text2sql_tool(agent_params, command_executor, llm)
        if sql_tool:
            tools.append(sql_tool)
    
    return tools

