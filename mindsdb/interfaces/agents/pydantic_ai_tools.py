"""Pydantic AI tool builders - create tools directly from agent configuration (no skills)"""

from typing import Dict, Any, List, Optional
from mindsdb.utilities import log
from mindsdb.interfaces.skills.sql_agent import SQLAgent
from mindsdb.interfaces.agents.mindsdb_database_agent import MindsDBSQL
from mindsdb.interfaces.knowledge_base.controller import KnowledgeBaseController
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
        llm: LLM instance (not used directly, but needed for SQLAgent compatibility)
        
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
    
    # Create SQLAgent
    sql_agent = SQLAgent(
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
    
    # Create MindsDBSQL wrapper
    db_wrapper = MindsDBSQL.custom_init(sql_agent=sql_agent)
    
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
        "Execute SQL queries against the configured database tables.",
        "Use this tool to answer questions about data in the database.",
    ]
    if table_info:
        description_parts.append(f"\nTable information:\n{table_info}")
    if kb_info:
        description_parts.append(f"\nKnowledge base information:\n{kb_info}")
    description_parts.append(
        "\nImportant: Always execute the SQL query using this tool. "
        "The SQL query string itself is NOT the final answer unless the user specifically asks for the query."
    )
    
    tool_description = "\n".join(description_parts)
    
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
            # Clean and execute query
            result = db_wrapper.run_no_throw(query, fetch="all")
            return result
        except Exception as e:
            logger.error(f"Error executing SQL query: {e}", exc_info=True)
            return f"Error executing SQL query: {str(e)}"
    
    # Set tool metadata
    sql_db_query.__name__ = "sql_db_query"
    sql_db_query.__doc__ = tool_description
    
    return sql_db_query


def create_knowledge_base_tool(
    kb_name: str,
    project_id: int,
    kb_controller: KnowledgeBaseController,
) -> Any:
    """
    Create a knowledge base query tool for a specific KB.
    
    Args:
        kb_name: Name of the knowledge base
        project_id: Project ID containing the KB
        kb_controller: Knowledge base controller instance
        
    Returns:
        Async function that can be used as a Pydantic AI tool
    """
    # Get KB table
    try:
        kb_table = kb_controller.get_table(kb_name, project_id)
    except Exception as e:
        logger.error(f"Error getting KB table for {kb_name}: {e}", exc_info=True)
        return None
    
    # Create async tool function
    async def query_knowledge_base(question: str) -> str:
        """
        Query the knowledge base to get relevant information.
        
        Args:
            question: Natural language question to search the knowledge base
            
        Returns:
            Retrieved information from the knowledge base
        """
        try:
            from mindsdb_sql_parser.ast import Select, BinaryOperation, Constant, Identifier, TableField
            
            # Create a SELECT query with content condition
            query = Select(
                targets=[Identifier(parts=["*"])],
                where=BinaryOperation(
                    op="=",
                    args=[
                        Identifier(parts=[TableField.CONTENT.value]),
                        Constant(question)
                    ]
                ),
                limit=Constant(5)  # Top 5 results
            )
            
            # Execute query
            result_df = kb_table.select_query(query)
            
            # Format results
            if result_df.empty:
                return f"No relevant information found in knowledge base '{kb_name}' for the question: {question}"
            
            # Extract content from results
            content_column = "chunk_content" if "chunk_content" in result_df.columns else "content"
            if content_column in result_df.columns:
                contents = result_df[content_column].tolist()
                return "\n\n".join([str(c) for c in contents if c])
            else:
                # Fallback: return first few columns as string
                return result_df.head(5).to_string()
                
        except Exception as e:
            logger.error(f"Error querying knowledge base {kb_name}: {e}", exc_info=True)
            return f"Error querying knowledge base '{kb_name}': {str(e)}"
    
    # Set tool metadata
    query_knowledge_base.__name__ = f"query_kb_{kb_name}"
    query_knowledge_base.__doc__ = (
        f"Query the knowledge base '{kb_name}' to get relevant information. "
        f"Use this tool to answer questions about topics covered in this knowledge base. "
        f"The input should be a clear, specific question."
    )
    
    return query_knowledge_base


def create_retrieval_tool(
    kb_name: str,
    project_id: int,
    kb_controller: KnowledgeBaseController,
    llm: Any,
    embedding_model: Any,
) -> Optional[Any]:
    """
    Create a RAG/retrieval tool for a knowledge base using the RAG pipeline.
    
    Args:
        kb_name: Name of the knowledge base
        project_id: Project ID containing the KB
        kb_controller: Knowledge base controller instance
        llm: LLM instance for RAG pipeline
        embedding_model: Embedding model for RAG pipeline
        
    Returns:
        Async function that can be used as a Pydantic AI tool, or None if KB not found
    """
    # Get KB
    try:
        kb = kb_controller.get(kb_name, project_id)
        if kb is None:
            logger.warning(f"Knowledge base '{kb_name}' not found")
            return None
    except Exception as e:
        logger.error(f"Error getting knowledge base {kb_name}: {e}", exc_info=True)
        return None
    
    # Import RAG pipeline components
    try:
        from mindsdb.integrations.utilities.rag.rag_pipeline_builder import get_pipeline_from_knowledge_base
        from mindsdb.interfaces.knowledge_base.embedding_model_utils import construct_embedding_model_from_args
        
        # Get KB params for RAG pipeline
        kb_params = kb.params or {}
        rag_config = kb_params.get("rag", {})
        
        # Create embedding model if not provided
        if embedding_model is None:
            embedding_args = rag_config.get("embedding_model", {})
            if embedding_args:
                embedding_model = construct_embedding_model_from_args(embedding_args, session=kb_controller.session)
        
        # Get RAG pipeline
        pipeline = get_pipeline_from_knowledge_base(
            kb_name=kb_name,
            project_id=project_id,
            llm=llm,
            embedding_model=embedding_model,
            config=rag_config,
        )
        
    except Exception as e:
        logger.warning(f"Could not create RAG pipeline for {kb_name}, falling back to simple query: {e}")
        # Fallback to simple KB query
        return create_knowledge_base_tool(kb_name, project_id, kb_controller)
    
    # Create async tool function
    async def rag_query(question: str) -> str:
        """
        Query the knowledge base using RAG pipeline to get comprehensive answers.
        
        Args:
            question: Natural language question to answer using the knowledge base
            
        Returns:
            Answer generated from the knowledge base using RAG
        """
        try:
            # Use RAG pipeline to get answer
            if hasattr(pipeline, 'ainvoke'):
                result = await pipeline.ainvoke({"question": question})
            elif hasattr(pipeline, 'invoke'):
                result = pipeline.invoke({"question": question})
            else:
                # Fallback: try calling directly
                result = pipeline(question)
            
            # Extract answer from result
            if isinstance(result, dict):
                answer = result.get("answer", result.get("output", str(result)))
            else:
                answer = str(result)
            
            return answer
            
        except Exception as e:
            logger.error(f"Error in RAG query for {kb_name}: {e}", exc_info=True)
            return f"Error querying knowledge base '{kb_name}': {str(e)}"
    
    # Set tool metadata
    rag_query.__name__ = f"rag_query_{kb_name}"
    rag_query.__doc__ = (
        f"Query the knowledge base '{kb_name}' using RAG (Retrieval-Augmented Generation) "
        f"to get comprehensive answers. Use this tool to answer questions about topics "
        f"covered in this knowledge base. The input should be a clear, specific question."
    )
    
    return rag_query


def build_tools_from_agent_config(
    agent_params: Dict[str, Any],
    command_executor: Any,
    llm: Any,
    embedding_model: Any,
    kb_controller: KnowledgeBaseController,
    project_id: int,
) -> List[Any]:
    """
    Build all tools from agent configuration.
    
    Args:
        agent_params: Agent parameters containing data configuration
        command_executor: Command executor for SQL queries
        llm: LLM instance
        embedding_model: Embedding model instance
        kb_controller: Knowledge base controller
        project_id: Project ID
        
    Returns:
        List of tool functions ready to be registered with Pydantic AI agent
    """
    tools = []
    
    data_config = agent_params.get("data", {})
    tables_list = data_config.get("tables")
    knowledge_bases_list = data_config.get("knowledge_bases")
    
    # Create text2sql tool if tables are configured
    if tables_list or knowledge_bases_list:
        sql_tool = create_text2sql_tool(agent_params, command_executor, llm)
        if sql_tool:
            tools.append(sql_tool)
    
    # Create knowledge base tools
    if knowledge_bases_list:
        for kb_name in knowledge_bases_list:
            # Try to create RAG tool first, fallback to simple KB query
            rag_tool = create_retrieval_tool(
                kb_name, project_id, kb_controller, llm, embedding_model
            )
            if rag_tool:
                tools.append(rag_tool)
            else:
                # Fallback to simple KB query tool
                kb_tool = create_knowledge_base_tool(kb_name, project_id, kb_controller)
                if kb_tool:
                    tools.append(kb_tool)
    
    return tools

