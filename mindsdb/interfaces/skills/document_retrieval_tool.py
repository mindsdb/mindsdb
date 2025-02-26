from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, Field
from langchain_core.tools import Tool
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
import tiktoken
import re
from mindsdb_sql_parser import parse_sql

from mindsdb.utilities import log
from mindsdb.interfaces.skills.skill_tool import skill_tool

logger = log.getLogger(__name__)


class SearchType(str, Enum):
    """Available search types for document retrieval"""
    EXACT = 'exact'
    WILDCARD = 'wildcard'
    CUSTOM = 'custom'


class DocumentConfig(BaseModel):
    """Configuration for document retrieval tool"""

    # Database configuration
    database: Optional[str] = Field(
        default='cw_db',
        description='Database name containing document tables'
    )
    metadata_table: str = Field(
        default='scientech_document',
        description='Table containing document metadata'
    )
    metadata_id_column: str = Field(
        default='Id',
        description='Column in metadata table containing document ID'
    )
    metadata_title_column: str = Field(
        default='Title',
        description='Column in metadata table containing document title'
    )
    content_table: str = Field(
        default='embeddings',
        description='Table containing document content'
    )
    content_id_column: str = Field(
        default='document_id',
        description='Column in content table containing document ID'
    )
    content_column: str = Field(
        default='content',
        description='Column in content table containing document content'
    )
    content_order_column: str = Field(
        default='chunk_id',
        description='Column in content table for ordering content chunks'
    )

    # Custom query configuration
    id_column: str = Field(
        default='id',
        description='ID column in custom query results'
    )
    title_column: str = Field(
        default='title',
        description='Title column in custom query results'
    )

    search_type: SearchType = Field(
        default=SearchType.WILDCARD,
        description='Type of search to perform'
    )
    custom_query: Optional[str] = Field(
        default=None,
        description='Optional custom SQL query with {search_term} placeholder',
        pattern=r'.*\{search_term\}.*'  # Must contain placeholder if provided
    )
    required_columns: List[str] = Field(
        default_factory=list,
        description='Columns that must be present in custom query results',
        min_items=0
    )

    # Allow additional fields (like llm) for backward compatibility
    class Config:
        extra = "allow"


# Default configuration
DEFAULT_CONFIG = DocumentConfig()


def _analyze_query(query: str, model) -> dict:
    """Analyze the query to determine its type and extract relevant information.

    Args:
        query (str): The user's query
        model: LLM model for analysis

    Returns:
        dict: Analysis results containing query_type and extracted_id
    """
    template = """Analyze the following document query and determine:
1. If it's a question (asking for specific information) or a request for document summary
2. Extract any document ID or title mentioned in the query

Query: {query}

Respond in the following JSON format:
{{
    "query_type": "question" or "summary",
    "extracted_id": "<extracted id or title, or null if none found>",
    "reasoning": "<brief explanation of your analysis>"
}}
"""

    prompt = PromptTemplate(template=template, input_variables=["query"])
    chain = LLMChain(llm=model, prompt=prompt)

    try:
        response = chain.run(query=query)
        # Convert string response to dict, handling potential JSON formatting issues
        import json
        result = json.loads(response)
        return result
    except Exception as e:
        logger.error(f"Error analyzing query: {str(e)}")
        # Fallback to simple analysis
        has_question = any(q in query.lower() for q in ["what", "how", "why", "when", "where", "who", "?"])
        return {
            "query_type": "question" if has_question else "summary",
            "extracted_id": None,
            "reasoning": "Fallback analysis due to error"
        }


def _handle_token_limit(content: str, model, max_tokens: int = 32000, budget_multiplier: float = 0.8) -> str:
    """Handle token limit by summarizing content if it exceeds the limit

    Args:
        content (str): Document content to check
        model: Model interface for summarization
        max_tokens (int, optional): Maximum tokens allowed. Defaults to 32000.
        budget_multiplier (float, optional): Conservative multiplier for token budget. Defaults to 0.8.

    Returns:
        str: Original or summarized content
    """
    # Get token count
    encoding = tiktoken.get_encoding("gpt4")
    n_tokens = len(encoding.encode(content))

    # Return original content if within limit
    if n_tokens <= max_tokens * budget_multiplier:
        return content

    # Create map-reduce summarization chains
    map_template = """Here is a document segment to analyze:
    {docs}
    Please identify the key points and main ideas from this segment.
    Key Points:"""
    map_prompt = PromptTemplate.from_template(map_template)
    map_chain = LLMChain(llm=model, prompt=map_prompt)

    reduce_template = """Here are summaries of different segments of a document:
    {doc_summaries}
    Please create a coherent summary that captures all the important information.
    Summary:"""
    reduce_prompt = PromptTemplate.from_template(reduce_template)
    reduce_chain = LLMChain(llm=model, prompt=reduce_prompt)

    # Split content into chunks that fit within token limit
    chunk_size = int(max_tokens * budget_multiplier / 2)  # Conservative chunk size
    chunks = [content[i:i + chunk_size] for i in range(0, len(content), chunk_size)]

    # Map: Summarize each chunk
    summaries = []
    for chunk in chunks:
        try:
            summary = map_chain.run(docs=chunk)
            summaries.append(summary)
        except Exception as e:
            logger.error(f"Error summarizing chunk: {str(e)}")
            continue

    # Reduce: Combine summaries
    try:
        final_summary = reduce_chain.run(doc_summaries="\n".join(summaries))
        return final_summary
    except Exception as e:
        logger.error(f"Error combining summaries: {str(e)}")
        # Fall back to first summary if reduce fails
        return summaries[0] if summaries else "Error: Could not summarize document"


def build_document_retrieval_tool(tool: dict, pred_args: dict, skill) -> Tool:
    """Build a document retrieval tool that can search documents by name/id and allow chat interactions.

    Args:
        tool: Tool configuration dictionary
        pred_args: Predictor arguments dictionary
        skill: Skills database object

    Returns:
        Tool: Configured document retrieval tool
    """
    """
    Builds a document retrieval tool that can search documents by name/id and allow chat interactions.

    Args:
        tool: Tool configuration dictionary
        pred_args: Predictor arguments dictionary
        skill: Skills database object

    Returns:
        Tool: Configured document retrieval tool
    """
    # Create config from tool config and pred_args
    config_dict = tool.get('config', {})
    config_dict.update(pred_args)

    # Create Pydantic model from config
    try:
        config = DocumentConfig(**config_dict)
    except Exception as e:
        logger.error(f"Error in document retrieval config: {str(e)}")
        config = DEFAULT_CONFIG.copy(deep=True)

    # Get command executor for SQL queries
    executor = skill_tool.get_command_executor()

    def retrieve_document(query: str) -> str:
        """
        Retrieve document content by name/id and allow chat interactions.

        Args:
            query: Document name/id or question about a document

        Returns:
            str: Document content or answer to question
        """
        # Initialize variables that might be referenced in exception handlers
        thought = "I'm processing your document request."
        observation = ""
        action = "I will analyze the document content to help answer your query."

        try:
            # Analyze query using LLM
            analysis = _analyze_query(query, pred_args['llm'])
            doc_id = analysis.get('extracted_id')
            query_type = analysis.get('query_type', 'question')
            reasoning = analysis.get('reasoning', '')

            # Log analysis results for debugging
            logger.debug(f"Query analysis: {analysis}")

            # Fallback to regex if LLM couldn't find an ID
            if not doc_id:
                for pattern in [
                    r'document[s]* (?:id|ID|Id)[: ]*([\w-]+)',  # Match 'document id: ABC123'
                    r'doc(?:ument)*[s]* [\"\']*([\w-]+)[\"\']*',     # Match 'document \"ABC123\"' or 'doc ABC123'
                    r'[\"\']*([\w-]+)[\"\']*'                      # Match any quoted ID as fallback
                ]:
                    match = re.search(pattern, query)
                    if match:
                        doc_id = match.group(1)
                        # Update analysis with regex-found ID
                        analysis['extracted_id'] = doc_id
                        analysis['reasoning'] += " (ID found via pattern matching)"
                        break

            # If still no doc_id, try to use the query itself as the doc_id
            if not doc_id and query and query.strip():
                doc_id = query.strip()
                analysis['extracted_id'] = doc_id
                analysis['reasoning'] += " (Using query as document ID)"

            if not doc_id:
                if query_type == 'summary':
                    return "Please specify which document you would like me to summarize."
                else:
                    return "Please specify which document you would like me to search for information in."

            # Build and execute SQL query based on configuration
            if config.search_type == SearchType.CUSTOM and config.custom_query:
                # Use custom SQL query if provided
                try:
                    # Format the query with the search term and database if provided
                    query_params = {'search_term': doc_id}
                    if config.database:
                        query_params['database'] = f'"{config.database}"'

                    # Format the query with parameters
                    sql = config.custom_query.format(**query_params)

                    # Parse SQL query into AST
                    ast = parse_sql(sql)

                    # Execute the query
                    result = executor.execute_command(
                        ast,
                        database_name=config.database if config.database else None
                    )

                    # Check if result exists and has data
                    if not result or not hasattr(result, '__iter__'):
                        return f"Document matching '{doc_id}' not found."

                    # Convert to list if it's not already
                    result_list = list(result)
                    if not result_list:
                        return f"Document matching '{doc_id}' not found."

                    # Validate required columns are present
                    doc = result_list[0]
                    missing_cols = []
                    for col in config.required_columns:
                        if col not in doc:
                            missing_cols.append(col)
                    if missing_cols:
                        raise ValueError(f"Custom query missing required columns: {', '.join(missing_cols)}")

                    # Use the custom query results
                    content = doc[config.content_column]
                    doc_id = doc.get(config.id_column, 'N/A')
                    title = doc.get(config.title_column, 'N/A')

                    # Handle token limit
                    content = _handle_token_limit(content, pred_args['llm'])

                except KeyError as e:
                    return f"Error in custom query: Missing column {str(e)}"
                except Exception as e:
                    return f"Error executing custom query: {str(e)}"
            else:

                if config.search_type == SearchType.WILDCARD:
                    # First, search in metadata table by title
                    table_ref = f'"{config.database}"."{config.metadata_table}"' if config.database else f'"{config.metadata_table}"'
                    # Ensure doc_id is not None before calling lower()
                    safe_doc_id = doc_id.lower() if doc_id else ""
                    sql = f"""
                    SELECT "{config.metadata_id_column}", "{config.metadata_title_column}"
                    FROM {table_ref}
                    WHERE "{config.metadata_title_column}" LIKE '%{safe_doc_id}%'
                    LIMIT 10
                    """
                else:
                    # Exact match by ID or title
                    table_ref = f'"{config.database}"."{config.metadata_table}"' if config.database else f'"{config.metadata_table}"'
                    sql = f"""
                    SELECT "{config.metadata_id_column}", "{config.metadata_title_column}"
                    FROM {table_ref}
                    WHERE "{config.metadata_id_column}" = '{doc_id}'
                       OR "{config.metadata_title_column}" = '{doc_id}'
                    LIMIT 1
                    """

                # Parse SQL query into AST
                ast = parse_sql(sql)

                result = executor.execute_command(
                    ast,
                    database_name=config.database if config.database else None
                )
                if not result or not hasattr(result, '__iter__'):
                    return f"Document matching '{doc_id}' not found."

                # Get metadata
                result_list = list(result)
                if not result_list:
                    return f"Document matching '{doc_id}' not found."

                doc = result_list[0]
                doc_id = doc[config.metadata_id_column]
                title = doc[config.metadata_title_column]

                # Get content
                content_table_ref = f'"{config.database}"."{config.content_table}"' if config.database else f'"{config.content_table}"'
                content_sql = f"""
                SELECT {config.content_column}
                FROM {content_table_ref}
                WHERE {config.content_id_column} = '{doc_id}'
                ORDER BY {config.content_order_column}
                """
                # Parse SQL query into AST
                content_ast = parse_sql(content_sql)
                content_result = executor.execute_command(
                    content_ast,
                    database_name=config.database if config.database else None
                )

                if not content_result or not hasattr(content_result, '__iter__'):
                    return f"Content not found for document '{title}' (ID: {doc_id})"

                # Combine all content chunks
                content_result_list = list(content_result)
                if not content_result_list:
                    return f"Content not found for document '{title}' (ID: {doc_id})"

                content = '\n'.join(row[0] for row in content_result_list)

                # Handle token limit
                content = _handle_token_limit(content, pred_args['llm'])

            # Format initial response
            response = {
                'document_info': {
                    'id': doc_id,
                    'title': title
                },
                'content': content,
                'query_type': query_type,
                'reasoning': reasoning,
                'analysis': {
                    'is_question': query_type == 'question',
                    'requires_summary': query_type == 'summary',
                    'confidence': 'high' if analysis.get('extracted_id') else 'medium',
                    'method': 'llm_analysis'
                }
            }

            # Handle token limit
            content = _handle_token_limit(response['content'], pred_args['llm'])

            # Structure the response in Langchain's ReAct format
            thought = "I found the relevant document. Let me analyze its content to help answer the query."

            if query_type == 'question':
                observation = (
                    f"Document Found:\n"
                    f"- Title: {response['document_info']['title']}\n"
                    f"- ID: {response['document_info']['id']}\n\n"
                    f"Question: {query}\n\n"
                    f"Document Content:\n{content}"
                )
                action = "Now I will analyze the content to answer the specific question."
            else:
                observation = (
                    f"Document Found:\n"
                    f"- Title: {response['document_info']['title']}\n"
                    f"- ID: {response['document_info']['id']}\n\n"
                    f"Document Content:\n{content}"
                )
                action = "Now I will create a clear and concise summary of the main points in this document."

            # Format in Langchain's expected format
            return f"Thought: {thought}\nObservation: {observation}\nThought: {action}\nAI: Let me help you with that."

        except Exception as e:
            logger.error(f"Error in document retrieval: {str(e)}")
            thought = "I encountered an error while trying to retrieve the document."
            observation = f"Error retrieving document: {str(e)}"
            follow_up = "I should inform the user about this error."
            return f"Thought: {thought}\nObservation: {observation}\nThought: {follow_up}\nAI: I'm sorry, but I encountered an error while trying to retrieve the document. {str(e)}"

    return Tool(
        name=tool.get('name', 'document_retrieval'),
        description=tool.get('description', 'Search for documents by name/id and ask questions about them.'),
        func=retrieve_document,
        return_direct=False  # Don't return directly so the LLM can process the content
    )
