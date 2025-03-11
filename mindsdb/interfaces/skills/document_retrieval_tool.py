from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field
from langchain_core.tools import Tool
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
import tiktoken

from mindsdb.utilities import log

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
    source: str = Field(
        default='scientech_kb',
        description='Knowledge base source'
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


def _get_knowledge_base(kb_name, project_id, executor):
    """
    Get knowledge base by name and project ID

    Args:
        kb_name: Name of the knowledge base
        project_id: Project ID
        executor: Command executor

    Returns:
        Knowledge base object or None if not found
    """
    try:
        # Get knowledge base from executor
        kb = executor.session.kb_controller.get(kb_name, project_id)
        return kb
    except Exception as e:
        logger.error(f"Error getting knowledge base {kb_name}: {str(e)}")
        return None


def build_document_retrieval_tool(tool: dict, pred_args: dict) -> Tool:
    """
    Build a document retrieval tool that can search documents by name/id and allow chat interactions.

    Args:
        tool: Tool configuration
        pred_args: Prediction arguments including LLM and config

    Returns:
        Tool: Document retrieval tool
    """
    # Get tool configuration
    config = tool.get('config', {})

    # Create DocumentConfig from config
    try:
        doc_config = DocumentConfig(**config)
        pred_args['config'] = doc_config
    except Exception as e:
        logger.error(f"Error in document retrieval config: {str(e)}")
        doc_config = DocumentConfig()
        pred_args['config'] = doc_config

    # Get knowledge base
    if 'source' not in tool:
        raise ValueError("Knowledge base for tool not found")

    kb_name = tool['source']
    executor = pred_args.get('executor')
    if not executor:
        raise ValueError("Executor not found in pred_args")

    # Get project ID from skill
    skill = pred_args.get('skill')
    if not skill:
        raise ValueError("Skill not found in pred_args")

    project_id = getattr(skill, 'project_id', None)
    if not project_id:
        raise ValueError("Project ID not found in skill")

    # Get knowledge base
    kb = _get_knowledge_base(kb_name, project_id, executor)
    if not kb:
        raise ValueError(f"Knowledge base not found: {kb_name}")

    # Get vector database handler
    kb_table = executor.session.kb_controller.get_table(kb.name, project_id)
    vector_db_handler = kb_table.get_vector_db()

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

            if not doc_id:
                return "I couldn't identify a specific document in your query. Please provide a document ID or title."

            # Prepare the search term
            safe_doc_id = doc_id.replace("'", "''")  # Escape single quotes for SQL

            try:
                # Query the document metadata
                documents_response = vector_db_handler.native_query(
                    f'SELECT * FROM {doc_config.metadata_table} WHERE "{doc_config.metadata_id_column}" ILIKE \'%{safe_doc_id}%\' OR "{doc_config.metadata_title_column}" ILIKE \'%{safe_doc_id}%\' LIMIT 1;'
                )

                # Check for errors
                if hasattr(documents_response, 'resp_type') and getattr(documents_response, 'resp_type', None) == 'error':
                    raise RuntimeError(f'There was an error looking up documents: {getattr(documents_response, "error_message", "Unknown error")}')

                # Check if we found any documents
                if hasattr(documents_response, 'data_frame') and getattr(documents_response, 'data_frame', None) is not None:
                    if documents_response.data_frame.empty:
                        return f"Document matching '{doc_id}' not found."

                    # Get the first document
                    document_row = documents_response.data_frame.head(1)
                    doc_id = document_row.get(doc_config.metadata_id_column).item()
                    title = document_row.get(doc_config.metadata_title_column).item()

                    # Query the document content
                    from mindsdb.integrations.handlers.vector_db.models.filter_models import FilterCondition, FilterOperator

                    # Create a filter condition to get chunks for this document
                    id_filter_condition = FilterCondition(
                        "metadata->>'original_row_id'",
                        FilterOperator.EQUAL,
                        str(doc_id)
                    )

                    # Get document chunks
                    document_chunks_df = vector_db_handler.select(
                        doc_config.content_table,
                        conditions=[id_filter_condition]
                    )

                    if document_chunks_df.empty:
                        return f"Content not found for document '{title}' (ID: {doc_id})"

                    # Sort chunks by order
                    sort_col = doc_config.content_order_column if doc_config.content_order_column in document_chunks_df.columns else 'id'
                    document_chunks_df = document_chunks_df.sort_values(by=sort_col)

                    # Get content with token limit
                    content = ''
                    for _, chunk in document_chunks_df.iterrows():
                        chunk_content = chunk.get(doc_config.content_column, '')
                        content += chunk_content

                    # Handle token limit with the LLM's max token limit
                    content = _handle_token_limit(content, pred_args['llm'])

                    # Structure the response
                    response = {
                        'document_info': {
                            'id': doc_id,
                            'title': title
                        },
                        'content': content,
                        'query_analysis': {
                            'query_type': query_type,
                            'extracted_id': doc_id,
                            'reasoning': reasoning,
                            'confidence': 'high' if analysis.get('extracted_id') else 'medium',
                            'method': 'llm_analysis'
                        }
                    }

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
                else:
                    return f"Document matching '{doc_id}' not found."

            except Exception as e:
                logger.error(f"Error in document retrieval query: {str(e)}")
                thought = "I encountered an error while trying to retrieve the document."
                observation = f"Error retrieving document: {str(e)}"
                follow_up = "I should inform the user about this error."
                return f"Thought: {thought}\nObservation: {observation}\nThought: {follow_up}\nAI: I'm sorry, but I encountered an error while trying to retrieve the document. {str(e)}"

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
