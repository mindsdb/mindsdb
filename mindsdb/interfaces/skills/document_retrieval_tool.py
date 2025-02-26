from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, Field, ConfigDict
from langchain_core.tools import Tool
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
import tiktoken
import re

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
    model_config = ConfigDict(frozen=True, extra='forbid')

    table_name: str = Field(
        default='documents',
        description='Name of the table containing documents',
        pattern=r'^[a-zA-Z_][a-zA-Z0-9_]*$'  # Valid SQL table name
    )
    id_column: str = Field(
        default='id',
        description='Column name for document ID',
        pattern=r'^[a-zA-Z_][a-zA-Z0-9_]*$'  # Valid SQL column name
    )
    title_column: str = Field(
        default='title',
        description='Column name for document title',
        pattern=r'^[a-zA-Z_][a-zA-Z0-9_]*$'  # Valid SQL column name
    )
    content_column: str = Field(
        default='content',
        description='Column name for document content',
        pattern=r'^[a-zA-Z_][a-zA-Z0-9_]*$'  # Valid SQL column name
    )
    search_type: SearchType = Field(
        default=SearchType.EXACT,
        description='Type of search to perform'
    )
    custom_query: Optional[str] = Field(
        default=None,
        description='Optional custom SQL query with {search_term} placeholder',
        pattern=r'.*\{search_term\}.*' if '{search_term}' in str else None  # Must contain placeholder if provided
    )
    required_columns: List[str] = Field(
        default_factory=list,
        description='Columns that must be present in custom query results',
        min_items=0
    )


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
        try:
            # Analyze query using LLM
            analysis = _analyze_query(query, pred_args['llm'])
            doc_id = analysis.get('extracted_id')

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
                        break

            if not doc_id:
                return "Please specify a document name or ID to search for."

            # Build and execute SQL query based on configuration
            if config.search_type == SearchType.CUSTOM and config.custom_query:
                # Use custom SQL query if provided
                try:
                    # Format the query with the search term
                    sql = config.custom_query.format(search_term=doc_id)

                    # Execute the query
                    result = executor.execute_command(sql)
                    if not result or len(result) == 0:
                        return f"Document matching '{doc_id}' not found."

                    # Validate required columns are present
                    doc = result[0]
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
                # Use default search behavior
                conditions = []
                if config.search_type == SearchType.WILDCARD:
                    # Use LIKE for wildcard search, case-insensitive
                    conditions.append(f"LOWER({config.title_column}) LIKE '%{doc_id.lower()}%'")
                    if config.id_column != config.title_column:
                        conditions.append(f"LOWER({config.id_column}) LIKE '%{doc_id.lower()}%'")
                else:
                    # Use exact match
                    conditions.append(f"{config.id_column} = '{doc_id}'")
                    if config.id_column != config.title_column:
                        conditions.append(f"{config.title_column} = '{doc_id}'")

                sql = f"""
                SELECT {config.id_column}, {config.title_column}, {config.content_column}
                FROM {config.table_name}
                WHERE {' OR '.join(conditions)}
                LIMIT 1
                """

                result = executor.execute_command(sql)
                if not result or len(result) == 0:
                    return f"Document matching '{doc_id}' not found."

                doc = result[0]
                content = doc[config.content_column]
                doc_id = doc[config.id_column]
                title = doc[config.title_column]

                # Handle token limit
                content = _handle_token_limit(content, pred_args['llm'])

            # Format initial response
            response = {
                'document_info': {
                    'id': doc_id,
                    'title': title
                },
                'content': content,
                'query_type': analysis['query_type'],
                'reasoning': analysis.get('reasoning', '')
            }

            # Handle token limit
            content = _handle_token_limit(response['content'], pred_args['llm'])

            # Structure the response in Langchain's ReAct format
            thought = "I found the relevant document. Let me analyze its content to help answer the query."

            if response['query_type'] == 'question':
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
            return f"Error retrieving document: {str(e)}"

    return Tool(
        name=tool.get('name', 'document_retrieval'),
        description=tool.get('description', 'Search for documents by name/id and ask questions about them.'),
        func=retrieve_document,
        return_direct=False  # Don't return directly so the LLM can process the content
    )
