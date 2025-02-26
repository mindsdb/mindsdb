from langchain_core.tools import Tool
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
import tiktoken

from mindsdb.utilities import log
from mindsdb.interfaces.skills.skill_tool import skill_tool

logger = log.getLogger(__name__)

# Default configuration
DEFAULT_CONFIG = {
    'table_name': 'documents',
    'id_column': 'id',
    'title_column': 'title',
    'content_column': 'content',
    'search_type': 'exact',  # 'exact', 'wildcard', or 'custom'
    'custom_query': None,    # Optional custom SQL query with {search_term} placeholder
    'required_columns': []   # Columns that must be present in custom query results
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
    encoding = tiktoken.get_encoding("gpt2")  # Use GPT-2 tokenizer as a reasonable default
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
    """
    Builds a document retrieval tool that can search documents by name/id and allow chat interactions.

    Args:
        tool: Tool configuration dictionary
        pred_args: Predictor arguments dictionary
        skill: Skills database object

    Returns:
        Tool: Configured document retrieval tool
    """
    tools_config = tool.get('config', {})
    tools_config.update(pred_args)

    # Merge with default config
    config = DEFAULT_CONFIG.copy()
    config.update(tools_config)

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
            # Parse document name/id from query if provided
            doc_id = None
            if "document" in query.lower():
                # Extract document name/id using simple pattern matching
                # Could be enhanced with better NLP/regex patterns
                import re
                doc_patterns = [
                    r'document (?:id|ID|Id)?\s*["\']?([^"\']+)["\']?',
                    r'document\s+["\']?([^"\']+)["\']?'
                ]
                for pattern in doc_patterns:
                    match = re.search(pattern, query)
                    if match:
                        doc_id = match.group(1)
                        break

            if not doc_id:
                return "Please specify a document name or ID to search for."

            # Build and execute SQL query based on configuration
            if config['search_type'] == 'custom' and config['custom_query']:
                # Use custom SQL query if provided
                try:
                    # Format the query with the search term
                    sql = config['custom_query'].format(search_term=doc_id)

                    # Execute the query
                    result = executor.execute_command(sql)
                    if not result or len(result) == 0:
                        return f"Document matching '{doc_id}' not found."

                    # Validate required columns are present
                    doc = result[0]
                    missing_cols = []
                    for col in config['required_columns']:
                        if col not in doc:
                            missing_cols.append(col)
                    if missing_cols:
                        raise ValueError(f"Custom query missing required columns: {', '.join(missing_cols)}")

                    # Use the custom query results
                    content = doc[config['content_column']]
                    doc_id = doc.get(config['id_column'], 'N/A')
                    title = doc.get(config['title_column'], 'N/A')

                    # Handle token limit
                    content = _handle_token_limit(content, pred_args['llm'])

                except KeyError as e:
                    return f"Error in custom query: Missing column {str(e)}"
                except Exception as e:
                    return f"Error executing custom query: {str(e)}"
            else:
                # Use default search behavior
                conditions = []
                if config['search_type'] == 'wildcard':
                    # Use LIKE for wildcard search, case-insensitive
                    conditions.append(f"LOWER(\"{config['title_column']}\") LIKE '%{doc_id.lower()}%'")
                    if config['id_column'] != config['title_column']:
                        conditions.append(f"LOWER(\"{config['id_column']}\") LIKE '%{doc_id.lower()}%'")
                else:
                    # Use exact match
                    conditions.append(f"\"{config['id_column']}\" = '{doc_id}'")
                    if config['id_column'] != config['title_column']:
                        conditions.append(f"\"{config['title_column']}\" = '{doc_id}'")

                sql = f"""
                SELECT \"{config['id_column']}\", \"{config['title_column']}\", \"{config['content_column']}\"
                FROM {config['table_name']}
                WHERE {' OR '.join(conditions)}
                LIMIT 1
                """

                result = executor.execute_command(sql)
                if not result or len(result) == 0:
                    return f"Document matching '{doc_id}' not found."

                doc = result[0]
                content = doc[config['content_column']]
                doc_id = doc[config['id_column']]
                title = doc[config['title_column']]

                # Handle token limit
                content = _handle_token_limit(content, pred_args['llm'])

            # Format initial response
            response = {
                'document_info': {
                    'id': doc_id,
                    'title': title
                },
                'content': content,
                'query_type': 'question' if "?" in query or any(q in query.lower() for q in ["what", "how", "why", "when", "where", "who"]) else 'summary'
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
