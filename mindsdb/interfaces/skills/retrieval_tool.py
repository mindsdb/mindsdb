from mindsdb.integrations.utilities.rag.rag_pipeline_builder import RAG
from mindsdb.integrations.utilities.rag.config_loader import load_rag_config

from mindsdb.interfaces.agents.constants import DEFAULT_EMBEDDINGS_MODEL_CLASS
from mindsdb.interfaces.skills.skill_tool import skill_tool
from mindsdb.interfaces.storage import db
from mindsdb.interfaces.storage.db import KnowledgeBase
from mindsdb.utilities import log
from mindsdb.utilities.cache import get_cache
from langchain_core.documents import Document
from langchain_core.tools import Tool
from mindsdb.integrations.handlers.langchain_embedding_handler.langchain_embedding_handler import construct_model_from_args
from mindsdb.integrations.libs.response import RESPONSE_TYPE
from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator

logger = log.getLogger(__name__)


def build_retrieval_tools(tool: dict, pred_args: dict, skill: db.Skills):
    """
    Builds a retrieval tool i.e RAG

    Args:
        tool: Tool configuration dictionary
        pred_args: Predictor arguments dictionary
        skill: Skills database object

    Returns:
        Tool: Configured retrieval tool

    Raises:
        ValueError: If knowledge base is not found or configuration is invalid
    """
    # build RAG config
    tools_config = tool['config']
    tools_config.update(pred_args)

    kb_params = {}
    embeddings_model = None

    if 'source' in tool:
        kb_name = tool['source']
        executor = skill_tool.get_command_executor()
        kb = _get_knowledge_base(kb_name, skill.project_id, executor)

        if not kb:
            raise ValueError(f"Knowledge base not found: {kb_name}")

        kb_table = executor.session.kb_controller.get_table(kb.name, kb.project_id)
        vector_store_config = {
            'kb_table': kb_table
        }
        is_sparse = tools_config.pop('is_sparse', None)
        vector_size = tools_config.pop('vector_size', None)
        if is_sparse is not None:
            vector_store_config['is_sparse'] = is_sparse
        if vector_size is not None:
            vector_store_config['vector_size'] = vector_size
        kb_params = {
            'vector_store_config': vector_store_config
        }

        # Get embedding model from knowledge base table
        if kb_table._kb.embedding_model:
            # Extract embedding model args from knowledge base table
            embedding_args = kb_table._kb.embedding_model.learn_args.get('using', {})
            # Construct the embedding model directly
            embeddings_model = construct_model_from_args(embedding_args)
            logger.debug(f"Using knowledge base embedding model with args: {embedding_args}")
        else:
            embeddings_model = DEFAULT_EMBEDDINGS_MODEL_CLASS()
            logger.debug("Using default embedding model as knowledge base has no embedding model")
    elif 'embedding_model' not in tools_config:
        embeddings_model = DEFAULT_EMBEDDINGS_MODEL_CLASS()
        logger.debug("Using default embedding model as no knowledge base provided")

    # Load and validate config
    rag_pipeline_tool = None
    name_lookup_tool = None
    rag_config = None
    try:
        rag_config = load_rag_config(tools_config, kb_params, embeddings_model)
        # build retriever
        rag_pipeline = RAG(rag_config)
        logger.debug(f"RAG pipeline created with config: {rag_config}")

        def rag_wrapper(query: str) -> str:
            try:
                result = rag_pipeline(query)
                logger.debug(f"RAG pipeline result: {result}")
                return result['answer']
            except Exception as e:
                logger.error(f"Error in RAG pipeline: {str(e)}")
                return f"Error in retrieval: {str(e)}"

        # Create RAG tool
        rag_pipeline_tool = Tool(
            func=rag_wrapper,
            name=tool['name'],
            description=tool['description'],
            response_format='content',
            # Return directly by default since we already use an LLM against retrieved context to generate a response.
            return_direct=tools_config.get('return_direct', True)
        )

    except Exception as e:
        logger.error(f"Error building RAG pipeline: {str(e)}")
        raise ValueError(f"Failed to build RAG pipeline: {str(e)}")
    
    vector_db_handler = kb_table.get_vector_db()
    metadata_config = rag_config.metadata_config

    def _get_document_by_name(name: str):
        if metadata_config.name_column_index is not None:
            tsquery_str = ' & '.join(name.split(' '))
            documents_response = vector_db_handler.native_query(
                f'SELECT * FROM {metadata_config.table} WHERE {metadata_config.name_column_index} @@ to_tsquery(\'{tsquery_str}\') LIMIT 1;'
            )
        else:
            documents_response = vector_db_handler.native_query(
                f'SELECT * FROM {metadata_config.table} WHERE "{metadata_config.name_column}" ILIKE \'%{name}%\' LIMIT 1;'
            )
        if documents_response.resp_type == RESPONSE_TYPE.ERROR:
            raise RuntimeError(f'There was an error looking up documents: {documents_response.error_message}')
        if documents_response.data_frame.empty:
            return None
        document_row = documents_response.data_frame.head(1)
        # Restore document from chunks, keeping in mind max context.
        id_filter_condition = FilterCondition(
            f"{metadata_config.embeddings_metadata_column}->>'{metadata_config.doc_id_key}'",
            FilterOperator.EQUAL,
            str(document_row.get(metadata_config.id_column).item())
        )
        document_chunks_df = vector_db_handler.select(
            metadata_config.embeddings_table,
            conditions=[id_filter_condition]
        )
        if document_chunks_df.empty:
            return None
        sort_col = 'chunk_id' if 'chunk_id' in document_chunks_df.columns else 'id'
        document_chunks_df.sort_values(by=sort_col)
        content = ''
        for _, chunk in document_chunks_df.iterrows():
            if len(content) > metadata_config.max_document_context:
                break
            content += chunk.get(metadata_config.content_column, '')

        return Document(
            page_content=content,
            metadata=document_row.to_dict(orient='records')[0]
        )

    def _lookup_document_by_name(name: str):
        found_document = _get_document_by_name(name)
        if found_document is None:
            return f'I could not find any document with name {name}. Please make sure the document name matches exactly.'
        return f"I found document {found_document.metadata.get(metadata_config.id_column)} with name {found_document.metadata.get(metadata_config.name_column)}. Here is the full document to use as context:\n\n{found_document.page_content}"
    
    def _lookup_documents_by_content(content: str):
        documents_response = vector_db_handler.native_query(
            f'''
                SELECT DISTINCT ON (t."{metadata_config.name_column}")
                    t."{metadata_config.doc_id_key}",
                    t."{metadata_config.name_column}",
                    ts_rank(e."{metadata_config.content_column_index}", plainto_tsquery({content})) as rank
                FROM {metadata_config.embeddings_table} e
                INNER JOIN {metadata_config.table} t ON e."{metadata_config.doc_id_key}" = t."{metadata_config.id_column}"
                WHERE e."{metadata_config.content_column_index}" @@ plainto_tsquery({content})
                ORDER BY t."{metadata_config.name_column}", rank DESC;
            '''
        )

        if documents_response.resp_type == RESPONSE_TYPE.ERROR:
            raise RuntimeError(f'There was an error looking up documents: {documents_response.error_message}')
        if documents_response.data_frame.empty:
            return None

        # TODO: Cache the response.

        return documents_response.data_frame[metadata_config.name_column].to_list()
    
    content_lookup_tool = Tool(
        func=_lookup_documents_by_content,
        name=tool.get('name', '') + '_content_lookup',
        # TODO: Review this description.
        description='You must use this tool ONLY when the user is asking about a specific document by content (e.g. "Find me document with content XXX", "search for document with content XXX"). The input should be the exact content of the document the user is looking for.',
        return_direct=False
    )

    name_lookup_tool = Tool(
        func=_lookup_document_by_name,
        name=tool.get('name', '') + '_name_lookup',
        description='You must use this tool ONLY when the user is asking about a specific document by name or title (e.g. "Find me document XXX", "search for document XXX"). The input should be the exact name of the document the user is looking for.',
        return_direct=False
    )

    def _lookup_document_id_by_name_in_cache(name: str):
        cache = get_cache("content")

        content = cache.get(name)

        if not content:
            return f'I could not find any content for the key {name}. Please make sure this key is valid.'

        # TODO: Fix this based on how the content will be stored.
        return f'I found the some content for the key {name}. Here is the full content to use as context:\n\n{content}'
    
    id_lookup_tool = Tool(
        func=_lookup_document_id_by_name_in_cache,
        name=tool.get('name', '') + '_id_lookup',
        # TODO: Review this description.
        description='You must use this tool FIRST when the user is asking about a specific document by by name or title (e.g. "Find me document XXX", "search for document XXX"). The input should be the exact name of the document the user is looking for. If the document cannot be found, use other tools to search for it.',
        return_direct=False
    )

    tools = [rag_pipeline_tool]
    if rag_config.metadata_config is None:
        return tools
    tools.extend([name_lookup_tool, content_lookup_tool, id_lookup_tool])
    return tools

def _get_knowledge_base(knowledge_base_name: str, project_id, executor) -> KnowledgeBase:
    """
    Get knowledge base by name and project ID

    Args:
        knowledge_base_name: Name of the knowledge base
        project_id: Project ID
        executor: Command executor instance

    Returns:
        KnowledgeBase: Knowledge base instance if found, None otherwise
    """
    kb = executor.session.kb_controller.get(knowledge_base_name, project_id)
    return kb
