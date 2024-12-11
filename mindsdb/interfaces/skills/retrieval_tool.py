from typing import Dict

from mindsdb.integrations.utilities.rag.rag_pipeline_builder import RAG

from mindsdb.integrations.utilities.rag.settings import (
    DEFAULT_COLLECTION_NAME,
    MultiVectorRetrieverMode,
    RAGPipelineModel,
    RetrieverType,
    SearchType,
    SearchKwargs,
    RerankerConfig,
    SummarizationConfig,
    VectorStoreConfig,
    VectorStoreType
)
from mindsdb.interfaces.skills.skill_tool import skill_tool
from mindsdb.interfaces.storage import db

from mindsdb.interfaces.agents.constants import DEFAULT_EMBEDDINGS_MODEL_CLASS
from mindsdb.interfaces.storage.db import KnowledgeBase
from mindsdb.utilities import log
from langchain_core.tools import Tool

logger = log.getLogger(__name__)


def build_retrieval_tool(tool: dict, pred_args: dict, skill: db.Skills):
    """
    Builds a retrieval tool i.e RAG
    """
    # build RAG config

    tools_config = tool['config']

    # we update the config with the pred_args to allow for custom config
    tools_config.update(pred_args)

    kb_params = {}
    kb_table = None
    if 'source' in tool:
        kb_name = tool['source']
        executor = skill_tool.get_command_executor()
        kb = _get_knowledge_base(kb_name, skill.project_id, executor)

        if not kb:
            raise ValueError(f"Knowledge base not found: {kb_name}")

        kb_table = executor.session.kb_controller.get_table(kb.name, kb.project_id)
        if kb.params is not None:
            kb_params = kb.params
        kb_params['vector_store_config'] = {
            'kb_table': kb_table
        }

    rag_params = _get_rag_params(tools_config, kb_params)

    # use knowledge base table embedding model by default, even if already set
    if kb_table is not None:
        # Get embedding model from knowledge base table
        kb_embedding_model = kb_table._kb.embedding_model
        if kb_embedding_model:
            # Extract embedding model args from knowledge base table
            embedding_args = kb_embedding_model.learn_args.get('using', {})
            # Construct the embedding model directly
            from mindsdb.integrations.handlers.langchain_embedding_handler.langchain_embedding_handler import construct_model_from_args
            rag_params['embedding_model'] = construct_model_from_args(embedding_args)
            logger.debug(f"Using knowledge base embedding model with args: {embedding_args}")
        else:
            rag_params['embedding_model'] = DEFAULT_EMBEDDINGS_MODEL_CLASS()
            logger.debug("Using default embedding model as knowledge base has no embedding model")
    elif 'embedding_model' not in rag_params:
        rag_params['embedding_model'] = DEFAULT_EMBEDDINGS_MODEL_CLASS()
        logger.debug("Using default embedding model as no knowledge base provided")

    logger.debug(f"Final RAG configuration: {rag_params}")

    # Handle enums and type conversions
    if 'retriever_type' in rag_params:
        rag_params['retriever_type'] = RetrieverType(rag_params['retriever_type'])
    if 'multi_retriever_mode' in rag_params:
        rag_params['multi_retriever_mode'] = MultiVectorRetrieverMode(rag_params['multi_retriever_mode'])
    if 'search_type' in rag_params:
        rag_params['search_type'] = SearchType(rag_params['search_type'])

    # Handle search kwargs if present
    if 'search_kwargs' in rag_params and isinstance(rag_params['search_kwargs'], dict):
        rag_params['search_kwargs'] = SearchKwargs(**rag_params['search_kwargs'])

    # Handle summarization config if present
    summarization_config = rag_params.get('summarization_config')
    if summarization_config is not None and isinstance(summarization_config, dict):
        rag_params['summarization_config'] = SummarizationConfig(**summarization_config)

    # Handle vector store config separately since it has a nested default
    if 'vector_store_config' in rag_params:
        if isinstance(rag_params['vector_store_config'], dict):
            rag_params['vector_store_config'] = VectorStoreConfig(**rag_params['vector_store_config'])

    if 'reranker_config' in rag_params:
        rag_params['reranker_config'] = RerankerConfig(**rag_params['reranker_config'])

    if 'vector_store_config' not in rag_params:
        rag_params['vector_store_config'] = {}
        logger.warning(f'No collection_name specified for the retrieval tool, '
                       f"using default collection_name: '{DEFAULT_COLLECTION_NAME}'"
                       f'\nWarning: If this collection does not exist, no data will be retrieved')

    # Create config with filtered params
    rag_config = RAGPipelineModel(**rag_params)

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
    return Tool(
        func=rag_wrapper,
        name=tool['name'],
        description=tool['description'],
        response_format='content',
        # Return directly by default since we already use an LLM against retrieved context to generate a response.
        return_direct=tools_config.get('return_direct', True)
    )


def _get_rag_params(tools_config: Dict, knowledge_base_params: Dict) -> Dict:
    """Convert tools config to RAG parameters"""
    # Get valid fields from RAGPipelineModel
    valid_fields = RAGPipelineModel.get_field_names()

    # Filter out invalid parameters
    rag_params = {k: v for k, v in tools_config.items() if k in valid_fields}

    knowledge_base_rag_params = {k: v for k, v in knowledge_base_params.items() if k in valid_fields}

    # Prioritize knowledge base params over tool config.
    rag_params.update(knowledge_base_rag_params)

    # Ensure default embedding model is set
    if 'embedding_model' not in rag_params:
        rag_params['embedding_model'] = DEFAULT_EMBEDDINGS_MODEL_CLASS()

    return rag_params


def _get_knowledge_base(knowledge_base_name: str, project_id, executor) -> db.KnowledgeBase:

    kb = executor.session.kb_controller.get(knowledge_base_name, project_id)

    return kb


def _build_vector_store_config_from_knowledge_base(rag_params: Dict, knowledge_base: KnowledgeBase, executor) -> Dict:
    """
    build vector store config from knowledge base
    """

    vector_store_config = rag_params['vector_store_config'].copy()

    vector_store_type = knowledge_base.vector_database.engine
    vector_store_config['vector_store_type'] = vector_store_type

    if vector_store_type == VectorStoreType.CHROMA.value:
        # For chromadb used, we get persist_directory
        vector_store_folder_name = knowledge_base.vector_database.data['persist_directory']
        integration_handler = executor.session.integration_controller.get_data_handler(
            knowledge_base.vector_database.name
        )
        persist_dir = integration_handler.handler_storage.folder_get(vector_store_folder_name)
        vector_store_config['persist_directory'] = persist_dir

    elif vector_store_type == VectorStoreType.PGVECTOR.value:
        # For pgvector, we get connection string
        # todo requires further testing

        # get pgvector runtime data
        kb_table = executor.session.kb_controller.get_table(knowledge_base.name, knowledge_base.project_id)
        vector_db = kb_table.get_vector_db()
        connection_params = vector_db.connection_args
        vector_store_config['collection_name'] = vector_db._check_table(knowledge_base.vector_database_table)

        vector_store_config['connection_string'] = _create_conn_string(connection_params)

    else:
        raise ValueError(f"Invalid vector store type: {vector_store_type}. "
                         f"Only {[v.name for v in VectorStoreType]} are currently supported.")

    return vector_store_config


def _create_conn_string(connection_args: dict) -> str:
    """
    Creates a PostgreSQL connection string from connection args.
    """
    user = connection_args.get('user')
    host = connection_args.get('host')
    port = connection_args.get('port')
    password = connection_args.get('password')
    dbname = connection_args.get('database')

    if password:
        return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    else:
        return f"postgresql://{user}@{host}:{port}/{dbname}"
