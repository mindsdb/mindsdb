from mindsdb.integrations.utilities.rag.rag_pipeline_builder import RAG
from mindsdb.integrations.utilities.rag.config_loader import load_rag_config

from mindsdb.interfaces.agents.constants import DEFAULT_EMBEDDINGS_MODEL_CLASS
from mindsdb.interfaces.skills.skill_tool import skill_tool
from mindsdb.interfaces.storage import db
from mindsdb.interfaces.storage.db import KnowledgeBase
from mindsdb.utilities import log
from langchain_core.tools import Tool
from mindsdb.integrations.handlers.langchain_embedding_handler.langchain_embedding_handler import construct_model_from_args

logger = log.getLogger(__name__)


def build_retrieval_tool(tool: dict, pred_args: dict, skill: db.Skills):
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
        return Tool(
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
