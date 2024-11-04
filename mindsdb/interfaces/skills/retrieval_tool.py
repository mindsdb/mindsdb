from typing import Dict
from langchain.agents import Tool

from mindsdb.integrations.utilities.rag.rag_pipeline_builder import RAG
from mindsdb.integrations.utilities.rag.settings import RAGPipelineModel, VectorStoreType, DEFAULT_COLLECTION_NAME
from mindsdb.interfaces.skills.skill_tool import skill_tool
from mindsdb.interfaces.storage import db

from mindsdb.interfaces.agents.constants import DEFAULT_EMBEDDINGS_MODEL_CLASS
from mindsdb.interfaces.storage.db import KnowledgeBase
from mindsdb.utilities import log

logger = log.getLogger(__name__)


def build_retrieval_tool(tool: dict, pred_args: dict, skill: db.Skills):
    """
    Builds a retrieval tool i.e RAG
    """
    # build RAG config

    tools_config = tool['config']

    # we update the config with the pred_args to allow for custom config
    tools_config.update(pred_args)

    rag_params = _get_rag_params(tools_config)

    if 'vector_store_config' not in rag_params:
        rag_params['vector_store_config'] = {}
        logger.warning(f'No collection_name specified for the retrieval tool, '
                       f"using default collection_name: '{DEFAULT_COLLECTION_NAME}'"
                       f'\nWarning: If this collection does not exist, no data will be retrieved')

    if 'source' in tool:
        kb_name = tool['source']
        executor = skill_tool.get_command_executor()
        kb = _get_knowledge_base(kb_name, skill.project_id, executor)

        if not kb:
            raise ValueError(f"Knowledge base not found: {kb_name}")

        kb_table = executor.session.kb_controller.get_table(kb.name, kb.project_id)

        rag_params['vector_store_config'] = {
            'kb_table': kb_table
        }

    # Can run into weird validation errors when unpacking rag_params directly into constructor.
    if 'embedding_model' in rag_params:
        embedding_model = rag_params['embedding_model']
    else:
        embedding_model = DEFAULT_EMBEDDINGS_MODEL_CLASS()

    rag_config = RAGPipelineModel(
        embedding_model=embedding_model
    )
    if 'documents' in rag_params:
        rag_config.documents = rag_params['documents']
    if 'vector_store_config' in rag_params:
        rag_config.vector_store_config = rag_params['vector_store_config']
    if 'db_connection_string' in rag_params:
        rag_config.db_connection_string = rag_params['db_connection_string']
    if 'table_name' in rag_params:
        rag_config.table_name = rag_params['table_name']
    if 'llm' in rag_params:
        rag_config.llm = rag_params['llm']
    if 'rag_prompt_template' in rag_params:
        rag_config.rag_prompt_template = rag_params['rag_prompt_template']
    if 'retriever_prompt_template' in rag_params:
        rag_config.retriever_prompt_template = rag_params['retriever_prompt_template']

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


def _get_rag_params(pred_args: Dict) -> Dict:
    model_config = pred_args.copy()

    supported_rag_params = RAGPipelineModel.get_field_names()

    rag_params = {k: v for k, v in model_config.items() if k in supported_rag_params}

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
