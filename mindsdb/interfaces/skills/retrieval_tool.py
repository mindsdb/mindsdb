import pandas as pd

from mindsdb.integrations.utilities.rag.rag_pipeline_builder import RAG
from mindsdb.integrations.utilities.rag.config_loader import load_rag_config
from mindsdb.integrations.utilities.rag.settings import DocumentIdentifier, DEFAULT_DOCUMENT_TYPE_IDENTIFIER, ID_LOOKUP_TOOL_DESCRIPTION, NAME_LOOKUP_TOOL_DESCRIPTION

from mindsdb.interfaces.agents.constants import DEFAULT_EMBEDDINGS_MODEL_CLASS
from mindsdb.interfaces.skills.skill_tool import skill_tool
from mindsdb.interfaces.storage import db
from mindsdb.interfaces.storage.db import KnowledgeBase
from mindsdb.utilities import log
from langchain.chains.llm import LLMChain
from langchain_core.documents import Document
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_core.tools import Tool
from mindsdb.integrations.handlers.langchain_embedding_handler.langchain_embedding_handler import construct_model_from_args
from mindsdb.integrations.libs.response import RESPONSE_TYPE
from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator
from mindsdb.interfaces.agents.langchain_agent import create_chat_model

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

    def _data_frame_to_document(df: pd.DataFrame) -> Document:
        # Restore document from chunks, keeping in mind max context.
        id_filter_condition = FilterCondition(
            f"{metadata_config.embeddings_metadata_column}->>'{metadata_config.doc_id_key}'",
            FilterOperator.EQUAL,
            str(df.get(metadata_config.id_column).item())
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

        metadata = df.to_dict(orient='records')[0]
        return Document(
            page_content=content,
            metadata=metadata
        )

    def _get_documents_by_identifiers(name: str):
        parser = PydanticOutputParser(pydantic_object=DocumentIdentifier)
        document_type_metadata = metadata_config.document_types
        document_type_key_to_metadata = {}
        for metadata in document_type_metadata:
            document_type_key_to_metadata[metadata.key] = metadata

        # Build descriptions of document types with their corresponding IDs
        type_key_description = ''
        for type_key, metadata in document_type_key_to_metadata.items():
            type_key_description += f"{type_key} - {metadata.name} - {metadata.description or ''}\n"

        doc_prompt_template = PromptTemplate(
            input_variables=['format_instructions', 'input', 'type_descriptions'],
            template=metadata_config.document_identifier_prompt_template
        )
        llm = create_chat_model({
            'model_name': metadata_config.llm_config.model_name,
            'provider': metadata_config.llm_config.provider,
            **metadata_config.llm_config.params
        })
        doc_chain = LLMChain(llm=llm, prompt=doc_prompt_template)

        # Generate document identifier based on user input.
        doc_output = doc_chain.predict(
            format_instructions=parser.get_format_instructions(),
            input=name,
            type_descriptions=type_key_description
        )
        document_identifier = parser.invoke(doc_output)

        # Construct SQL query to filter based on generated document identifier.
        base_query = f'''SELECT * FROM {metadata_config.table} AS s WHERE '''
        nrs_document_type_metadata = document_type_key_to_metadata.get(document_identifier.type_key)
        if nrs_document_type_metadata is None:
            # Fallback.
            logger.debug('Could not identify document type. Falling back to retrieving a document using name/title.')
            return _get_document_by_name(name)
        report_number_rex = nrs_document_type_metadata.type_pattern or f'%{document_identifier.type_key}%'
        report_number_rex += f'{document_identifier.document_number}%' if document_identifier.document_number else ''
        if document_identifier.revision:
            report_number_rex += f'REV%{document_identifier.revision}%'
        document_id_report_number_clause = ''
        document_id_title_clause = ''
        if document_identifier.document_number:
            document_id_report_number_clause = f"AND s.\"{metadata_config.type_pattern_column}\" ILIKE '%{document_identifier.document_number}%'"
            document_id_title_clause = f"AND s.\"{metadata_config.name_column}\" ILIKE '%{document_identifier.document_number}%'"
        lookup_query = base_query + f"""
        (

            s."{metadata_config.type_column}" = {nrs_document_type_metadata.id or DEFAULT_DOCUMENT_TYPE_IDENTIFIER}
            AND s."{metadata_config.type_pattern_column}" ILIKE '{report_number_rex}'
            {document_id_report_number_clause}
        )
        OR
        (
            s."{metadata_config.type_column}" = {nrs_document_type_metadata.id or DEFAULT_DOCUMENT_TYPE_IDENTIFIER}
            AND s."{metadata_config.name_column}" ILIKE '{report_number_rex}'
            {document_id_title_clause}
        )

        ORDER BY s."{metadata_config.date_column}" DESC;
        """

        logger.debug(f'Executing query to fetch document: {lookup_query}')
        documents_response = vector_db_handler.native_query(lookup_query)
        if documents_response.resp_type == RESPONSE_TYPE.ERROR:
            # Fallback.
            logger.warning(f'Something went wrong retrieving document by identifiers: {documents_response.error_message}')
            logger.debug('Falling back to retrieving a document using name/title')
            return _get_document_by_name(name)
        if documents_response.data_frame.empty:
            # Fallback.
            logger.debug('Could not find any relevant documents. Falling back to retrieving a document by name/title')
            return None
        document_row = documents_response.data_frame.head(1)
        return _data_frame_to_document(document_row)

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
        found_documents = []
        for _, row in documents_response.data_frame.iterrows():
            found_documents.append(_data_frame_to_document(row))
        return found_documents

    def _lookup_document_by_name(name: str):
        found_documents = _get_documents_by_identifiers(name)
        if not found_documents:
            return f'I could not find any document with name {name}. Please make sure the document name matches exactly.'
        num_docs = len(found_documents)
        if num_docs == 1:
            first_document = found_documents[0]
            # Return directly if we only find one document.
            return f"I found document {first_document.metadata.get(metadata_config.id_column)} with name {first_document.metadata.get(metadata_config.name_column)}. Here is the full document to use as context:\n\n{first_document.page_content}"
        lookup_response = f'I found {num_docs} documents after looking up {name}. Please let me know which ID is the right one and I will fetch the full document:\n\n'
        for doc in found_documents:
            lookup_response += f'ID {doc.metadata.get(metadata_config.id_column)} - {doc.metadata.get(metadata_config.name_column)}\n'
        return lookup_response

    def _lookup_document_by_document_id(id: int):
        lookup_query = f'''SELECT * FROM {metadata_config.table} AS s WHERE s."{metadata_config.id_column}" = {id} LIMIT 1'''
        documents_response = vector_db_handler.native_query(lookup_query)
        if documents_response.resp_type == RESPONSE_TYPE.ERROR:
            # Fallback.
            logger.warning(f'Something went wrong retrieving document by ID {id}: {documents_response.error_message}')
            return None
        if documents_response.data_frame.empty:
            # Fallback.
            logger.debug(f'Could not find any relevant documents by ID {id}')
            return None
        document_row = documents_response.data_frame.head(1)
        return _data_frame_to_document(document_row)

    name_lookup_tool = Tool(
        func=_lookup_document_by_name,
        name=tool.get('name', '') + '_name_lookup',
        description=NAME_LOOKUP_TOOL_DESCRIPTION,
        return_direct=False
    )
    id_lookup_tool = Tool(
        func=_lookup_document_by_document_id,
        name=tool.get('name', '') + '_id_lookup',
        description=ID_LOOKUP_TOOL_DESCRIPTION,
        return_direct=False
    )

    tools = []
    if rag_config.enable_retrieval:
        tools.append(rag_pipeline_tool)
    if rag_config.metadata_config is not None:
        tools.append(name_lookup_tool)
        tools.append(id_lookup_tool)
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
