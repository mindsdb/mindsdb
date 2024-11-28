from typing import Any, Dict, List, Optional

from mindsdb.interfaces.agents.langchain_agent import create_chat_model
from langchain.chains.base import Chain
from langchain.chains.combine_documents.stuff import StuffDocumentsChain
from langchain.chains.llm import LLMChain
from langchain.chains.combine_documents.map_reduce import MapReduceDocumentsChain, ReduceDocumentsChain
from langchain_core.callbacks.manager import CallbackManagerForChainRun
from langchain_core.documents import Document
from langchain_core.prompts import PromptTemplate

from mindsdb.integrations.libs.vectordatabase_handler import VectorStoreHandler
from mindsdb.integrations.utilities.rag.settings import SummarizationConfig
from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator
from mindsdb.utilities import log

logger = log.getLogger(__name__)


def create_map_reduce_documents_chain(summarization_config: SummarizationConfig) -> MapReduceDocumentsChain:
    '''Creats a chain that map reduces documents into a single consolidated summary

    Args:
        summarization_config (SummarizationConfig): Configuration for how to perform summarization

    Returns:
        chain (MapReduceDocumentsChain): Chain that map reduces documents.
    '''
    summarization_llm = create_chat_model({
        'model_name': summarization_config.llm_config.model_name,
        'provider': summarization_config.llm_config.provider,
        **summarization_config.llm_config.params
    })
    map_prompt_template = summarization_config.map_prompt_template
    map_prompt = PromptTemplate.from_template(map_prompt_template)
    # Handles summarization of individual chunks.
    map_chain = LLMChain(llm=summarization_llm, prompt=map_prompt)

    reduce_prompt_template = summarization_config.reduce_prompt_template
    reduce_prompt = PromptTemplate.from_template(reduce_prompt_template)
    # Combines summarizations from multiple chunks into a consolidated summary.
    reduce_chain = LLMChain(llm=summarization_llm, prompt=reduce_prompt)

    # Takes a list of docs, combines them into a single string, then passes to an LLMChain.
    combine_documents_chain = StuffDocumentsChain(
        llm_chain=reduce_chain,
        document_variable_name='docs'
    )

    # Combines & iteratively reduces mapped documents.
    reduce_documents_chain = ReduceDocumentsChain(
        combine_documents_chain=combine_documents_chain,
        collapse_documents_chain=combine_documents_chain,
        # Max number of tokens to group documents into.
        token_max=summarization_config.max_summarization_tokens
    )
    return MapReduceDocumentsChain(
        llm_chain=map_chain,
        reduce_documents_chain=reduce_documents_chain,
        document_variable_name='docs',
        return_intermediate_steps=False
    )


class MapReduceSummarizerChain(Chain):
    '''Chain to summarize the source documents for document chunks & return as context'''

    context_key: str = 'context'
    metadata_key: str = 'metadata'
    doc_id_key: str = 'original_row_id'

    vector_store_handler: VectorStoreHandler
    table_name: str = 'embeddings'
    id_column_name: str = 'id'
    content_column_name: str = 'content'
    metadata_column_name: str = 'metadata'

    map_reduce_documents_chain: MapReduceDocumentsChain

    @property
    def input_keys(self) -> List[str]:
        return [self.context_key, 'question']

    @property
    def output_keys(self) -> List[str]:
        return [self.context_key, 'question']

    def _get_document_ids_from_chunks(self, chunks: List[Document]) -> List[str]:
        unique_document_ids = set()
        document_ids = []
        for chunk in chunks:
            metadata = chunk.metadata
            doc_id = str(metadata.get(self.doc_id_key, ''))
            if doc_id not in unique_document_ids:
                # Sets in Python don't guarantee preserved order, so we use a list to make testing easier.
                document_ids.append(doc_id)
            unique_document_ids.add(doc_id)
        return document_ids

    def _get_all_chunks_for_document(self, id: str) -> List[Document]:
        id_filter_condition = FilterCondition(
            f"{self.metadata_column_name}->>'{self.doc_id_key}'",
            FilterOperator.EQUAL,
            id
        )
        all_source_chunks = self.vector_store_handler.select(
            self.table_name,
            columns=[self.content_column_name],
            conditions=[id_filter_condition]
        )
        document_chunks = []
        for _, row in all_source_chunks.iterrows():
            document_chunks.append(Document(page_content=row[self.content_column_name]))
        return document_chunks

    def _call(
        self,
        inputs: Dict[str, Any],
        run_manager: Optional[CallbackManagerForChainRun] = None
    ) -> Dict[str, Any]:
        context_chunks = inputs.get(self.context_key, [])
        unique_document_ids = self._get_document_ids_from_chunks(context_chunks)

        # For each document ID associated with one or more chunks, build the full document by
        # geting ALL chunks associated with that ID. Then, map reduce summarize the complete document.
        source_id_to_summary = {}
        for source_id in unique_document_ids:
            document_chunks = self._get_all_chunks_for_document(source_id)
            source_id_to_summary[source_id] = self.map_reduce_documents_chain.run(document_chunks)

        # Update context chunks with document summaries.
        for chunk in context_chunks:
            doc_id = chunk.metadata.get(self.doc_id_key, '')
            summary = source_id_to_summary[str(doc_id)]
            chunk.metadata['summary'] = summary

        return inputs
