import asyncio
from collections import namedtuple
from typing import Any, Dict, List, Optional

from mindsdb.interfaces.agents.langchain_agent import create_chat_model
from langchain.chains.base import Chain
from langchain.chains.combine_documents.stuff import StuffDocumentsChain
from langchain.chains.llm import LLMChain
from langchain.chains.combine_documents.map_reduce import MapReduceDocumentsChain, ReduceDocumentsChain
from langchain_core.callbacks.manager import CallbackManagerForChainRun
from langchain_core.documents import Document
from langchain_core.prompts import PromptTemplate
from pandas import DataFrame

from mindsdb.integrations.libs.vectordatabase_handler import VectorStoreHandler
from mindsdb.integrations.utilities.rag.settings import SummarizationConfig
from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator
from mindsdb.utilities import log

logger = log.getLogger(__name__)


Summary = namedtuple('Summary', ['source_id', 'content'])


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
        logger.debug(f"Processing {len(chunks)} chunks to extract document IDs")
        for chunk in chunks:
            if not chunk.metadata:
                chunk.metadata = {}
                logger.warning("Chunk metadata was empty, creating new metadata dictionary")
            metadata = chunk.metadata
            doc_id = str(metadata.get(self.doc_id_key, ''))
            logger.debug(f"Processing chunk with metadata: {metadata}, extracted doc_id: {doc_id}")
            if doc_id and doc_id not in unique_document_ids:
                # Sets in Python don't guarantee preserved order, so we use a list to make testing easier.
                document_ids.append(doc_id)
                unique_document_ids.add(doc_id)
        logger.debug(f"Found {len(document_ids)} unique document IDs: {document_ids}")
        return document_ids

    def _select_chunks_from_vector_store(self, conditions: List[FilterCondition]) -> DataFrame:
        return self.vector_store_handler.select(
            self.table_name,
            columns=[self.content_column_name],
            conditions=conditions
        )

    async def _get_all_chunks_for_document(self, id: str) -> List[Document]:
        logger.debug(f"Fetching all chunks for document ID: {id}")
        id_filter_condition = FilterCondition(
            f"{self.metadata_column_name}->>'{self.doc_id_key}'",
            FilterOperator.EQUAL,
            id
        )
        all_source_chunks = await asyncio.get_event_loop().run_in_executor(None, self._select_chunks_from_vector_store, [id_filter_condition])
        document_chunks = []
        for _, row in all_source_chunks.iterrows():
            document_chunks.append(Document(page_content=row[self.content_column_name]))
        logger.debug(f"Found {len(document_chunks)} chunks for document ID {id}")
        return document_chunks

    async def _get_source_summary(self, source_id: str) -> Summary:
        if not source_id:
            logger.warning("Received empty source_id, returning empty summary")
            return Summary(source_id='', content='')

        logger.debug(f"Getting summary for source ID: {source_id}")
        source_chunks = await self._get_all_chunks_for_document(source_id)

        if not source_chunks:
            logger.warning(f"No chunks found for source ID: {source_id}")
            return Summary(source_id=source_id, content='')

        logger.debug(f"Summarizing {len(source_chunks)} chunks for source ID: {source_id}")
        summary = await self.map_reduce_documents_chain.ainvoke(source_chunks)
        content = summary.get('output_text', '')
        logger.debug(f"Generated summary for source ID {source_id}: {content[:100]}...")
        return Summary(source_id=source_id, content=content)

    async def _get_source_summaries(self, source_ids: List[str]) -> List[Summary]:
        summaries = await asyncio.gather(
            *[self._get_source_summary(source_id) for source_id in source_ids]
        )
        return summaries

    def _call(
        self,
        inputs: Dict[str, Any],
        run_manager: Optional[CallbackManagerForChainRun] = None
    ) -> Dict[str, Any]:
        logger.debug(f"Processing inputs with keys: {list(inputs.keys())}")
        context_chunks = inputs.get(self.context_key, [])
        logger.debug(f"Found {len(context_chunks)} context chunks")

        unique_document_ids = self._get_document_ids_from_chunks(context_chunks)
        logger.debug(f"Extracted {len(unique_document_ids)} unique document IDs")

        # For each document ID associated with one or more chunks, build the full document by
        # getting ALL chunks associated with that ID. Then, map reduce summarize the complete document.
        try:
            logger.debug("Starting async summary generation")
            summaries = asyncio.get_event_loop().run_until_complete(self._get_source_summaries(unique_document_ids))
        except RuntimeError:
            logger.info("No event loop available, creating new one")
            # If no event loop is available, create a new one
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            summaries = loop.run_until_complete(self._get_source_summaries(unique_document_ids))

        source_id_to_summary = {}
        for summary in summaries:
            source_id_to_summary[summary.source_id] = summary.content
        logger.debug(f"Generated {len(source_id_to_summary)} summaries")

        # Update context chunks with document summaries.
        for chunk in context_chunks:
            if not chunk.metadata:
                chunk.metadata = {}
                logger.warning("Chunk metadata was empty, creating new metadata dictionary")

            metadata = chunk.metadata
            doc_id = str(metadata.get(self.doc_id_key, ''))
            logger.debug(f"Updating chunk with doc_id {doc_id}")
            if doc_id in source_id_to_summary:
                chunk.metadata['summary'] = source_id_to_summary[doc_id]
            else:
                logger.warning(f"No summary found for doc_id: {doc_id}")
                chunk.metadata['summary'] = ''

        return inputs
