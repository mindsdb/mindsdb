import asyncio
from collections import namedtuple
from typing import Any, Dict, List, Optional

from mindsdb.interfaces.agents.langchain_agent import create_chat_model
from langchain.chains.base import Chain
from langchain.chains.combine_documents.stuff import StuffDocumentsChain
from langchain.chains.llm import LLMChain
from langchain.chains.combine_documents.map_reduce import MapReduceDocumentsChain, ReduceDocumentsChain
from langchain_core.callbacks import dispatch_custom_event
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


def create_map_reduce_documents_chain(summarization_config: SummarizationConfig, input: str) -> ReduceDocumentsChain:
    """Creates a chain that map-reduces documents into a single consolidated summary."""
    summarization_llm = create_chat_model({
        'model_name': summarization_config.llm_config.model_name,
        'provider': summarization_config.llm_config.provider,
        **summarization_config.llm_config.params
    })

    reduce_prompt_template = summarization_config.reduce_prompt_template
    reduce_prompt = PromptTemplate.from_template(reduce_prompt_template)
    if 'input' in reduce_prompt.input_variables:
        reduce_prompt = reduce_prompt.partial(input=input)

    reduce_chain = LLMChain(llm=summarization_llm, prompt=reduce_prompt)

    combine_documents_chain = StuffDocumentsChain(
        llm_chain=reduce_chain,
        document_variable_name='docs'
    )

    return ReduceDocumentsChain(
        combine_documents_chain=combine_documents_chain,
        collapse_documents_chain=combine_documents_chain,
        token_max=summarization_config.max_summarization_tokens
    )


class LocalContextSummarizerChain(Chain):
    """Summarizes M chunks before and after a given chunk in a document."""

    doc_id_key: str = 'original_row_id'
    chunk_index_key: str = 'chunk_index'

    vector_store_handler: VectorStoreHandler
    table_name: str = 'embeddings'
    content_column_name: str = 'content'
    metadata_column_name: str = 'metadata'

    summarization_config: SummarizationConfig
    map_reduce_documents_chain: Optional[ReduceDocumentsChain] = None

    def _select_chunks_from_vector_store(self, doc_id: str) -> DataFrame:
        condition = FilterCondition(
            f"{self.metadata_column_name}->>'{self.doc_id_key}'",
            FilterOperator.EQUAL,
            doc_id
        )
        return self.vector_store_handler.select(
            self.table_name,
            columns=[self.content_column_name, self.metadata_column_name],
            conditions=[condition]
        )

    async def _get_all_chunks_for_document(self, doc_id: str) -> List[Document]:
        df = await asyncio.get_event_loop().run_in_executor(
            None, self._select_chunks_from_vector_store, doc_id
        )
        chunks = []
        for _, row in df.iterrows():
            metadata = row.get(self.metadata_column_name, {})
            metadata[self.chunk_index_key] = row.get('chunk_id', 0)
            chunks.append(Document(page_content=row[self.content_column_name], metadata=metadata))

        return sorted(chunks, key=lambda x: x.metadata.get(self.chunk_index_key, 0))

    async def summarize_local_context(self, doc_id: str, target_chunk_index: int, M: int) -> Summary:
        """
        Summarizes M chunks before and after the given chunk.

        Args:
            doc_id (str): Document ID.
            target_chunk_index (int): Index of the chunk to summarize around.
            M (int): Number of chunks before and after to include.

        Returns:
            Summary: Summary object containing source_id and summary content.
        """
        logger.debug(f"Fetching chunks for document {doc_id}")
        all_chunks = await self._get_all_chunks_for_document(doc_id)

        if not all_chunks:
            logger.warning(f"No chunks found for document {doc_id}")
            return Summary(source_id=doc_id, content='')

        # Determine window boundaries
        start_idx = max(0, target_chunk_index - M)
        end_idx = min(len(all_chunks), target_chunk_index + M + 1)
        local_chunks = all_chunks[start_idx:end_idx]

        logger.debug(f"Summarizing chunks {start_idx} to {end_idx - 1} for document {doc_id}")

        if not self.map_reduce_documents_chain:
            self.map_reduce_documents_chain = create_map_reduce_documents_chain(
                self.summarization_config, input="Summarize these chunks."
            )

        summary_result = await self.map_reduce_documents_chain.ainvoke(local_chunks)
        summary_text = summary_result.get('output_text', '')

        logger.debug(f"Generated summary: {summary_text[:100]}...")

        return Summary(source_id=doc_id, content=summary_text)

    @property
    def input_keys(self) -> List[str]:
        return [self.context_key, self.question_key]

    @property
    def output_keys(self) -> List[str]:
        return [self.context_key, self.question_key]

    async def _get_source_summary(self, source_id: str, map_reduce_documents_chain: MapReduceDocumentsChain) -> Summary:
        if not source_id:
            logger.warning("Received empty source_id, returning empty summary")
            return Summary(source_id='', content='')

        logger.debug(f"Getting summary for source ID: {source_id}")
        source_chunks = await self._get_all_chunks_for_document(source_id)

        if not source_chunks:
            logger.warning(f"No chunks found for source ID: {source_id}")
            return Summary(source_id=source_id, content='')

        logger.debug(f"Summarizing {len(source_chunks)} chunks for source ID: {source_id}")
        summary = await map_reduce_documents_chain.ainvoke(source_chunks)
        content = summary.get('output_text', '')
        logger.debug(f"Generated summary for source ID {source_id}: {content[:100]}...")

        # Stream summarization update.
        dispatch_custom_event('summary', {'source_id': source_id, 'content': content})

        return Summary(source_id=source_id, content=content)

    async def _get_source_summaries(self, source_ids: List[str], map_reduce_documents_chain: MapReduceDocumentsChain) -> \
            List[Summary]:
        summaries = await asyncio.gather(
            *[self._get_source_summary(source_id, map_reduce_documents_chain) for source_id in source_ids]
        )
        return summaries

    def _call(
            self,
            inputs: Dict[str, Any],
            run_manager: Optional[CallbackManagerForChainRun] = None
    ) -> Dict[str, Any]:
        # Step 1: Connect to vector store to ensure embeddings are accessible
        self.vector_store_handler.connect()

        context_chunks: List[Document] = inputs.get(self.context_key, [])
        logger.debug(f"Found {len(context_chunks)} context chunks.")

        # Step 2: Extract unique document IDs from the provided chunks
        unique_document_ids = self._get_document_ids_from_chunks(context_chunks)
        logger.debug(f"Extracted {len(unique_document_ids)} unique document IDs: {unique_document_ids}")

        # Step 3: Initialize the summarization chain if not provided
        question = inputs.get(self.question_key, '')
        map_reduce_documents_chain = self.map_reduce_documents_chain or create_map_reduce_documents_chain(
            self.summarization_config, question
        )

        # Step 4: Dispatch event to signal summarization start
        if run_manager:
            run_manager.on_text("Starting summarization for documents.", verbose=True)

        # Step 5: Process each document ID to summarize chunks with local context
        for doc_id in unique_document_ids:
            logger.debug(f"Fetching and summarizing chunks for document ID: {doc_id}")

            # Fetch all chunks for the document
            chunks = asyncio.get_event_loop().run_until_complete(self._get_all_chunks_for_document(doc_id))
            if not chunks:
                logger.warning(f"No chunks found for document ID: {doc_id}")
                continue

            # Summarize each chunk with M neighboring chunks
            M = self.neighbor_window
            for i, chunk in enumerate(chunks):
                window_chunks = chunks[max(0, i - M): min(len(chunks), i + M + 1)]
                local_summary = asyncio.get_event_loop().run_until_complete(
                    map_reduce_documents_chain.ainvoke(window_chunks)
                )
                chunk.metadata['summary'] = local_summary.get('output_text', '')
                logger.debug(f"Chunk {i} summary: {chunk.metadata['summary'][:100]}...")

        # Step 6: Update the original context chunks with the newly generated summaries
        for chunk in context_chunks:
            doc_id = str(chunk.metadata.get(self.doc_id_key, ''))
            matching_chunk = next((c for c in chunks if c.metadata.get(self.doc_id_key) == doc_id and c.metadata.get(
                'chunk_index') == chunk.metadata.get('chunk_index')), None)
            if matching_chunk:
                chunk.metadata['summary'] = matching_chunk.metadata.get('summary', '')
            else:
                chunk.metadata['summary'] = ''
                logger.warning(f"No matching chunk found for doc_id: {doc_id}")

        # Step 7: Signal summarization end
        if run_manager:
            run_manager.on_text("Summarization completed.", verbose=True)

        logger.debug(f"Updated {len(context_chunks)} context chunks with summaries.")
        return inputs
