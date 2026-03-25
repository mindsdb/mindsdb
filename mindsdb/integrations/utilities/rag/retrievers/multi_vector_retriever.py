from typing import List, Tuple, Any
import uuid
import asyncio

from mindsdb.integrations.utilities.rag.retrievers.base import BaseRetriever, RunnableRetriever
from mindsdb.integrations.utilities.rag.settings import DEFAULT_LLM_MODEL, MultiVectorRetrieverMode, RAGPipelineModel
from mindsdb.integrations.utilities.rag.vector_store import VectorStoreOperator
from mindsdb.integrations.utilities.rag.retrievers.safe_output_parser import SafeOutputParser
from mindsdb.interfaces.knowledge_base.preprocessing.document_types import SimpleDocument
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class MultiVectorRetriever(BaseRetriever):
    """
    MultiVectorRetriever stores multiple vectors per document.
    """

    def __init__(self, config: RAGPipelineModel):
        self.vectorstore = config.vector_store
        self.parent_store = config.parent_store
        self.id_key = config.id_key
        self.documents = config.documents
        self.text_splitter = config.text_splitter
        self.embedding_model = config.embedding_model
        self.max_concurrency = config.max_concurrency
        self.mode = config.multi_retriever_mode

    def _generate_id_and_split_document(self, doc: Any) -> Tuple[str, List[Any]]:
        """
        Generate a unique id for the document and split it into sub-documents.
        :param doc: Document with page_content and metadata
        :return: Tuple of (doc_id, list of sub_documents)
        """
        doc_id = str(uuid.uuid4())
        sub_docs = self.text_splitter.split_documents([doc])
        for sub_doc in sub_docs:
            # Use duck typing to access metadata
            if not hasattr(sub_doc, "metadata"):
                sub_doc.metadata = {}
            sub_doc.metadata[self.id_key] = doc_id
        return doc_id, sub_docs

    def _split_documents(self) -> Tuple[List[Any], List[str]]:
        """
        Split the documents into sub-documents and generate unique ids for each document.
        :return: Tuple of (list of split_docs, list of doc_ids)
        """
        split_info = list(map(self._generate_id_and_split_document, self.documents))
        doc_ids, split_docs_lists = zip(*split_info)
        split_docs = [doc for sublist in split_docs_lists for doc in sublist]
        return split_docs, list(doc_ids)

    def _create_retriever_and_vs_operator(
        self, docs: List[Any]
    ) -> Tuple["CustomMultiVectorRetriever", VectorStoreOperator]:
        vstore_operator = VectorStoreOperator(
            vector_store=self.vectorstore,
            documents=docs,
            embedding_model=self.embedding_model,
        )
        retriever = CustomMultiVectorRetriever(
            vectorstore=vstore_operator.vector_store, byte_store=self.parent_store, id_key=self.id_key
        )
        return retriever, vstore_operator

    def _get_document_summaries(self, llm: Any) -> List[str]:
        """
        Get document summaries using LLM

        Args:
            llm: LLM instance with invoke method

        Returns:
            List of summary strings
        """
        summaries = []
        prompt_template = "Summarize the following document:\n\n{doc}"

        for doc in self.documents:
            # Extract page_content using duck typing
            page_content = doc.page_content if hasattr(doc, "page_content") else str(doc)
            prompt = prompt_template.format(doc=page_content)

            try:
                # Call LLM
                llm_response = llm.invoke(prompt)
                # Extract content from LLM response
                if hasattr(llm_response, "content"):
                    summary = llm_response.content
                elif isinstance(llm_response, str):
                    summary = llm_response
                else:
                    summary = str(llm_response)

                # Use SafeOutputParser to clean the output (extract actual text from parse result)
                parser = SafeOutputParser()
                parsed_result = parser.parse(summary)
                summary = parser.extract_output(parsed_result)
                summaries.append(summary)
            except Exception as e:
                logger.warning(f"Error generating summary for document: {e}")
                # Fallback to empty summary or first part of content
                summaries.append(page_content[:200] if len(page_content) > 200 else page_content)

        return summaries

    def as_runnable(self) -> RunnableRetriever:
        # Get LLM from config - need to check how it's passed
        # For now, assume we need to get it from somewhere
        # This might need to be passed in config
        llm = getattr(self, "llm", None)
        if llm is None:
            # Try to create a default LLM - this might need adjustment
            from mindsdb.interfaces.knowledge_base.llm_wrapper import create_chat_model

            llm = create_chat_model({"model_name": DEFAULT_LLM_MODEL, "provider": "openai"})

        if self.mode in {MultiVectorRetrieverMode.SPLIT, MultiVectorRetrieverMode.BOTH}:
            split_docs, doc_ids = self._split_documents()
            retriever, vstore_operator = self._create_retriever_and_vs_operator(split_docs)
            summaries = self._get_document_summaries(llm)
            summary_docs = [
                SimpleDocument(page_content=s, metadata={self.id_key: doc_ids[i]}) for i, s in enumerate(summaries)
            ]
            vstore_operator.add_documents(summary_docs)
            retriever.docstore.mset(list(zip(doc_ids, self.documents)))
            return retriever

        elif self.mode == MultiVectorRetrieverMode.SUMMARIZE:
            summaries = self._get_document_summaries(llm)
            doc_ids = [str(uuid.uuid4()) for _ in self.documents]
            summary_docs = [
                SimpleDocument(page_content=s, metadata={self.id_key: doc_ids[i]}) for i, s in enumerate(summaries)
            ]
            retriever, vstore_operator = self._create_retriever_and_vs_operator(summary_docs)
            retriever.docstore.mset(list(zip(doc_ids, self.documents)))
            return retriever

        else:
            raise ValueError(f"Invalid mode: {self.mode}")


class CustomMultiVectorRetriever:
    """
    Custom implementation of MultiVectorRetriever to replace langchain's MultiVectorRetriever.
    Stores parent documents in docstore and sub-documents/summaries in vectorstore.
    """

    def __init__(self, vectorstore: Any, byte_store: Any, id_key: str = "doc_id"):
        """
        Initialize CustomMultiVectorRetriever

        Args:
            vectorstore: Vector store for storing sub-documents/summaries
            byte_store: Store for parent documents (must have mset and mget methods)
            id_key: Key used to link sub-documents to parent documents
        """
        self.vectorstore = vectorstore
        self.docstore = byte_store
        self.id_key = id_key

    def invoke(self, query: str) -> List[Any]:
        """Sync invocation - retrieve documents for a query"""
        # Get sub-documents from vectorstore
        sub_docs = self.vectorstore.similarity_search(query, k=4)

        # Get parent document IDs from sub-documents
        parent_ids = []
        for doc in sub_docs:
            metadata = getattr(doc, "metadata", {})
            if self.id_key in metadata:
                parent_ids.append(metadata[self.id_key])

        # Get parent documents from docstore
        parent_docs = []
        if parent_ids and hasattr(self.docstore, "mget"):
            parent_docs = self.docstore.mget(parent_ids)
        elif parent_ids and hasattr(self.docstore, "get"):
            parent_docs = [self.docstore.get(pid) for pid in parent_ids if self.docstore.get(pid) is not None]

        # Return parent documents (or sub-docs if no parent store)
        return parent_docs if parent_docs else sub_docs

    async def ainvoke(self, query: str) -> List[Any]:
        """Async invocation - retrieve documents for a query"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.invoke, query)

    def get_relevant_documents(self, query: str) -> List[Any]:
        """Get relevant documents (sync)"""
        return self.invoke(query)
