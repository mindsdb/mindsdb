from typing import List, Tuple
import uuid

from langchain.retrievers.multi_vector import MultiVectorRetriever as LangChainMultiVectorRetriever
from langchain_core.documents import Document
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI

from mindsdb.integrations.utilities.rag.retrievers.base import BaseRetriever
from mindsdb.integrations.utilities.rag.settings import DEFAULT_LLM_MODEL, \
    MultiVectorRetrieverMode, RAGPipelineModel
from mindsdb.integrations.utilities.rag.vector_store import VectorStoreOperator
from mindsdb.interfaces.agents.safe_output_parser import SafeOutputParser


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

    def _generate_id_and_split_document(self, doc: Document) -> Tuple[str, List[Document]]:
        """
        Generate a unique id for the document and split it into sub-documents.
        :param doc:
        :return:
        """
        doc_id = str(uuid.uuid4())
        sub_docs = self.text_splitter.split_documents([doc])
        for sub_doc in sub_docs:
            sub_doc.metadata[self.id_key] = doc_id
        return doc_id, sub_docs

    def _split_documents(self) -> Tuple[List[Document], List[str]]:
        """
        Split the documents into sub-documents and generate unique ids for each document.
        :return:
        """
        split_info = list(map(self._generate_id_and_split_document, self.documents))
        doc_ids, split_docs_lists = zip(*split_info)
        split_docs = [doc for sublist in split_docs_lists for doc in sublist]
        return split_docs, list(doc_ids)

    def _create_retriever_and_vs_operator(self, docs: List[Document]) \
            -> Tuple[LangChainMultiVectorRetriever, VectorStoreOperator]:
        vstore_operator = VectorStoreOperator(
            vector_store=self.vectorstore,
            documents=docs,
            embedding_model=self.embedding_model,
        )
        retriever = LangChainMultiVectorRetriever(
            vectorstore=vstore_operator.vector_store,
            byte_store=self.parent_store,
            id_key=self.id_key
        )
        return retriever, vstore_operator

    def _get_document_summaries(self) -> List[str]:
        chain = (
                {"doc": lambda x: x.page_content}  # noqa: E126, E122
                | ChatPromptTemplate.from_template("Summarize the following document:\n\n{doc}")
                | ChatOpenAI(max_retries=0, model_name=DEFAULT_LLM_MODEL)
                | SafeOutputParser()
        )
        return chain.batch(self.documents, {"max_concurrency": self.max_concurrency})

    def as_runnable(self) -> BaseRetriever:
        if self.mode in {MultiVectorRetrieverMode.SPLIT, MultiVectorRetrieverMode.BOTH}:
            split_docs, doc_ids = self._split_documents()
            retriever, vstore_operator = self._create_retriever_and_vs_operator(split_docs)
            summaries = self._get_document_summaries()
            summary_docs = [
                Document(page_content=s, metadata={self.id_key: doc_ids[i]})
                for i, s in enumerate(summaries)
            ]
            vstore_operator.add_documents(summary_docs)
            retriever.docstore.mset(list(zip(doc_ids, self.documents)))
            return retriever

        elif self.mode == MultiVectorRetrieverMode.SUMMARIZE:
            summaries = self._get_document_summaries()
            doc_ids = [str(uuid.uuid4()) for _ in self.documents]
            summary_docs = [
                Document(page_content=s, metadata={self.id_key: doc_ids[i]})
                for i, s in enumerate(summaries)
            ]
            retriever, vstore_operator = self._create_retriever_and_vs_operator(summary_docs)
            retriever.docstore.mset(list(zip(doc_ids, self.documents)))
            return retriever

        else:
            raise ValueError(f"Invalid mode: {self.mode}")
