from typing import List

from langchain.text_splitter import TextSplitter
from langchain_core.language_models import BaseChatModel
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.retrievers import BaseRetriever
from langchain_core.vectorstores import VectorStore

from mindsdb.integrations.utilities.rag.retrievers.auto_retriever import AutoRetriever
from mindsdb.integrations.utilities.rag.retrievers.multi_vector_retriever import MultiVectorRetriever, MultiVectorRetrieverMode
from mindsdb.integrations.utilities.rag.retrievers.sql_retriever import SQLRetriever
from langchain_core.runnables import RunnableParallel, RunnablePassthrough, RunnableSerializable
from langchain.docstore.document import Document

from mindsdb.integrations.utilities.rag.settings import DEFAULT_LLM, DEFAULT_SQL_RETRIEVAL_PROMPT_TEMPLATE, DEFAULT_AUTO_META_PROMPT_TEMPLATE


class LangChainRAGPipeline:
    """
    Builds a RAG pipeline using langchain LCEL components
    """

    def __init__(self, retriever_runnable, prompt_template, llm=DEFAULT_LLM):

        self.retriever_runnable = retriever_runnable
        self.prompt_template = prompt_template
        self.llm = llm

    def with_returned_sources(self) -> RunnableSerializable:
        """
        Builds a RAG pipeline with returned sources
        :return:
        """

        def format_docs(docs):
            if isinstance(docs, str):
                # this is to handle the case where the retriever returns a string
                # instead of a list of documents e.g. SQLRetriever
                return docs
            return "\n\n".join(doc.page_content for doc in docs)

        prompt = ChatPromptTemplate.from_template(self.prompt_template)

        rag_chain_from_docs = (
                RunnablePassthrough.assign(context=(lambda x: format_docs(x["context"])))  # noqa: E126, E122
                | prompt
                | self.llm
                | StrOutputParser()
        )

        rag_chain_with_source = RunnableParallel(
            {"context": self.retriever_runnable, "question": RunnablePassthrough()}
        ).assign(answer=rag_chain_from_docs)

        return rag_chain_with_source

    @classmethod
    def from_retriever(cls, retriever: BaseRetriever, prompt_template: str, llm: BaseChatModel):
        """
        Builds a RAG pipeline with returned sources using a BaseRetriever
        :param retriever: BaseRetriever
        :param prompt_template: str
        :param llm: BaseChatModel

        :return:
        """
        return cls(retriever, prompt_template, llm)

    @classmethod
    def from_sql_retriever(cls,
                           connection_string,
                           retriever_prompt_template: dict,
                           rag_prompt_template,
                           llm: BaseChatModel = None
                           ):
        """
        Builds a RAG pipeline with returned sources using a SQLRetriever

        :param connection_string: str
        :param retriever_prompt_template: dict
        :param rag_prompt_template: str
        :param llm: BaseChatModel

        :return:
        """
        retriever_prompt_template = retriever_prompt_template or DEFAULT_SQL_RETRIEVAL_PROMPT_TEMPLATE

        retriever_runnable = SQLRetriever(
            connection_string=connection_string,
            prompt_template=retriever_prompt_template
        ).as_runnable()

        return cls(retriever_runnable, rag_prompt_template, llm)

    @classmethod
    def from_auto_retriever(cls,
                            retriever_prompt_template: str,
                            rag_prompt_template: str,
                            data_description: str,
                            content_column_name: str,
                            data: List[Document],
                            vectorstore: VectorStore = None,
                            llm: BaseChatModel = None
                            ):
        """
        Builds a RAG pipeline with returned sources using a AutoRetriever

        NB specify either data or vectorstore, not both


        :param retriever_prompt_template: str
        :param rag_prompt_template: str
        :param data_description: str
        :param content_column_name: str
        :param data: List[Document]
        :param vectorstore: VectorStore
        :param llm: BaseChatModel

        :return:
        """
        retriever_prompt_template = retriever_prompt_template or DEFAULT_AUTO_META_PROMPT_TEMPLATE

        retriever_runnable = AutoRetriever(data=data, content_column_name=content_column_name, vectorstore=vectorstore,
                                           document_description=data_description,
                                           prompt_template=retriever_prompt_template).as_runnable()
        return cls(retriever_runnable, rag_prompt_template, llm)

    @classmethod
    def from_multi_vector_retriever(
        cls,
        documents: List[Document],
        rag_prompt_template: str,
        vectorstore: VectorStore = None,
        text_splitter: TextSplitter = None,
        llm: BaseChatModel = None,
        mode: MultiVectorRetrieverMode = MultiVectorRetrieverMode.BOTH,
    ):
        retriever_runnable = MultiVectorRetriever(
            documents=documents, vectorstore=vectorstore, text_splitter=text_splitter, mode=mode).as_runnable()
        return cls(retriever_runnable, rag_prompt_template, llm)
