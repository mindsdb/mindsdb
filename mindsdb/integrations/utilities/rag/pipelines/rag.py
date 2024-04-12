from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableParallel, RunnablePassthrough, RunnableSerializable


from mindsdb.integrations.utilities.rag.retrievers.auto_retriever import AutoRetriever
from mindsdb.integrations.utilities.rag.retrievers.multi_vector_retriever import MultiVectorRetriever


from mindsdb.integrations.utilities.rag.settings import RAGPipelineModel, DEFAULT_AUTO_META_PROMPT_TEMPLATE
from mindsdb.integrations.utilities.rag.vector_store import VectorStoreOperator


class LangChainRAGPipeline:
    """
    Builds a RAG pipeline using langchain LCEL components
    """

    def __init__(self, retriever_runnable, prompt_template, llm):

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
    def from_retriever(cls, config: RAGPipelineModel):
        """
        Builds a RAG pipeline with returned sources using a simple vector store retriever
        :param config: RAGPipelineModel

        :return:
        """
        vector_store_operator = VectorStoreOperator(
            vector_store=config.vector_store,
            documents=config.documents,
            embeddings_model=config.embeddings_model,
            vector_store_config=config.vector_store_config
        )

        return cls(vector_store_operator.vector_store.as_retriever(), config.rag_prompt_template, config.llm)

    @classmethod
    def from_auto_retriever(cls, config: RAGPipelineModel):
        """
        Builds a RAG pipeline with returned sources using a AutoRetriever

        :param config: RAGPipelineModel

        :return:
        """

        if not config.retriever_prompt_template:
            config.retriever_prompt_template = DEFAULT_AUTO_META_PROMPT_TEMPLATE

        retriever_runnable = AutoRetriever(config=config).as_runnable()
        return cls(retriever_runnable, config.rag_prompt_template, config.llm)

    @classmethod
    def from_multi_vector_retriever(cls, config: RAGPipelineModel):
        """
        Builds a RAG pipeline with returned sources using a MultiVectorRetriever

        :param config: RAGPipelineModel
        """

        retriever_runnable = MultiVectorRetriever(config=config).as_runnable()
        return cls(retriever_runnable, config.rag_prompt_template, config.llm)
