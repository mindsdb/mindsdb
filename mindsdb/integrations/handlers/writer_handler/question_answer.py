from collections import defaultdict

from langchain.llms import Writer

from mindsdb.integrations.handlers.writer_handler.settings import (
    VectorStoreConfig,
    VectorStoreLoader,
    WriterHandlerParameters,
    WriterLLMParameters,
)
from mindsdb.utilities.log import get_log

logger = get_log(logger_name=__name__)


class QuestionAnswerer:
    def __init__(self, args: WriterHandlerParameters):

        self.output_data = defaultdict(list)

        self.embeddings_model_name = args.embeddings_model_name

        self.persist_directory = args.vector_store_storage_path

        self.collection_or_index_name = args.collection_or_index_name

        config = VectorStoreConfig(
            embeddings_model_name=self.embeddings_model_name,
            persist_directory=self.persist_directory,
            collection_or_index_name=self.collection_or_index_name,
        )

        self.vector_store_loader = VectorStoreLoader(config)

        self.vector_store_name = args.vector_store_name

        self.vector_store = self.vector_store_loader.load_vector_store(
            self.vector_store_name
        )

        self.prompt_template = args.prompt_template

        self.llm = Writer(**args.llm_params.dict())

    def _prepare_prompt(self, vector_store_response, question):

        # todo ensure contexts don't exceed max length
        # todo maybe use a selector model, summarizer or otherwise truncate contexts
        context = [doc.page_content for doc in vector_store_response]

        combined_context = "\n\n".join(context)

        return self.prompt_template.format(question=question, context=combined_context)

    def query(self, question: str):
        logger.debug(f"Querying: {question}")

        vector_store_response = self.vector_store.similarity_search(query=question)

        formatted_prompt = self._prepare_prompt(vector_store_response, question)

        llm_response = self.llm(prompt=formatted_prompt)

        result = defaultdict(list)

        result["question"].append(question)
        result["answer"].append(llm_response)

        sources = defaultdict(list)

        for idx, document in enumerate(vector_store_response):
            sources["sources_document"].append(document.metadata["source"])
            sources["column"].append(document.metadata.get("column"))
            sources["sources_row"].append(document.metadata.get("row"))
            sources["sources_content"].append(document.page_content)

        result["source_documents"].append(dict(sources))

        return result
