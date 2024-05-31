import json
from collections import defaultdict
from typing import List

from openai.types import Completion

from mindsdb.integrations.handlers.rag_handler.settings import (
    LLMLoader,
    PersistedVectorStoreLoader,
    PersistedVectorStoreLoaderConfig,
    RAGBaseParameters,
    RAGHandlerParameters,
    load_embeddings_model,
)
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class RAGQuestionAnswerer:
    """A class for using a RAG model for question answering"""

    def __init__(self, args: RAGBaseParameters):

        self.output_data = defaultdict(list)

        self.args = args

        self.embeddings_model = args.embeddings_model
        if self.embeddings_model is None:
            self.embeddings_model = load_embeddings_model(args.embeddings_model_name)

        self.persist_directory = args.vector_store_storage_path

        self.collection_name = args.collection_name

        vector_store_config = PersistedVectorStoreLoaderConfig(
            vector_store_name=args.vector_store_name,
            embeddings_model=self.embeddings_model,
            persist_directory=self.persist_directory,
            collection_name=self.collection_name,
        )

        self.vector_store_loader = PersistedVectorStoreLoader(vector_store_config)

        self.persisted_vector_store = self.vector_store_loader.load_vector_store()

        self.prompt_template = args.prompt_template

        if isinstance(args, RAGHandlerParameters):

            llm_config = {"llm_config": args.llm_params.model_dump()}

            llm_loader = LLMLoader(**llm_config)

            self.llm = llm_loader.load_llm()

    def __call__(self, question: str) -> defaultdict:
        return self.query(question)

    def _prepare_prompt(self, vector_store_response, question) -> str:

        context = [doc.page_content for doc in vector_store_response]

        combined_context = "\n\n".join(context)

        if self.args.summarize_context:
            return self.summarize_context(combined_context, question)

        return self.prompt_template.format(question=question, context=combined_context)

    def summarize_context(self, combined_context: str, question: str) -> str:

        summarization_prompt_template = self.args.summarization_prompt_template

        summarization_prompt = summarization_prompt_template.format(
            context=combined_context, question=question
        )

        summarized_context = self.llm(prompt=summarization_prompt)

        return self.prompt_template.format(
            question=question, context=self.extract_generated_text(summarized_context)
        )

    def query_vector_store(self, question: str) -> List:

        return self.persisted_vector_store.similarity_search(
            query=question,
            k=self.args.top_k,
        )

    @staticmethod
    def extract_generated_text(response: str) -> str:
        """Extract generated text from LLM response"""

        if isinstance(response, str):
            data = json.loads(response)
        else:
            data = response

        try:
            if "choices" in data:
                return data["choices"][0]["text"]
            elif isinstance(data, Completion):
                return data.choices[0].text
            else:
                logger.info(
                    f"Error extracting generated text: failed to parse response {response}"
                )
                return response

        except Exception as e:
            raise Exception(
                f"{e} Error extracting generated text: failed to parse response {response}"
            )

    def query(self, question: str) -> defaultdict:
        """Post process LLM response"""
        llm_response, vector_store_response = self._query(question)

        result = defaultdict(list)
        extracted_text = self.extract_generated_text(llm_response)
        result["answer"].append(extracted_text)

        sources = defaultdict(list)

        for idx, document in enumerate(vector_store_response):
            sources["sources_content"].append(document.page_content)
            sources["sources_document"].append(document.metadata.get("source", None))
            sources["column"].append(document.metadata.get("column", None))
            sources["sources_row"].append(document.metadata.get("row", None))

        result["source_documents"].append(dict(sources))

        return result

    def _query(self, question: str):
        logger.debug(f"Querying: {question}")

        vector_store_response = self.query_vector_store(question)

        formatted_prompt = self._prepare_prompt(vector_store_response, question)

        llm_response = self.llm(prompt=formatted_prompt)

        return llm_response, vector_store_response
