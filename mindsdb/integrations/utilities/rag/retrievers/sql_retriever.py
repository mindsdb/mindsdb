import json
from typing import List, Optional

from langchain.chains.llm import LLMChain
from langchain_core.callbacks.manager import CallbackManagerForRetrieverRun
from langchain_core.documents.base import Document
from langchain_core.embeddings import Embeddings
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.prompts import PromptTemplate
from langchain_core.retrievers import BaseRetriever

from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from mindsdb.integrations.libs.vectordatabase_handler import DistanceFunction, VectorStoreHandler
from mindsdb.integrations.utilities.rag.settings import LLMExample, MetadataSchema, SearchKwargs
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class SQLRetriever(BaseRetriever):
    '''Retriever that uses a LLM to generate pgvector queries to do similarity search with metadata filters.

    How it works:

    1. Use a LLM to rewrite the user input to something more suitable for retrieval. For example:
    "Show me documents containing how to finetune a LLM please" --> "how to finetune a LLM"

    2. Use a LLM to generate a pgvector query with metadata filters based on the user input. Provided
    metadata schemas & examples are used as additional context to generate the query.

    3. Use a LLM to double check the generated pgvector query is correct.

    4. Actually execute the query against our vector database to retrieve documents & return them.
    '''
    fallback_retriever: BaseRetriever
    vector_store_handler: VectorStoreHandler
    metadata_schemas: Optional[List[MetadataSchema]] = None
    examples: Optional[List[LLMExample]] = None

    embeddings_model: Embeddings
    rewrite_prompt_template: str
    retry_prompt_template: str
    num_retries: int
    sql_prompt_template: str
    query_checker_template: str
    embeddings_table: str
    source_table: str
    distance_function: DistanceFunction
    search_kwargs: SearchKwargs

    llm: BaseChatModel

    def _prepare_sql_prompt(self) -> PromptTemplate:
        base_prompt_template = PromptTemplate(
            input_variables=['dialect', 'input', 'embeddings_table', 'source_table', 'embeddings', 'distance_function', 'schema', 'examples'],
            template=self.sql_prompt_template
        )
        schema_prompt_str = ''
        if self.metadata_schemas is not None:
            for i, schema in enumerate(self.metadata_schemas):
                column_mapping = {}
                for column in schema.columns:
                    column_mapping[column.name] = {
                        'type': column.type,
                        'description': column.description,
                    }
                    if column.values is not None:
                        column_mapping[column.name]['values'] = column.values
                column_mapping_json_str = json.dumps(column_mapping, indent=4)
                schema_str = f'''{i+2}. {schema.table} - {schema.description}

Columns:
```json
{column_mapping_json_str}
```

'''
                schema_prompt_str += schema_str

        examples_prompt_str = ''
        if self.examples is not None:
            for i, example in enumerate(self.examples):
                example_str = f'''{i + 1}. User input: "{example.input}"

Output:
{example.output}

'''
            examples_prompt_str += example_str
        return base_prompt_template.partial(
            schema=schema_prompt_str,
            examples=examples_prompt_str
        )

    def _prepare_retrieval_query(self, query: str) -> str:
        rewrite_prompt = PromptTemplate(
            input_variables=['input'],
            template=self.rewrite_prompt_template
        )
        rewrite_chain = LLMChain(llm=self.llm, prompt=rewrite_prompt)
        return rewrite_chain.predict(input=query)

    def _prepare_pgvector_query(self, query: str, run_manager: CallbackManagerForRetrieverRun) -> str:
        # Incorporate metadata schemas & examples into prompt.
        sql_prompt = self._prepare_sql_prompt()
        sql_chain = LLMChain(llm=self.llm, prompt=sql_prompt)
        # Generate the initial pgvector query.
        sql_query = sql_chain.predict(
            # Only pgvector & similarity search is supported.
            dialect='postgres',
            input=query,
            embeddings_table=self.embeddings_table,
            source_table=self.source_table,
            distance_function=self.distance_function.value[0],
            k=self.search_kwargs.k,
            callbacks=run_manager.get_child() if run_manager else None
        )
        query_checker_prompt = PromptTemplate(
            input_variables=['dialect', 'query'],
            template=self.query_checker_template
        )
        query_checker_chain = LLMChain(llm=self.llm, prompt=query_checker_prompt)
        # Check the query & return the final result to be executed.
        return query_checker_chain.predict(
            dialect='postgres',
            query=sql_query
        )

    def _prepare_retry_query(self, query: str, error: str, run_manager: CallbackManagerForRetrieverRun) -> str:
        sql_prompt = self._prepare_sql_prompt()
        # Use provided schema as context for retrying failed queries.
        schema = sql_prompt.partial_variables.get('schema', '')
        retry_prompt = PromptTemplate(
            input_variables=['query', 'dialect', 'error', 'embeddings_table', 'schema'],
            template=self.retry_prompt_template
        )
        retry_chain = LLMChain(llm=self.llm, prompt=retry_prompt)
        # Generate rewritten query.
        sql_query = retry_chain.predict(
            query=query,
            dialect='postgres',
            error=error,
            embeddings_table=self.embeddings_table,
            schema=schema,
            callbacks=run_manager.get_child() if run_manager else None
        )
        query_checker_prompt = PromptTemplate(
            input_variables=['dialect', 'query'],
            template=self.query_checker_template
        )
        query_checker_chain = LLMChain(llm=self.llm, prompt=query_checker_prompt)
        # Check the query & return the final result to be executed.
        return query_checker_chain.predict(
            dialect='postgres',
            query=sql_query
        )

    def _get_relevant_documents(
        self, query: str, *, run_manager: CallbackManagerForRetrieverRun
    ) -> List[Document]:
        # Rewrite query to be suitable for retrieval.
        retrieval_query = self._prepare_retrieval_query(query)

        # Generate & check the query to be executed
        checked_sql_query = self._prepare_pgvector_query(query, run_manager)

        # Embed the rewritten retrieval query & include it in the similarity search pgvector query.
        embedded_query = self.embeddings_model.embed_query(retrieval_query)
        checked_sql_query_with_embeddings = checked_sql_query.format(embeddings=str(embedded_query))
        # Handle LLM output that has the ```sql delimiter possibly.
        checked_sql_query_with_embeddings = checked_sql_query_with_embeddings.replace('```sql', '')
        checked_sql_query_with_embeddings = checked_sql_query_with_embeddings.replace('```', '')
        # Actually execute the similarity search with metadata filters.
        document_response = self.vector_store_handler.native_query(checked_sql_query_with_embeddings)
        num_retries = 0
        while num_retries < self.num_retries:
            if document_response.resp_type == RESPONSE_TYPE.ERROR:
                error_msg = document_response.error_message
                # LLMs won't always generate a working SQL query so we should have a fallback after retrying.
                logger.info(f'SQL Retriever query {checked_sql_query} failed with error {error_msg}')
                checked_sql_query = self._prepare_retry_query(checked_sql_query, error_msg, run_manager)
            elif len(document_response.data_frame) == 0:
                error_msg = "No documents retrieved from query."
                checked_sql_query = self._prepare_retry_query(checked_sql_query, error_msg, run_manager)
            else:
                break

            checked_sql_query_with_embeddings = checked_sql_query.format(embeddings=str(embedded_query))
            # Handle LLM output that has the ```sql delimiter possibly.
            checked_sql_query_with_embeddings = checked_sql_query_with_embeddings.replace('```sql', '')
            checked_sql_query_with_embeddings = checked_sql_query_with_embeddings.replace('```', '')
            document_response = self.vector_store_handler.native_query(checked_sql_query_with_embeddings)

            num_retries += 1
            if num_retries >= self.num_retries:
                logger.info('Using fallback retriever in SQL retriever.')
                return self.fallback_retriever._get_relevant_documents(retrieval_query, run_manager=run_manager)

        document_df = document_response.data_frame
        retrieved_documents = []
        for _, document_row in document_df.iterrows():
            retrieved_documents.append(Document(
                document_row.get('content', ''),
                metadata=document_row.get('metadata', {})
            ))
        if retrieved_documents:
            return retrieved_documents
        # If the SQL query constructed did not return any documents, fallback.
        logger.info('No documents returned from SQL retriever. using fallback retriever.')
        return self.fallback_retriever._get_relevant_documents(retrieval_query, run_manager=run_manager)
