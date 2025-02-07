import json
import re
from pydantic import BaseModel, Field
from typing import List, Any, Optional, Dict

from langchain.chains.llm import LLMChain
from langchain_core.callbacks.manager import CallbackManagerForRetrieverRun
from langchain_core.documents.base import Document
from langchain_core.embeddings import Embeddings
from langchain_core.exceptions import OutputParserException
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_core.retrievers import BaseRetriever

from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from mindsdb.integrations.libs.response import HandlerResponse
from mindsdb.integrations.libs.vectordatabase_handler import (
    DistanceFunction,
    VectorStoreHandler,
)
from mindsdb.integrations.utilities.rag.settings import (
    LLMExample,
    TableSchema,
    ColumnSchema,
    ValueSchema,
    SearchKwargs,
)
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class MetadataFilter(BaseModel):
    """Represents an LLM generated metadata filter to apply to a PostgreSQL query."""

    attribute: str = Field(description="Database column to apply filter to")
    comparator: str = Field(
        description="PostgreSQL comparator to use to filter database column"
    )
    value: Any = Field(description="Value to use to filter database column")


class MetadataFilters(BaseModel):
    """List of LLM generated metadata filters to apply to a PostgreSQL query."""

    filters: List[MetadataFilter] = Field(
        description="List of PostgreSQL metadata filters to apply for user query"
    )


class SQLRetriever(BaseRetriever):
    """Retriever that uses a LLM to generate pgvector queries to do similarity search with metadata filters.

    How it works:

    1. Use a LLM to rewrite the user input to something more suitable for retrieval. For example:
    "Show me documents containing how to finetune a LLM please" --> "how to finetune a LLM"

    2. Use a LLM to generate structured metadata filters based on the user input. Provided
    metadata schemas & examples are used as additional context.

    3. Generate a prepared PostgreSQL query from the structured metadata filters.

    4. Actually execute the query against our vector database to retrieve documents & return them.
    """

    fallback_retriever: BaseRetriever
    vector_store_handler: VectorStoreHandler
    metadata_schemas: Optional[List[TableSchema]] = None
    examples: Optional[List[LLMExample]] = None

    rewrite_prompt_template: str
    metadata_filters_prompt_template: str
    embeddings_model: Embeddings
    num_retries: int
    embeddings_table: str
    source_table: str
    source_id_column: str = "Id"
    distance_function: DistanceFunction
    search_kwargs: SearchKwargs

    llm: BaseChatModel

    def _prepare_value_prompt(self, value_schema: ValueSchema) -> PromptTemplate:
        base_prompt_template = PromptTemplate(
            input_variables=[
                "value",
                "description",
                "usage",
                "example_questions",
                "input",
            ],
            template=self.value_prompt_template,
        )

        example_str = """
## **Example Question**:
"""
        for example in value_schema.examples:
            example_str += f"""- {example}
"""

        return base_prompt_template.partial(
            value=value_schema.value,
            description=value_schema.description,
            usage=value_schema.usage,
            example_questions=example_str,
        )

    def _prepare_column_prompt(self, column_schema: ColumnSchema) -> PromptTemplate:
        base_prompt_template = PromptTemplate(
            input_variables=[
                "column",
                "type",
                "description",
                "usage",
                "values," "example_questions",
                "input",
            ],
            template=self.column_prompt_template,
        )

        if column_schema.model_fields["values"] == Dict[Any, ValueSchema]:
            value_str = """
## **Value Descriptions**

Below are descriptions of each value in this column:
"""
            for value_schema in column_schema.values.values():
                value_str += f"""
    **{value_schema.value}:** {value_schema.description}
"""
        elif column_schema.model_fields["values"] == Dict[Any, Any]:
            value_str = """
## **Enumerated Values**

These column values are an enumeration of named values. These are listed below with format **[Column Value]**: [named value].
"""
            for value, value_name in column_schema.values.items():
                value_str += f"""
**- {value}:** {value_name}"""
        elif column_schema.model_fields["values"] == List[Any]:
            value_str = f"""
## **Sample Values**

There are too many unique values in the column to list exhaustively. Below is a sampling of values found in the column:
{column_schema.model_fields['values']}
"""
        example_str = """
## **Example Question**:
"""
        for example in column_schema.examples:
            example_str += f"""- {example}
"""

        return base_prompt_template.partial(
            column=column_schema.column,
            type=column_schema.type,
            description=column_schema.description,
            usage=column_schema.usage,
            values=value_str,
            example_questions=example_str,
        )

    def _prepare_table_prompt(self) -> PromptTemplate:
        pass

    def _prepare_metadata_prompt(self) -> PromptTemplate:
        base_prompt_template = PromptTemplate(
            input_variables=[
                "format_instructions",
                "schema",
                "examples",
                "input",
                "embeddings",
            ],
            template=self.metadata_filters_prompt_template,
        )
        schema_prompt_str = ""
        if self.metadata_schemas is not None:
            for i, schema in enumerate(self.metadata_schemas):
                schema_str = f"""
                ** {i}. {schema.table} **
                ** Table Description: **
                {schema.description}

                **Columns:**"""

                for column in schema.columns:
                    schema_str += f"""

                    **{column.name} Column**
                    - **Type:** {column.type}
                    - **Description:** {column.description}
                    """
                    if column.values is not None:
                        schema_str += """
                    - **Values:**
                    """
                        for value in column.values.values():
                            schema_str += f"""{value}
                            """
                    if column.example_values is not None:
                        schema_str += f"""
                    - **Example Values:**
                        {json.dumps(column.example_values)}
                        """
                schema_prompt_str += schema_str

        examples_prompt_str = ""
        if self.examples is not None:
            for i, example in enumerate(self.examples):
                example_str = f"""{i + 1}. User input: "{example.input}"

Output:
{example.output}

"""
                examples_prompt_str += example_str
        return base_prompt_template.partial(
            schema=schema_prompt_str, examples=examples_prompt_str
        )

    def _prepare_retrieval_query(self, query: str) -> str:
        rewrite_prompt = PromptTemplate(
            input_variables=["input"], template=self.rewrite_prompt_template
        )
        rewrite_chain = LLMChain(llm=self.llm, prompt=rewrite_prompt)
        return rewrite_chain.predict(input=query)

    def _prepare_pgvector_query(self, metadata_filters: List[MetadataFilter]) -> str:
        # Base select JOINed with document source table.
        base_query = f"""SELECT * FROM {self.embeddings_table} AS e INNER JOIN {self.source_table} AS s ON (e.metadata->>'original_row_id')::int = s."{self.source_id_column}" """
        col_to_schema = {}
        if not self.metadata_schemas:
            return ""
        for schema in self.metadata_schemas:
            for col in schema.columns:
                col_to_schema[col.name] = schema
        joined_schemas = set()
        for filter in metadata_filters:
            # Join schemas before filtering.
            schema = col_to_schema.get(filter.attribute)
            if (
                schema is None
                or schema.table in joined_schemas
                or schema.table == self.source_table
            ):
                continue
            joined_schemas.add(schema.table)
            base_query += schema.join + " "
        # Actually construct WHERE conditions from metadata filters.
        if metadata_filters:
            base_query += "WHERE "
        for i, filter in enumerate(metadata_filters):
            value = filter.value
            if isinstance(value, str):
                value = f"'{value}'"
            base_query += f'"{filter.attribute}" {filter.comparator} {value}'
            if i < len(metadata_filters) - 1:
                base_query += " AND "
        base_query += f" ORDER BY e.embeddings {self.distance_function.value[0]} '{{embeddings}}' LIMIT {self.search_kwargs.k};"
        return base_query

    def _generate_metadata_filters(self, query: str) -> List[MetadataFilter]:
        parser = PydanticOutputParser(pydantic_object=MetadataFilters)
        metadata_prompt = self._prepare_metadata_prompt()
        metadata_filters_chain = LLMChain(llm=self.llm, prompt=metadata_prompt)
        metadata_filters_output = metadata_filters_chain.predict(
            format_instructions=parser.get_format_instructions(), input=query
        )
        # If the LLM outputs raw JSON, use it as-is.
        # If the LLM outputs anything including a json markdown section, use the last one.
        json_markdown_output = re.findall(
            r"```json.*```", metadata_filters_output, re.DOTALL
        )
        if json_markdown_output:
            metadata_filters_output = json_markdown_output[-1]
            # Clean the json tags.
            metadata_filters_output = metadata_filters_output[7:]
            metadata_filters_output = metadata_filters_output[:-3]
        metadata_filters = parser.invoke(metadata_filters_output)
        return metadata_filters.filters

    def _prepare_and_execute_query(
        self, query: str, embeddings_str: str
    ) -> HandlerResponse:
        try:
            metadata_filters = self._generate_metadata_filters(query)
            checked_sql_query = self._prepare_pgvector_query(metadata_filters)
            checked_sql_query_with_embeddings = checked_sql_query.format(
                embeddings=embeddings_str
            )
            return self.vector_store_handler.native_query(
                checked_sql_query_with_embeddings
            )
        except OutputParserException as e:
            logger.warning(
                f"LLM failed to generate structured metadata filters: {str(e)}"
            )
            return HandlerResponse(RESPONSE_TYPE.ERROR, error_message=str(e))
        except Exception as e:
            logger.warning(
                f"Failed to prepare and execute SQL query from structured metadata: {str(e)}"
            )
            return HandlerResponse(RESPONSE_TYPE.ERROR, error_message=str(e))

    def _get_relevant_documents(
        self, query: str, *, run_manager: CallbackManagerForRetrieverRun
    ) -> List[Document]:
        # Rewrite query to be suitable for retrieval.
        retrieval_query = self._prepare_retrieval_query(query)
        # Embed the rewritten retrieval query & include it in the similarity search pgvector query.
        embedded_query = self.embeddings_model.embed_query(retrieval_query)
        # Actually execute the similarity search with metadata filters.
        document_response = self._prepare_and_execute_query(
            retrieval_query, str(embedded_query)
        )
        num_retries = 0
        while num_retries < self.num_retries:
            if (
                document_response.resp_type != RESPONSE_TYPE.ERROR
                and len(document_response.data_frame) > 0
            ):
                # Successfully retrieved documents.
                break
            if document_response.resp_type == RESPONSE_TYPE.ERROR:
                # LLMs won't always generate structured metadata so we should have a fallback after retrying.
                logger.info(
                    f"SQL Retriever query failed with error {document_response.error_message}"
                )
            elif len(document_response.data_frame) == 0:
                logger.info("No documents retrieved from SQL Retriever query")

            document_response = self._prepare_and_execute_query(
                retrieval_query, str(embedded_query)
            )
            num_retries += 1
            if num_retries >= self.num_retries:
                logger.info("Using fallback retriever in SQL retriever.")
                return self.fallback_retriever._get_relevant_documents(
                    retrieval_query, run_manager=run_manager
                )

        document_df = document_response.data_frame
        retrieved_documents = []
        for _, document_row in document_df.iterrows():
            retrieved_documents.append(
                Document(
                    document_row.get("content", ""),
                    metadata=document_row.get("metadata", {}),
                )
            )
        if retrieved_documents:
            return retrieved_documents
        # If the SQL query constructed did not return any documents, fallback.
        logger.info(
            "No documents returned from SQL retriever. using fallback retriever."
        )
        return self.fallback_retriever._get_relevant_documents(
            retrieval_query, run_manager=run_manager
        )
