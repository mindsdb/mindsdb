import re

from pydantic import BaseModel, Field
from typing import List, Any, Optional, Dict, Tuple, Union, Callable
import collections
import math

from langchain.chains.llm import LLMChain
from langchain_core.callbacks.manager import CallbackManagerForRetrieverRun
from langchain_core.documents.base import Document
from langchain_core.embeddings import Embeddings
from langchain_core.exceptions import OutputParserException
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import PromptTemplate, ChatPromptTemplate
from langchain_core.retrievers import BaseRetriever

from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from mindsdb.integrations.libs.response import HandlerResponse
from mindsdb.integrations.libs.vectordatabase_handler import (
    DistanceFunction,
    VectorStoreHandler,
)
from mindsdb.integrations.utilities.rag.settings import (
    DatabaseSchema,
    TableSchema,
    ColumnSchema,
    ValueSchema,
    SearchKwargs,
)
from mindsdb.utilities import log

import numpy as np

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
    database_schema: Optional[DatabaseSchema] = None

    # prompt templates
    rewrite_prompt_template: str

    # schema templates
    table_prompt_template: str
    value_prompt_template: str
    column_prompt_template: str

    # formatting templates
    boolean_system_prompt: str
    generative_system_prompt: str

    embeddings_model: Embeddings
    num_retries: int
    embeddings_table: str
    source_table: str
    source_id_column: str = "Id"
    distance_function: DistanceFunction
    search_kwargs: SearchKwargs

    # search parameters
    max_filters: int
    filter_threshold: float

    llm: BaseChatModel

    def _sort_schema_by_priority_key(
        self,
        schema_dict_item: Tuple[str, Union[TableSchema, ColumnSchema, ValueSchema]],
    ):
        return schema_dict_item[1].priority

    def _sort_schema_by_relevance_key(
        self,
        schema_dict_item: Tuple[str, Union[TableSchema, ColumnSchema, ValueSchema]],
    ):
        if schema_dict_item[1].relevance is not None:
            return schema_dict_item[1].relevance
        else:
            return float("-inf")

    def _sort_schema_by_key(
        self,
        schema: Union[DatabaseSchema, TableSchema, ColumnSchema],
        key: Callable,
        update: Dict[str, Any] = None,
    ) -> Union[DatabaseSchema, TableSchema, ColumnSchema]:
        """Takes a schema and converts its dict into an OrderedDict"""
        if isinstance(schema, DatabaseSchema):
            collection_key = "tables"
        elif isinstance(schema, TableSchema):
            collection_key = "columns"
        elif isinstance(schema, ColumnSchema):
            collection_key = "values"
        else:
            raise Exception(
                "schema must be either a DatabaseSchema, TableSchema, or ColumnSchema."
            )

        if update is not None:
            ordered = collections.OrderedDict(sorted(update.items(), key=key))
        else:
            ordered = collections.OrderedDict(
                sorted(getattr(schema, collection_key).items(), key=key)
            )
        schema = schema.model_copy(update={collection_key: ordered})

        return schema

    def _sort_database_schema_by_key(
        self, database_schema: DatabaseSchema, key: Callable
    ) -> DatabaseSchema:
        """Re-build schema with OrderedDicts"""
        tables = {}
        # build new tables dict
        for table_key, table_schema in database_schema.tables.items():
            columns = {}
            # build new column dict
            for column_key, column_schema in table_schema.columns.items():
                # sort values directly and update column schema
                columns[column_key] = self._sort_schema_by_key(
                    schema=column_schema, key=key
                )
            # update table schema and sort
            tables[table_key] = self._sort_schema_by_key(
                schema=table_schema, key=key, update=columns
            )
        # update table schema and sort
        database_schema = self._sort_schema_by_key(
            schema=database_schema, key=key, update=tables
        )

        return database_schema

    def _prepare_value_prompt(
        self,
        value_schema: ValueSchema,
        column_schema: ColumnSchema,
        table_schema: TableSchema,
        boolean_system_prompt: bool = True,
    ) -> ChatPromptTemplate:

        if boolean_system_prompt is True:
            system_prompt = self.boolean_system_prompt
        else:
            system_prompt = self.generative_system_prompt

        prepared_column_prompt = self._prepare_column_prompt(
            column_schema=column_schema, table_schema=table_schema
        )
        column_schema_str = (
            prepared_column_prompt.messages[1]
            .format(
                **prepared_column_prompt.partial_variables,
                query="See query at the lowest level schema.",
            )
            .content
        )

        base_prompt_template = ChatPromptTemplate.from_messages(
            [("system", system_prompt), ("user", self.value_prompt_template)]
        )

        if getattr(value_schema, "examples", None) is not None:
            example_str = """## **Example Questions**
"""
            for example in value_schema.examples:
                example_str += f"""- {example}
"""
        else:
            example_str = ""

        return base_prompt_template.partial(
            column_schema=column_schema_str,
            value=value_schema.value,
            type=value_schema.type,
            description=value_schema.description,
            usage=value_schema.usage,
            examples=example_str,
        )

    def _prepare_column_prompt(
        self,
        column_schema: ColumnSchema,
        table_schema: TableSchema,
        boolean_system_prompt: bool = True,
    ) -> ChatPromptTemplate:

        if boolean_system_prompt is True:
            system_prompt = self.boolean_system_prompt
        else:
            system_prompt = self.generative_system_prompt

        prepared_table_prompt = self._prepare_table_prompt(
            table_schema=table_schema, boolean_system_prompt=boolean_system_prompt
        )
        table_schema_str = (
            prepared_table_prompt.messages[1]
            .format(
                **prepared_table_prompt.partial_variables,
                query="See query at the lowest level schema",
            )
            .content
        )

        base_prompt_template = ChatPromptTemplate.from_messages(
            [("system", system_prompt), ("user", self.column_prompt_template)]
        )

        value_str = ""
        if type(column_schema.model_fields["values"]) is dict:
            if (
                type(list(column_schema.model_fields["values"].values)[0])
                is ValueSchema
            ):
                value_str = """
## **Value Descriptions**

Below are descriptions of each value in this column:
"""
                for value_schema in column_schema.values.values():
                    value_str += f"""
- **{value_schema.value}:** {value_schema.description}
"""

            else:
                value_str = """
## **Enumerated Values**

These column values are an enumeration of named values. These are listed below with format **[Column Value]**: [named value].
"""
                for value, value_name in column_schema.values.items():
                    value_str += f"""
- **{value}:** {value_name}"""

        elif type(column_schema.model_fields["values"]) is list:
            value_str = f"""
## **Sample Values**

There are too many unique values in the column to list exhaustively. Below is a sampling of values found in the column:
{column_schema.model_fields['values']}
"""

        if getattr(column_schema, "examples", None) is not None:
            example_str = """## **Example Questions**
"""
            for example in column_schema.examples:
                example_str += f"""- {example}
"""
        else:
            example_str = ""

        return base_prompt_template.partial(
            table_schema=table_schema_str,
            column=column_schema.column,
            type=column_schema.type,
            description=column_schema.description,
            usage=column_schema.usage,
            values=value_str,
            examples=example_str,
        )

    def _prepare_table_prompt(
        self, table_schema: TableSchema, boolean_system_prompt: bool = True
    ) -> ChatPromptTemplate:
        if boolean_system_prompt is True:
            system_prompt = self.boolean_system_prompt
        else:
            system_prompt = self.generative_system_prompt

        base_prompt_template = ChatPromptTemplate.from_messages(
            [("system", system_prompt), ("user", self.table_prompt_template)]
        )

        columns_str = ""
        for column_key, column_schema in table_schema.columns.items():
            columns_str += f"""
- **{column_schema.column}:** {column_schema.description}
"""

        if getattr(table_schema, "examples", None) is not None:
            example_str = """## **Example Questions**
"""
            for example in table_schema.examples:
                example_str += f"""- {example}
"""
        else:
            example_str = ""

        return base_prompt_template.partial(
            table=table_schema.table,
            description=table_schema.description,
            usage=table_schema.usage,
            columns=columns_str,
            examples=example_str,
        )

    def _rank_schema(self, prompt: ChatPromptTemplate, query: str) -> float:
        rank_chain = LLMChain(llm=self.llm.bind(logprobs=True), prompt=prompt, return_final_only=False)
        output = rank_chain({"query": query})  # returns metadata

        #  parse through metadata tokens until encountering either yes, or no.
        score = None  # a None score indicates the model output could not be parsed.
        for content in output["full_generation"][0].message.response_metadata[
            "logprobs"
        ]["content"]:
            #  Convert answer to score using the model's confidence
            if content["token"].lower().strip() == "yes":
                score = (
                    1 + math.exp(content["logprob"])
                ) / 2  # If yes, use the model's confidence
                break
            elif content["token"].lower().strip() == "no":
                score = (
                    1 - math.exp(content["logprob"])
                ) / 2  # If no, invert the confidence
                break

        return score

    def _breadth_first_search(self, query: str, greedy: bool = True):
        """Search breadth wise through Tables, then Columns, then Values.Uses a greedy strategy to maximize quota if greedy=True, otherwise a dynamic strategy."""

        # sort based on priority
        ordered_database_schema = self._sort_database_schema_by_key(
            database_schema=self.database_schema, key=self._sort_schema_by_priority_key
        )

        #  Rank Tables ########################################################
        greedy_count = 0
        # rank tables by relevance
        for table_key, table_schema in ordered_database_schema.items():
            prompt: ChatPromptTemplate = self._prepare_table_prompt(
                table_schema=table_schema, boolean_system_prompt=True
            )
            table_schema.relevance = self._rank_schema(prompt=prompt, query=query)

            if greedy:
                if table_schema.relevance >= ordered_database_schema.filter_threshold:
                    greedy_count += 1
                if greedy_count >= ordered_database_schema.max_filters:
                    break
                if greedy_count >= self.max_filters:
                    break

        #  sort tables
        ordered_database_schema = self._sort_schema_by_key(
            schema=ordered_database_schema, key=self._sort_schema_by_relevance_key
        )

        #  Rank Columns #######################################################
        #  iterate through tables to rank columns
        for table_key, table_schema in ordered_database_schema.items():
            tables = {}
            # only drop into tables above the filter threshold
            if table_schema.relevance >= ordered_database_schema.filter_threshold:
                greedy_count = 0
                # rank columns by relevance
                for column_key, column_schema in table_schema.columns.items():
                    prompt: ChatPromptTemplate = self._prepare_column_prompt(
                        column_schema=column_schema, boolean_system_prompt=True
                    )
                    column_schema.relevance = self._rank_schema(
                        prompt=prompt, query=query
                    )

                    if greedy:
                        if column_schema.relevance >= self.filter_threshold:
                            greedy_count += 1
                        if greedy_count >= table_schema.max_filters:
                            break
                        if greedy_count >= self.max_filters:
                            break

                # sort tables, drops tables that did not make the cut.
                tables[table_key] = self._sort_schema_by_key(
                    table_schema, key=self._sort_schema_by_relevance_key
                )

        # update top level schema with new tables
        ordered_database_schema = ordered_database_schema.model_copy(
            update={"tables": tables}
        )

        #  Rank Values ########################################################

        #  iterate through tables to rank values
        for table_key, table_schema in ordered_database_schema.items():
            tables = {}
            # iterate through columns to rank values
            for column_key, column_schema in table_schema.columns.items():
                columns = {}
                greedy_count = 0
                #  rank values by relevance
                if column_schema.relevance >= table_schema.filter_threshold:
                    for value_key, value_schema in column_schema.values.items():
                        prompt: ChatPromptTemplate = self._prepare_value_prompt(
                            value_schema=value_schema, boolean_system_prompt=True
                        )
                        column_schema.relevance = self._rank_schema(
                            prompt=prompt, query=query
                        )

                        if greedy:
                            if column_schema.relevance >= self.filter_threshold:
                                greedy_count += 1
                            if greedy_count >= table_schema.max_filters:
                                break
                            if greedy_count >= self.max_filters:
                                break

                    # sort the values, drop the values that did not make the cut
                    columns[column_key] = self._sort_schema_by_key(
                        value_schema, key=self._sort_schema_by_relevance_key
                    )

            # update table schema with new columns
            table_schema = table_schema.model_copy(update={"columns": columns})

            # sort tables, drops tables that did not make the cut.
            tables[table_key] = self._sort_schema_by_key(
                table_schema, key=self._sort_schema_by_relevance_key
            )

        # update database schema with new tables
        ordered_database_schema = ordered_database_schema.model_copy(
            update={"tables": tables}
        )

        self.ranked_database_schema = ordered_database_schema

        #  Build Ablation #####################################################

        ablation_value_dict = ({},)
        # assemble a relevance dictionary
        for table_key, table_schema in ordered_database_schema.items():
            for column_key, column_schema in table_schema.columns.items():
                for value_key, value_schema in column_schema.values.items():
                    ablation_value_dict[value_key] = value_schema.relevance

        self.ablation_value_dict = ablation_value_dict

        self.ablation_quantiles = np.quantile(
            ablation_value_dict.values, np.linspace(0, 1, self.num_retries + 2)[1:-1]
        )

    def breadth_first_ablation(self):
        """Ablate metadata filters in reverse breadth first search until the required minimum number of documents are returned."""
        pass

    def depth_first_search(self, greedy=True):
        """Search depth wise through Tables, then Columns, then Values. Uses a greedy strategy to maximize quota if greedy=True, otherwise a dynamic strategy."""
        pass

    def depth_first_ablation(self):
        """Ablate metadata filters in reverse depth first search until the required minimum number of documents are returned."""
        pass

    def _prepare_retrieval_query(self, query: str) -> str:
        rewrite_prompt = PromptTemplate(
            input_variables=["input"], template=self.rewrite_prompt_template
        )
        rewrite_chain = LLMChain(llm=self.llm, prompt=rewrite_prompt)
        return rewrite_chain.predict(input=query)

    def _prepare_pgvector_query(
        self, metadata_filters: MetadataFilters, retry: int = 0
    ) -> str:
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
