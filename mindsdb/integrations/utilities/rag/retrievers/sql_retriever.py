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


class AblativeMetadataFilter(MetadataFilter):
    """Adds additional fields to support ablation."""

    schema_table: str = Field(description="schema name of the table for this filter")
    schema_column: str = Field(description="schema name of the column for this filter")
    schema_value: str = Field(description="schema name of the value for this filter")


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
    # search parameters
    max_filters: int
    filter_threshold: float
    min_k: int

    # Schema description
    database_schema: Optional[DatabaseSchema] = None

    # Embeddings
    embeddings_model: Embeddings
    search_kwargs: SearchKwargs

    # prompt templates
    rewrite_prompt_template: str

    # schema templates
    table_prompt_template: str
    column_prompt_template: str
    value_prompt_template: str

    # formatting templates
    boolean_system_prompt: str
    generative_system_prompt: str

    # SQL search config
    num_retries: int
    embeddings_table: str
    source_table: str
    source_id_column: str
    distance_function: DistanceFunction

    # Re-rank and metadata generation model.
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
            return 0

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
            ordered = collections.OrderedDict(
                sorted(update.items(), key=key, reverse=True)
            )
        else:
            ordered = collections.OrderedDict(
                sorted(getattr(schema, collection_key).items(), key=key, reverse=True)
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
        format_instructions: Optional[str] = None,
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

        value_str = ""
        header_str = ""
        if type(value_schema.value) in [str, int, float, bool]:
            header_str = f"This schema describes a single value in the {column_schema.column} column."

            value_str = f"""
 -**Value**: {value_schema.value}
"""

        elif type(value_schema.value) is dict:
            header_str = f"This schema describes enumerated values in the {column_schema.column} column."

            value_str = """
## **Enumerated Values**

The values in the column are an enumeration of named values. These are listed below with format **[Column Value]**: [named value].
"""
            for value, value_name in value_schema.value.items():
                value_str += f"""
- **{value}:** {value_name}"""

        elif type(value_schema.value) is list:
            header_str = f"This schema describes some of the values in the {column_schema.column} column."

            value_str = """
## **Sample Values**

There are too many values in this column to list exhaustively. Below is a sampling of values found in the column:
"""
            for value in value_schema.value:
                value_str += f"""
- {value}"""

        if getattr(value_schema, "comparator", None) is not None:
            comparator_str = """

## **Comparators**

Below is a list of comparison operators for constructing filters for this value schema:
"""
            if type(value_schema.comparator) is str:
                comparator_str += f"""- {value_schema.comparator}
"""
            else:
                for comp in value_schema.comparator:
                    comparator_str += f"""- {comp}
"""
        else:
            comparator_str = ""

        if getattr(value_schema, "example_questions", None) is not None:
            example_str = """## **Example Questions**
"""
            for i, example in enumerate(value_schema.example_questions):
                example_str += f"""{i}. **Query:** {example.input} **Answer:** {example.output}
"""
        else:
            example_str = ""

        return base_prompt_template.partial(
            format_instructions=format_instructions,
            header=header_str,
            column_schema=column_schema_str,
            value=value_str,
            comparator=comparator_str,
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

        header_str = (
            f"This schema describes a column in the {table_schema.table} table."
        )

        value_str = """
## **Content**

Below is a description of the contents in this column in list format:
"""
        for value_schema in column_schema.values.values():
            value_str += f"""
- {value_schema.description}
"""
        value_str += """
**Important:** The above descriptions are not the actual values stored in this column. See the Value schema for actual values.
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
            header=header_str,
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

        header_str = "This schema describes a table in the database."

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
            header=header_str,
            table=table_schema.table,
            description=table_schema.description,
            usage=table_schema.usage,
            columns=columns_str,
            examples=example_str,
        )

    def _rank_schema(self, prompt: ChatPromptTemplate, query: str) -> float:
        rank_chain = LLMChain(
            llm=self.llm.bind(logprobs=True), prompt=prompt, return_final_only=False
        )
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

        if score is None:
            score = 0.0

        return score

    def _breadth_first_search(self, query: str, greedy: bool = False) -> Tuple:
        """Search breadth wise through Tables, then Columns, then Values.Uses a greedy strategy to maximize quota if greedy=True, otherwise a dynamic strategy."""

        # sort based on priority
        ordered_database_schema = self._sort_database_schema_by_key(
            database_schema=self.database_schema, key=self._sort_schema_by_priority_key
        )

        #  Rank Tables ########################################################
        greedy_count = 0
        tables = {}
        # rank tables by relevance
        for table_key, table_schema in ordered_database_schema.tables.items():
            prompt: ChatPromptTemplate = self._prepare_table_prompt(
                table_schema=table_schema, boolean_system_prompt=True
            )
            table_schema.relevance = self._rank_schema(prompt=prompt, query=query)

            # only keep greedy tables
            tables[table_key] = table_schema

            if greedy:
                if table_schema.relevance >= ordered_database_schema.filter_threshold:
                    greedy_count += 1
                if greedy_count >= ordered_database_schema.max_filters:
                    break

        #  sort tables
        ordered_database_schema = self._sort_schema_by_key(
            schema=ordered_database_schema,
            key=self._sort_schema_by_relevance_key,
            update=tables,
        )

        #  Rank Columns #######################################################
        #  iterate through tables to rank columns
        tables = {}
        table_count = 0  # take only the top n number of tables specified by the databases max filters
        for table_key, table_schema in ordered_database_schema.tables.items():
            # only drop into tables above the filter threshold
            if table_schema.relevance >= ordered_database_schema.filter_threshold:
                greedy_count = 0
                # rank columns by relevance
                columns = {}
                for column_key, column_schema in table_schema.columns.items():
                    prompt: ChatPromptTemplate = self._prepare_column_prompt(
                        column_schema=column_schema,
                        table_schema=table_schema,
                        boolean_system_prompt=True,
                    )
                    column_schema.relevance = self._rank_schema(
                        prompt=prompt, query=query
                    )

                    columns[column_key] = column_schema

                    if greedy:
                        if column_schema.relevance >= table_schema.filter_threshold:
                            greedy_count += 1
                        if greedy_count >= table_schema.max_filters:
                            break

                # sort columns and keep only columns that made the cut.
                tables[table_key] = self._sort_schema_by_key(
                    table_schema, key=self._sort_schema_by_relevance_key, update=columns
                )

                table_count += 1
                if table_count >= ordered_database_schema.max_filters:
                    break

        # sort tables and keep only tables that made the cut.
        ordered_database_schema = self._sort_schema_by_key(
            ordered_database_schema,
            key=self._sort_schema_by_relevance_key,
            update=tables,
        )

        #  Rank Values ########################################################
        #  iterate through tables to rank values
        tables = {}
        for table_key, table_schema in ordered_database_schema.tables.items():
            columns = {}
            column_count = 0
            # iterate through columns to rank values
            for column_key, column_schema in table_schema.columns.items():
                if column_schema.relevance >= table_schema.filter_threshold:
                    greedy_count = 0
                    values = {}
                    #  rank values by relevance
                    for value_key, value_schema in column_schema.values.items():
                        prompt: ChatPromptTemplate = self._prepare_value_prompt(
                            value_schema=value_schema,
                            column_schema=column_schema,
                            table_schema=table_schema,
                            boolean_system_prompt=True,
                        )
                        value_schema.relevance = self._rank_schema(
                            prompt=prompt, query=query
                        )

                        values[value_key] = value_schema

                        if greedy:
                            if value_schema.relevance >= column_schema.filter_threshold:
                                greedy_count += 1
                            if greedy_count >= column_schema.max_filters:
                                break

                    # sort values and keep only values that make the cut
                    columns[column_key] = self._sort_schema_by_key(
                        column_schema,
                        key=self._sort_schema_by_relevance_key,
                        update=values,
                    )

                    column_count += 1
                    if column_count >= table_schema.max_filters:
                        break

            # sort columns and keep only columns that made the cut
            tables[table_key] = self._sort_schema_by_key(
                table_schema, key=self._sort_schema_by_relevance_key, update=columns
            )

        # sort tables and keep only tables that made the cut.
        ordered_database_schema = self._sort_schema_by_key(
            ordered_database_schema,
            key=self._sort_schema_by_relevance_key,
            update=tables,
        )

        #  discard low ranked values ###################################################################################
        tables = {}
        for table_key, table_schema in ordered_database_schema.tables.items():
            columns = {}
            # iterate through columns to rank values
            for column_key, column_schema in table_schema.columns.items():
                value_count = 0
                values = {}
                #  rank values by relevance
                for value_key, value_schema in column_schema.values.items():
                    if value_schema.relevance >= column_schema.filter_threshold:
                        values[value_key] = value_schema

                        value_count += 1
                        if value_count >= column_schema.max_filters:
                            break

                # sort values and keep only values that make the cut
                columns[column_key] = self._sort_schema_by_key(
                    column_schema,
                    key=self._sort_schema_by_relevance_key,
                    update=values,
                )

            # sort columns and keep only columns that made the cut
            tables[table_key] = self._sort_schema_by_key(
                table_schema, key=self._sort_schema_by_relevance_key, update=columns
            )

        # sort tables and keep only tables that made the cut.
        ordered_database_schema = self._sort_schema_by_key(
            ordered_database_schema,
            key=self._sort_schema_by_relevance_key,
            update=tables,
        )

        ranked_database_schema = ordered_database_schema

        #  Build Ablation #####################################################

        ablation_value_dict = {}
        # assemble a relevance dictionary
        for table_key, table_schema in ordered_database_schema.tables.items():
            for column_key, column_schema in table_schema.columns.items():
                for value_key, value_schema in column_schema.values.items():
                    ablation_value_dict[(table_key, column_key, value_key)] = (
                        value_schema.relevance
                    )

        ablation_value_dict = collections.OrderedDict(
            sorted(ablation_value_dict.items(), key=lambda x: x[1])
        )

        relevance_scores = list(ablation_value_dict.values())
        if len(relevance_scores) > 0:
            ablation_quantiles = np.quantile(
                relevance_scores, np.linspace(0, 1, self.num_retries + 2)[1:-1]
            )
        else:
            ablation_quantiles = None

        return ranked_database_schema, ablation_value_dict, ablation_quantiles

    def _dynamic_ablation(
        self,
        metadata_filters: List[AblativeMetadataFilter],
        ablation_value_dict,
        ablation_quantiles,
        retry: int,
    ):
        """Ablate metadata filters in aggregate by quantiles until the required minimum number of documents are returned."""

        ablated_dict = {}
        for key, value in ablation_value_dict.items():
            if value >= ablation_quantiles[retry]:
                ablated_dict[key] = value

        #  discard low ranked filters ##################################################################################
        ablated_filters = []
        for filter in metadata_filters:
            for key in ablated_dict.keys():
                if (
                    filter.schema_table in key
                    and filter.schema_column in key
                    and filter.schema_value in key
                ):
                    ablated_filters.append(filter)

        return ablated_filters

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
        self,
        ranked_database_schema: DatabaseSchema,
        metadata_filters: List[AblativeMetadataFilter],
        retry: int = 0,
    ) -> str:
        # Base select JOINed with document source table.
        base_query = f"""SELECT * FROM {self.embeddings_table} AS e INNER JOIN {self.source_table} AS s ON (e.metadata->>'original_row_id')::int = s."{self.source_id_column}" """

        # return an empty string if schema has not been ranked
        if not ranked_database_schema:
            return ""

        # Add Table JOIN statements
        join_clauses = set()
        for metadata_filter in metadata_filters:
            join_clause = ranked_database_schema.tables[
                metadata_filter.schema_table
            ].join
            if join_clause in join_clauses:
                continue
            else:
                join_clauses.add(join_clause)
                base_query += join_clause + " "

        # Add WHERE conditions from metadata filters
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

    def _generate_filter(
        self, prompt: ChatPromptTemplate, query: str
    ) -> MetadataFilter:
        gen_filter_chain = LLMChain(llm=self.llm, prompt=prompt)
        output = gen_filter_chain({"query": query})
        return output

    def _generate_metadata_filters(
        self, query: str, ranked_database_schema
    ) -> Union[List[AblativeMetadataFilter], HandlerResponse]:
        parser = PydanticOutputParser(pydantic_object=MetadataFilter)

        metadata_filter_list = []
        #  iterate through tables to rank values
        for table_key, table_schema in ranked_database_schema.tables.items():
            # iterate through columns to rank values
            for column_key, column_schema in table_schema.columns.items():
                if column_schema.relevance >= table_schema.filter_threshold:
                    #  generate filters
                    for value_key, value_schema in column_schema.values.items():
                        # must use generation if field is a dictionary of tuples or a list
                        if type(value_schema.value) in [list, dict]:
                            try:
                                metadata_prompt: ChatPromptTemplate = (
                                    self._prepare_value_prompt(
                                        format_instructions=parser.get_format_instructions(),
                                        value_schema=value_schema,
                                        column_schema=column_schema,
                                        table_schema=table_schema,
                                        boolean_system_prompt=False,
                                    )
                                )

                                metadata_filters_chain = LLMChain(
                                    llm=self.llm, prompt=metadata_prompt
                                )
                                metadata_filter_output = metadata_filters_chain.predict(
                                    query=query,
                                )

                                # If the LLM outputs raw JSON, use it as-is.
                                # If the LLM outputs anything including a json markdown section, use the last one.
                                json_markdown_output = re.findall(
                                    r"```json.*```", metadata_filter_output, re.DOTALL
                                )
                                if json_markdown_output:
                                    metadata_filter_output = json_markdown_output[-1]
                                    # Clean the json tags.
                                    metadata_filter_output = metadata_filter_output[7:]
                                    metadata_filter_output = metadata_filter_output[:-3]

                                metadata_filter = parser.invoke(metadata_filter_output)
                                model_dump = metadata_filter.model_dump()
                                model_dump.update(
                                    {
                                        "schema_table": table_key,
                                        "schema_column": column_key,
                                        "schema_value": value_key,
                                    }
                                )
                                metadata_filter = AblativeMetadataFilter(**model_dump)
                            except OutputParserException as e:
                                logger.warning(
                                    f"LLM failed to generate structured metadata filters: {str(e)}"
                                )
                                return HandlerResponse(
                                    RESPONSE_TYPE.ERROR, error_message=str(e)
                                )
                        else:
                            metadata_filter = AblativeMetadataFilter(
                                attribute=column_schema.column,
                                comparator=value_schema.comparator,
                                value=value_schema.value,
                                schema_table=table_key,
                                schema_column=column_key,
                                schema_value=value_key,
                            )
                        metadata_filter_list.append(metadata_filter)

        return metadata_filter_list

    def _prepare_and_execute_query(
        self,
        ranked_database_schema: DatabaseSchema,
        metadata_filters: List[AblativeMetadataFilter],
        embeddings_str: str,
    ) -> HandlerResponse:
        try:
            checked_sql_query = self._prepare_pgvector_query(
                ranked_database_schema, metadata_filters
            )
            checked_sql_query_with_embeddings = checked_sql_query.format(
                embeddings=embeddings_str
            )
            return self.vector_store_handler.native_query(
                checked_sql_query_with_embeddings
            )
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

        # Search for relevant filters
        ranked_database_schema, ablation_value_dict, ablation_quantiles = (
            self._breadth_first_search(query=query)
        )

        # Generate metadata filters
        metadata_filters = self._generate_metadata_filters(
            query=query, ranked_database_schema=ranked_database_schema
        )

        if type(metadata_filters) is list:
            # Initial Execution of the similarity search with metadata filters.
            document_response = self._prepare_and_execute_query(
                ranked_database_schema=ranked_database_schema,
                metadata_filters=metadata_filters,
                embeddings_str=str(embedded_query),
            )
            num_retries = 0
            while num_retries < self.num_retries:
                if (
                    document_response.resp_type != RESPONSE_TYPE.ERROR
                    and len(document_response.data_frame) >= self.min_k
                ):
                    # Successfully retrieved k documents to send to re-ranker.
                    break
                elif document_response.resp_type == RESPONSE_TYPE.ERROR:
                    # LLMs won't always generate structured metadata so we should have a fallback after retrying.
                    logger.info(
                        f"SQL Retriever query failed with error {document_response.error_message}"
                    )
                else:
                    logger.info(
                        f"SQL Retriever did not retrieve {self.min_k} documents: {len(document_response.data_frame)} documents retrieved."
                    )

                ablated_metadata_filters = self._dynamic_ablation(
                    metadata_filters=metadata_filters,
                    ablation_value_dict=ablation_value_dict,
                    ablation_quantiles=ablation_quantiles,
                    retry=num_retries,
                )

                document_response = self._prepare_and_execute_query(
                    ranked_database_schema=ranked_database_schema,
                    metadata_filters=ablated_metadata_filters,
                    embeddings_str=str(embedded_query),
                )

                num_retries += 1

            retrieved_documents = []
            if document_response.resp_type != RESPONSE_TYPE.ERROR:
                document_df = document_response.data_frame
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
                "No documents returned from SQL retriever, using fallback retriever."
            )
            return self.fallback_retriever._get_relevant_documents(
                retrieval_query, run_manager=run_manager
            )
        else:
            # If no metadata fields could be generated fallback.
            logger.info(
                "No metadata fields were successfully generated, using fallback retriever."
            )
            return self.fallback_retriever._get_relevant_documents(
                retrieval_query, run_manager=run_manager
            )
