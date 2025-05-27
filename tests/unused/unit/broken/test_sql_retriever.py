from unittest.mock import MagicMock

import pandas as pd
from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings
from langchain_core.outputs.generation import Generation
from langchain_core.outputs.llm_result import LLMResult
from langchain_core.retrievers import BaseRetriever
from langchain_openai.chat_models.base import ChatOpenAI

from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from mindsdb.integrations.libs.response import HandlerResponse
from mindsdb.integrations.libs.vectordatabase_handler import DistanceFunction, VectorStoreHandler
from mindsdb.integrations.utilities.rag.retrievers.sql_retriever import SQLRetriever
from mindsdb.integrations.utilities.rag.settings import DEFAULT_METADATA_FILTERS_PROMPT_TEMPLATE, DEFAULT_SEMANTIC_PROMPT_TEMPLATE, ColumnSchema, MetadataSchema, SearchKwargs


class TestSQLRetriever:
    def test_basic(self):
        llm = MagicMock(spec=ChatOpenAI, wraps=ChatOpenAI)
        llm_result = MagicMock(spec=LLMResult, wraps=LLMResult)
        llm_result.generations = [
            [
                Generation(
                    text='''```json
{
    "filters": [
        {
            "attribute": "ContributorName",
            "comparator": "=",
            "value": "Alfred"
        }
    ]
}
```'''
                )
            ]
        ]
        llm.generate_prompt.return_value = llm_result
        vector_db_mock = MagicMock(spec=VectorStoreHandler, wraps=VectorStoreHandler)
        series = pd.Series(
            [0, 'Chunk1', '[1.0, 2.0, 3.0]', {'key1': 'value1'}, 0, 1],
            index=['id', 'content', 'embeddings', 'metadata', 'Id', 'Type']
        )
        df = pd.DataFrame([series])
        vector_db_mock.native_query.return_value = HandlerResponse(
            RESPONSE_TYPE.TABLE,
            data_frame=df
        )
        embeddings_mock = MagicMock(spec=Embeddings, wraps=Embeddings)
        embeddings_mock.embed_query.return_value = list(range(768))

        source_schema = MetadataSchema(
            table='test_source_table',
            description='Contains source documents',
            columns=[
                ColumnSchema(name='Id', type='int', description='Unique ID as primary key of doc'),
                ColumnSchema(name='Type', type='int', description='Document Type', values={1: 'Unknown', 2: 'Site Audit'})
            ]
        )
        unit_schema = MetadataSchema(
            table='unit',
            description='Contains information about specific units of power plants. Several units can be part of a single plant.',
            columns=[
                ColumnSchema(name='UnitKey', type='int', description='Unique ID of the unit'),
                ColumnSchema(name='PlantKey', type='int', description='ID of the plant the unit belongs to')
            ]
        )
        plant_schema = MetadataSchema(
            table='plant',
            description='Contains information about specific power plants',
            columns=[
                ColumnSchema(name='PlantKey', type='int', description='The unique ID of the plant'),
                ColumnSchema(name='PlantName', type='str', description='The name of the plant')
            ]
        )
        document_unit_schema = MetadataSchema(
            table='document_unit',
            description='Links documents to the power plant they are relevant to',
            columns=[
                ColumnSchema(name='DocumentId', type='int', description='The ID of the document associated with the unit'),
                ColumnSchema(name='UnitKey', type='int', description='The ID of the unit the documnet is associated with')
            ]
        )
        all_schemas = [source_schema, unit_schema, plant_schema, document_unit_schema]
        fallback_retriever = MagicMock(spec=BaseRetriever, wraps=BaseRetriever)
        sql_retriever = SQLRetriever(
            fallback_retriever=fallback_retriever,
            vector_store_handler=vector_db_mock,
            metadata_schemas=all_schemas,
            embeddings_model=embeddings_mock,
            metadata_filters_prompt_template=DEFAULT_METADATA_FILTERS_PROMPT_TEMPLATE,
            rewrite_prompt_template=DEFAULT_SEMANTIC_PROMPT_TEMPLATE,
            num_retries=2,
            embeddings_table='test_embeddings_table',
            source_table='test_source_table',
            distance_function=DistanceFunction.SQUARED_EUCLIDEAN_DISTANCE,
            search_kwargs=SearchKwargs(k=5),
            llm=llm
        )

        docs = sql_retriever.invoke('What are Beaver Valley plant documents for nuclear fuel waste?')
        # Make sure right doc was retrieved.
        assert len(docs) == 1
        assert docs[0].page_content == 'Chunk1'
        assert docs[0].metadata == {'key1': 'value1'}

    def test_retries(self):
        llm = MagicMock(spec=ChatOpenAI, wraps=ChatOpenAI)
        llm_result = MagicMock(spec=LLMResult, wraps=LLMResult)
        llm_result.generations = [
            [
                Generation(
                    text='''```json
{
    "filters": [
        {
            "attribute": "ContributorName",
            "comparator": "=",
            "value": "Alfred"
        }
    ]
}
```'''
                )
            ]
        ]
        llm.generate_prompt.return_value = llm_result
        vector_db_mock = MagicMock(spec=VectorStoreHandler, wraps=VectorStoreHandler)
        series = pd.Series(
            [0, 'Chunk1', '[1.0, 2.0, 3.0]', {'key1': 'value1'}, 0, 1],
            index=['id', 'content', 'embeddings', 'metadata', 'Id', 'Type']
        )
        df = pd.DataFrame([series])
        vector_db_mock.native_query.side_effect = [
            HandlerResponse(
                RESPONSE_TYPE.ERROR,
                error_message='Something went wrong I am in absolute shambles'
            ),
            HandlerResponse(
                RESPONSE_TYPE.ERROR,
                error_message='Something went wrong I am in absolute shambles'
            ),
            HandlerResponse(
                RESPONSE_TYPE.TABLE,
                data_frame=df
            )
        ]
        embeddings_mock = MagicMock(spec=Embeddings, wraps=Embeddings)
        embeddings_mock.embed_query.return_value = list(range(768))

        source_schema = MetadataSchema(
            table='test_source_table',
            description='Contains source documents',
            columns=[
                ColumnSchema(name='Id', type='int', description='Unique ID as primary key of doc'),
                ColumnSchema(name='Type', type='int', description='Document Type', values={1: 'Unknown', 2: 'Site Audit'})
            ]
        )
        unit_schema = MetadataSchema(
            table='unit',
            description='Contains information about specific units of power plants. Several units can be part of a single plant.',
            columns=[
                ColumnSchema(name='UnitKey', type='int', description='Unique ID of the unit'),
                ColumnSchema(name='PlantKey', type='int', description='ID of the plant the unit belongs to')
            ]
        )
        plant_schema = MetadataSchema(
            table='plant',
            description='Contains information about specific power plants',
            columns=[
                ColumnSchema(name='PlantKey', type='int', description='The unique ID of the plant'),
                ColumnSchema(name='PlantName', type='str', description='The name of the plant')
            ]
        )
        document_unit_schema = MetadataSchema(
            table='document_unit',
            description='Links documents to the power plant they are relevant to',
            columns=[
                ColumnSchema(name='DocumentId', type='int', description='The ID of the document associated with the unit'),
                ColumnSchema(name='UnitKey', type='int', description='The ID of the unit the documnet is associated with')
            ]
        )
        all_schemas = [source_schema, unit_schema, plant_schema, document_unit_schema]
        fallback_retriever = MagicMock(spec=BaseRetriever, wraps=BaseRetriever)
        sql_retriever = SQLRetriever(
            fallback_retriever=fallback_retriever,
            vector_store_handler=vector_db_mock,
            metadata_schemas=all_schemas,
            embeddings_model=embeddings_mock,
            metadata_filters_prompt_template=DEFAULT_METADATA_FILTERS_PROMPT_TEMPLATE,
            rewrite_prompt_template=DEFAULT_SEMANTIC_PROMPT_TEMPLATE,
            num_retries=3,
            embeddings_table='test_embeddings_table',
            source_table='test_source_table',
            distance_function=DistanceFunction.SQUARED_EUCLIDEAN_DISTANCE,
            search_kwargs=SearchKwargs(k=5),
            llm=llm
        )

        docs = sql_retriever.invoke('What are Beaver Valley plant documents for nuclear fuel waste?')
        # Make sure we retried.
        assert len(vector_db_mock.native_query.mock_calls) == 3
        # Make sure right doc was retrieved.
        assert len(docs) == 1
        assert docs[0].page_content == 'Chunk1'
        assert docs[0].metadata == {'key1': 'value1'}

    def test_fallback(self):
        llm = MagicMock(spec=ChatOpenAI, wraps=ChatOpenAI)
        llm_result = MagicMock(spec=LLMResult, wraps=LLMResult)
        llm_result.generations = [
            [
                Generation(
                    text='''```json
{
    "filters": [
        {
            "attribute": "ContributorName",
            "comparator": "=",
            "value": "Alfred"
        }
    ]
}
```'''
                )
            ]
        ]
        llm.generate_prompt.return_value = llm_result
        vector_db_mock = MagicMock(spec=VectorStoreHandler, wraps=VectorStoreHandler)
        vector_db_mock.native_query.side_effect = [
            HandlerResponse(
                RESPONSE_TYPE.ERROR,
                error_message='Something went wrong I am in absolute shambles'
            ),
            HandlerResponse(
                RESPONSE_TYPE.ERROR,
                error_message='Something went wrong I am in absolute shambles'
            ),
            HandlerResponse(
                RESPONSE_TYPE.ERROR,
                error_message='Something went wrong I am in absolute shambles'
            ),
        ]
        embeddings_mock = MagicMock(spec=Embeddings, wraps=Embeddings)
        embeddings_mock.embed_query.return_value = list(range(768))

        source_schema = MetadataSchema(
            table='test_source_table',
            description='Contains source documents',
            columns=[
                ColumnSchema(name='Id', type='int', description='Unique ID as primary key of doc'),
                ColumnSchema(name='Type', type='int', description='Document Type', values={1: 'Unknown', 2: 'Site Audit'})
            ]
        )
        unit_schema = MetadataSchema(
            table='unit',
            description='Contains information about specific units of power plants. Several units can be part of a single plant.',
            columns=[
                ColumnSchema(name='UnitKey', type='int', description='Unique ID of the unit'),
                ColumnSchema(name='PlantKey', type='int', description='ID of the plant the unit belongs to')
            ]
        )
        plant_schema = MetadataSchema(
            table='plant',
            description='Contains information about specific power plants',
            columns=[
                ColumnSchema(name='PlantKey', type='int', description='The unique ID of the plant'),
                ColumnSchema(name='PlantName', type='str', description='The name of the plant')
            ]
        )
        document_unit_schema = MetadataSchema(
            table='document_unit',
            description='Links documents to the power plant they are relevant to',
            columns=[
                ColumnSchema(name='DocumentId', type='int', description='The ID of the document associated with the unit'),
                ColumnSchema(name='UnitKey', type='int', description='The ID of the unit the documnet is associated with')
            ]
        )
        all_schemas = [source_schema, unit_schema, plant_schema, document_unit_schema]
        fallback_retriever = MagicMock(spec=BaseRetriever, wraps=BaseRetriever)
        fallback_retriever._get_relevant_documents.return_value = [
            Document(
                page_content='Chunk1',
                metadata={
                    'key1': 'value1'
                }
            )
        ]
        sql_retriever = SQLRetriever(
            fallback_retriever=fallback_retriever,
            vector_store_handler=vector_db_mock,
            metadata_schemas=all_schemas,
            embeddings_model=embeddings_mock,
            metadata_filters_prompt_template=DEFAULT_METADATA_FILTERS_PROMPT_TEMPLATE,
            rewrite_prompt_template=DEFAULT_SEMANTIC_PROMPT_TEMPLATE,
            num_retries=2,
            embeddings_table='test_embeddings_table',
            source_table='test_source_table',
            distance_function=DistanceFunction.SQUARED_EUCLIDEAN_DISTANCE,
            search_kwargs=SearchKwargs(k=5),
            llm=llm
        )

        docs = sql_retriever.invoke('What are Beaver Valley plant documents for nuclear fuel waste?')
        # Make sure we retried.
        assert len(vector_db_mock.native_query.mock_calls) == 3
        # Make sure we falled back.
        assert len(fallback_retriever._get_relevant_documents.mock_calls) == 1
        # Make sure right doc was retrieved.
        assert len(docs) == 1
        assert docs[0].page_content == 'Chunk1'
        assert docs[0].metadata == {'key1': 'value1'}
