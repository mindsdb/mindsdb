import os
import pytest
from unittest.mock import MagicMock

from langchain_community.tools.sql_database.tool import QuerySQLCheckerTool
from langchain_community.utilities.sql_database import SQLDatabase
from langchain_core.embeddings import Embeddings
from langchain_openai.chat_models.base import ChatOpenAI

from mindsdb.integrations.libs.vectordatabase_handler import VectorStoreHandler
from mindsdb.integrations.utilities.rag.retrievers.sql_retriever import SQLRetriever


OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')

class TestSQLRetriever:
    @pytest.mark.skipif(OPENAI_API_KEY is None, reason='Missing OpenAI API key (OPENAI_API_KEY env variable)')
    def test_basic(self):
        model = 'gpt-4o'
        llm = ChatOpenAI(
            api_key=OPENAI_API_KEY,
            model=model,
            temperature=0
        )
        db_mock = MagicMock(spec=SQLDatabase, wraps=SQLDatabase)
        vector_db_mock = MagicMock(spec=VectorStoreHandler, wraps=VectorStoreHandler)
        embeddings_mock = MagicMock(spec=Embeddings, wraps=Embeddings)
        embeddings_mock.embed_query.return_value = list(range(768))
        query_checker_tool = QuerySQLCheckerTool(
            llm=llm,
            db=db_mock
        )

        sql_retriever = SQLRetriever(
            vector_store_handler=vector_db_mock,
            embeddings_model=embeddings_mock,
            query_checker_tool=query_checker_tool,
            llm=llm
        )
        sql_retriever.invoke('What are Beaver Valley plant documents for nuclear fuel waste?')