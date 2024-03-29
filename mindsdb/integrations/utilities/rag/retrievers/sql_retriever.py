import re

from langchain.sql_database import SQLDatabase
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableLambda, RunnablePassthrough, RunnableSerializable

from mindsdb.integrations.utilities.rag.retrievers.base import BaseRetriever
from mindsdb.integrations.utilities.rag.settings import RAGPipelineModel


class SQLRetriever(BaseRetriever):
    """
    A retriever used to connect to a postgres DB with pgvector extension
    """

    def __init__(self,
                 config: RAGPipelineModel,
                 ):
        self.prompt_template = config.retriever_prompt_template

        self.db = SQLDatabase.from_uri(config.db_connection_string)
        self.llm = config.llm
        self.embeddings_model = config.embeddings_model

    @staticmethod
    def format_prompt(prompt_template: str):
        """
        format prompt template

        :return:
        """
        return ChatPromptTemplate.from_messages(
            [("system", prompt_template), ("human", "{question}")]
        )

    def get_schema(self, _):
        """
        Get DB schema
        :return:
        """
        return self.db.get_table_info()

    def replace_brackets(self, match):
        words_inside_brackets = match.group(1).split(", ")
        embedded_words = [
            str(self.embeddings_model.embed_query(word)) for word in words_inside_brackets
        ]
        return "', '".join(embedded_words)

    def get_query(self, query):
        sql_query = re.sub(r"\[([\w\s,]+)\]", self.replace_brackets, query)
        return sql_query

    @property
    def sql_query_chain(self):
        return (
                RunnablePassthrough.assign(schema=self.get_schema)  # noqa: E126, E122
                | self.format_prompt(self.prompt_template["sql_query"])
                | self.llm.bind(stop=["\nSQLResult:"])
                | StrOutputParser()
        )

    def as_runnable(self) -> RunnableSerializable:
        return (
                RunnablePassthrough.assign(query=self.sql_query_chain)  # noqa: E126, E122
                | RunnablePassthrough.assign(
            schema=self.get_schema,  # noqa: E126, E122
            response=RunnableLambda(  # noqa: E126, E122
                lambda x: self.db.run(self.get_query(x["query"]))),
                )
                | self.format_prompt(self.prompt_template["sql_result"])
                | self.llm
                | StrOutputParser()
        )
