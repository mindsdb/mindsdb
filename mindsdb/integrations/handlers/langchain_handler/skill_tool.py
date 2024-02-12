from mindsdb_sql.parser.ast import Select, BinaryOperation, Identifier, Constant, Star

from mindsdb.integrations.libs.vectordatabase_handler import TableField
from mindsdb.interfaces.storage import db
from mindsdb.integrations.handlers.langchain_handler.mindsdb_database_agent import MindsDBSQL

import os
from typing import List

_DEFAULT_TOP_K_SIMILARITY_SEARCH = 5


def _make_text_to_sql_tools(skill: db.Skills, llm, executor) -> List:
    # To prevent dependency on Langchain unless an actual tool uses it.
    try:
        from langchain.agents.agent_toolkits import SQLDatabaseToolkit
        from langchain.tools.sql_database.tool import QuerySQLDataBaseTool
    except ImportError:
        raise ImportError('To use the text-to-SQL skill, please install langchain with `pip install langchain`')
    database = skill.params['database']
    tables = skill.params['tables']
    tables_to_include = [f'{database}.{table}' for table in tables]
    db = MindsDBSQL(
        engine=executor,
        metadata=executor.session.integration_controller,
        include_tables=tables_to_include
    )
    sql_database_tools = SQLDatabaseToolkit(db=db, llm=llm).get_tools()
    description = skill.params.get('description', '')
    tables_list = ','.join([f'{database}.{table}' for table in tables])
    for i, tool in enumerate(sql_database_tools):
        if isinstance(tool, QuerySQLDataBaseTool):
            # Add our own custom description so our agent knows when to query this table.
            tool.description = (
                f'Use this tool if you need data about {description}. '
                'Use the conversation context to decide which table to query. '
                f'These are the available tables: {tables_list}.\n'
                f'{tool.description}'
            )
            sql_database_tools[i] = tool
    return sql_database_tools


def _get_rag_query_function(
        skill: db.Skills,
        openai_api_key: str,
        session_controller):

    def _answer_question(question: str) -> str:
        knowledge_base_name = skill.params['source']

        # make select in KB table
        query = Select(
            targets=[Star()],
            where=BinaryOperation(op='=', args=[
                Identifier(TableField.CONTENT.value), Constant(question)
            ]),
            limit=Constant(10),
        )
        kb_table = session_controller.kb_controller.get_table(knowledge_base_name, skill.project_id)

        res = kb_table.select_query(query)
        return '\n'.join(res.content)

    return _answer_question


def _make_knowledge_base_tools(
        skill: db.Skills,
        openai_api_key: str,
        session_controller) -> List:
    # To prevent dependency on Langchain unless an actual tool uses it.
    try:
        from langchain.agents import Tool
    except ImportError:
        raise ImportError('To use the knowledge base skill, please install langchain with `pip install langchain`')
    description = skill.params.get('description', '')
    all_tools = []
    all_tools.append(Tool(
        name='Knowledge Base Retrieval',
        func=_get_rag_query_function(skill, openai_api_key, session_controller),
        description=f'Use this tool to get more context or information to answer a question about {description}. The input should be the exact question the user is asking.'
    ))
    return all_tools


def make_tools_from_skill(
        skill: db.Skills,
        llm,
        openai_api_key: str,
        executor) -> List:
    """Makes Langchain compatible tools from a skill

    Args:
        skill (Skills): Skill to make a tool from
        llm (BaseLanguageModel): LLM to use if the skill requires one
        openai_api_key (str): OpenAI API key to use if the skill requires one
        executor (ExecuteCommands): MindsDB executor to use if the skill requires one

    Returns:
        tools (List[BaseTool]): List of tools for the given skill
    """
    if skill.type == 'text_to_sql':
        return _make_text_to_sql_tools(skill, llm, executor)
    elif skill.type == 'knowledge_base':
        return _make_knowledge_base_tools(skill, openai_api_key, executor.session)
    raise NotImplementedError(f'skill of type {skill.type} is not supported as a tool')
