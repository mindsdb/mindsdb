from mindsdb.interfaces.storage import db
from mindsdb.integrations.handlers.langchain_embedding_handler.langchain_embedding_handler import construct_model_from_args
from mindsdb.integrations.handlers.langchain_handler.mindsdb_database_agent import MindsDBSQL
from mindsdb.integrations.handlers.rag_handler.rag import RAGQuestionAnswerer
from mindsdb.integrations.handlers.rag_handler.settings import OpenAIParameters, RAGHandlerParameters

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
        # Get the knowledge base associated with the skill.
        knowledge_base_name = skill.params['source']
        knowledge_base = session_controller.kb_controller.get(knowledge_base_name, skill.project_id)

        # Get embedding model to use when querying the vector store.
        embedding_model_data = session_controller.model_controller.get_model(knowledge_base.embedding_model.name)
        embedding_model_args = embedding_model_data['problem_definition']['using']
        embedding_model_args['target'] = embedding_model_data['problem_definition']['target']
        embedding_model = construct_model_from_args(embedding_model_args)

        # Get vector store handler.
        vector_store_handler_data = session_controller.integration_controller.get(knowledge_base.vector_database.name)
        vector_store_args = vector_store_handler_data['connection_data']
        vector_store_folder_name = vector_store_args.get('persist_directory', os.getcwd())
        vector_store_handler = session_controller.integration_controller.get_data_handler(knowledge_base.vector_database.name)
        vector_store_storage_path = vector_store_handler.handler_storage.folder_get(vector_store_folder_name)

        # Use OpenAI for interpreting the vector store search response.
        llm_params_dict = {
            'llm_name': 'openai',
            'openai_api_key': openai_api_key
        }
        qa_params = {
            'llm_type': 'openai',
            'llm_params': OpenAIParameters(**llm_params_dict),
            'embeddings_model': embedding_model,
            'top_k': skill.params.get('top_k', _DEFAULT_TOP_K_SIMILARITY_SEARCH),
            'collection_name': knowledge_base.vector_database_table,
            'vector_store_folder_name': vector_store_folder_name,
            'vector_store_storage_path': vector_store_storage_path,
            'vector_store_name': vector_store_handler_data['engine']
        }

        args = RAGHandlerParameters(**qa_params)
        question_answerer = RAGQuestionAnswerer(args=args)
        # Actually answer the query.
        qa_query_response = question_answerer.query(question)
        return qa_query_response['answer']

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
