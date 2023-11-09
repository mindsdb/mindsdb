from mindsdb.interfaces.database.integrations import integration_controller
from mindsdb.interfaces.storage import db

from langchain.tools.base import BaseTool
from sqlalchemy import create_engine, URL

import openai

def _make_text_to_sql_tool(skill: db.Skills) -> BaseTool:
    openai.api_key = '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'
    # Only import LlamaIndex if needed to avoid unnecessary dependencies when using unrelated skills.
    from llama_index import SQLDatabase
    from llama_index.indices.struct_store import NLSQLTableQueryEngine
    from llama_index.langchain_helpers.agents import LlamaIndexTool, IndexToolConfig
    integration = integration_controller.get_handler(skill.params['database'])
    # Supported dialects: https://docs.sqlalchemy.org/en/20/dialects/index.html
    connection_url = URL.create(
        # TODO DONT HARDCODE DIALECT
        'postgresql',
        username=integration.connection_args.get('user', None),
        password=integration.connection_args.get('password', None),
        host=integration.connection_args.get('host', None),
        port=integration.connection_args.get('port', None),
        database=integration.connection_args.get('database', None)
    )
    print('CREATED CONNECTION URL')
    print(connection_url)
    engine = create_engine(connection_url)
    tables_to_include = skill.params['tables']
    database = SQLDatabase(engine, include_tables=tables_to_include)
    query_engine = NLSQLTableQueryEngine(database)
    database_tool_config = IndexToolConfig(
        query_engine=query_engine,
        name=f'Database Retreival',
        description=f'useful for when you want to answer queries about {",".join(tables_to_include)}',
        tool_kwargs={'return_direct': True}
    )
    return LlamaIndexTool.from_tool_config(tool_config=database_tool_config)

def make_tool_from_skill(skill: db.Skills) -> BaseTool:
    if skill.type == 'text_to_sql':
        return _make_text_to_sql_tool(skill)
    raise NotImplementedError(f'skill of type {skill.type} is not supported as a tool')
