import tiktoken

from typing import Callable, Dict

from langchain.tools.retriever import create_retriever_tool
from mindsdb_sql import parse_sql, Insert

from langchain.prompts import PromptTemplate
from langchain.agents import load_tools, Tool

from langchain_experimental.utilities import PythonREPL
from langchain_community.utilities import GoogleSerperAPIWrapper

from langchain.chains.llm import LLMChain
from langchain.text_splitter import CharacterTextSplitter
from langchain.chains.combine_documents.stuff import StuffDocumentsChain
from langchain.chains import ReduceDocumentsChain, MapReduceDocumentsChain

from mindsdb.integrations.utilities.rag.rag_pipeline_builder import RAG
from mindsdb.integrations.utilities.rag.settings import RAGPipelineModel, VectorStoreType, DEFAULT_COLLECTION_NAME
from mindsdb.interfaces.skills.skill_tool import skill_tool, SkillType
from mindsdb.interfaces.storage import db
from mindsdb.utilities import log

logger = log.getLogger(__name__)
from mindsdb.interfaces.storage.db import KnowledgeBase
from mindsdb.utilities import log

logger = log.getLogger(__name__)

# Individual tools
# Note: all tools are defined in a closure to pass required args (apart from LLM input) through it, as custom tools don't allow custom field assignment.  # noqa
def get_exec_call_tool(llm, executor, model_kwargs) -> Callable:
    def mdb_exec_call_tool(query: str) -> str:
        try:
            ast_query = parse_sql(query.strip('`'), dialect='mindsdb')
            ret = executor.execute_command(ast_query)
            if ret.data is None and ret.error_code is None:
                return ''
            data = ret.data  # list of lists
            data = '\n'.join([  # rows
                '\t'.join(      # columns
                    str(row) if isinstance(row, str) else [str(value) for value in row]
                ) for row in data
            ])
        except Exception as e:
            data = f"mindsdb tool failed with error:\n{str(e)}"   # let the agent know

        # summarize output if needed
        data = summarize_if_overflowed(data, llm, model_kwargs['max_tokens'])

        return data
    return mdb_exec_call_tool

def get_exec_metadata_tool(llm, executor, model_kwargs) -> Callable:
    def mdb_exec_metadata_call(query: str) -> str:
        try:
            parts = query.replace('`', '').split('.')
            assert 1 <= len(parts) <= 2, 'query must be in the format: `integration` or `integration.table`'

            integration = parts[0]
            integrations = executor.session.integration_controller
            handler = integrations.get_data_handler(integration)

            if len(parts) == 1:
                df = handler.get_tables().data_frame
                data = f'The integration `{integration}` has {df.shape[0]} tables: {", ".join(list(df["TABLE_NAME"].values))}'  # noqa

            if len(parts) == 2:
                df = handler.get_tables().data_frame
                table_name = parts[-1]
                try:
                    table_name_col = 'TABLE_NAME' if 'TABLE_NAME' in df.columns else 'table_name'
                    mdata = df[df[table_name_col] == table_name].iloc[0].to_list()
                    if len(mdata) == 3:
                        _, nrows, table_type = mdata
                        data = f'Metadata for table {table_name}:\n\tRow count: {nrows}\n\tType: {table_type}\n'
                    elif len(mdata) == 2:
                        nrows = mdata
                        data = f'Metadata for table {table_name}:\n\tRow count: {nrows}\n'
                    else:
                        data = f'Metadata for table {table_name}:\n'
                    fields = handler.get_columns(table_name).data_frame['Field'].to_list()
                    types = handler.get_columns(table_name).data_frame['Type'].to_list()
                    data += f'List of columns and types:\n'
                    data += '\n'.join([f'\tColumn: `{field}`\tType: `{typ}`' for field, typ in zip(fields, types)])
                except:
                    data = f'Table {table_name} not found.'
        except Exception as e:
            data = f"mindsdb tool failed with error:\n{str(e)}"  # let the agent know

        # summarize output if needed
        data = summarize_if_overflowed(data, llm, model_kwargs['max_tokens'])

        return data
    return mdb_exec_metadata_call

def get_mdb_write_tool(executor) -> Callable:
    def mdb_write_call(query: str) -> str:
        try:
            query = query.strip('`')
            ast_query = parse_sql(query.strip('`'), dialect='mindsdb')
            if isinstance(ast_query, Insert):
                _ = executor.execute_command(ast_query)
                return "mindsdb write tool executed successfully"
        except Exception as e:
            return f"mindsdb write tool failed with error:\n{str(e)}"
    return mdb_write_call

def _setup_standard_tools(tools, llm, model_kwargs):
    executor = skill_tool.get_command_executor()

    all_standard_tools = []
    langchain_tools = []
    for tool in tools:
        if tool == 'mindsdb_read':
            mdb_tool = Tool(
                name="MindsDB",
                func=get_exec_call_tool(llm, executor, model_kwargs),
                description="useful to read from databases or tables connected to the mindsdb machine learning package. the action must be a valid simple SQL query, always ending with a semicolon. For example, you can do `show databases;` to list the available data sources, and `show tables;` to list the available tables within each data source."  # noqa
            )

            mdb_meta_tool = Tool(
                name="MDB-Metadata",
                func=get_exec_metadata_tool(llm, executor, model_kwargs),
                description="useful to get column names from a mindsdb table or metadata from a mindsdb data source. the command should be either 1) a data source name, to list all available tables that it exposes, or 2) a string with the format `data_source_name.table_name` (for example, `files.my_table`), to get the table name, table type, column names, data types per column, and amount of rows of the specified table."  # noqa
            )
            all_standard_tools.append(mdb_tool)
            all_standard_tools.append(mdb_meta_tool)
        if tool == 'mindsdb_write':
            mdb_write_tool = Tool(
                name="MDB-Write",
                func=get_mdb_write_tool(executor),
                description="useful to write into data sources connected to mindsdb. command must be a valid SQL query with syntax: `INSERT INTO data_source_name.table_name (column_name_1, column_name_2, [...]) VALUES (column_1_value_row_1, column_2_value_row_1, [...]), (column_1_value_row_2, column_2_value_row_2, [...]), [...];`. note the command always ends with a semicolon. order of column names and values for each row must be a perfect match. If write fails, try casting value with a function, passing the value without quotes, or truncating string as needed.`."  # noqa
            )
            all_standard_tools.append(mdb_write_tool)
        elif tool == 'python_repl':
            tool = Tool(
                name="python_repl",
                func=PythonREPL().run,
                description="useful for running custom Python code. Note: this is a powerful tool, so use with caution."  # noqa
            )
            langchain_tools.append(tool)
        elif tool == 'serper':
            search = GoogleSerperAPIWrapper()
            tool = Tool(
                name="Intermediate Answer",
                func=search.run,
                description="useful for when you need to ask with search",
            )
            langchain_tools.append(tool)
        else:
            raise ValueError(f"Unsupported tool: {tool}")

    if langchain_tools:
        all_standard_tools += load_tools(langchain_tools)
    return all_standard_tools


def _get_rag_params(pred_args: Dict) -> Dict:
    model_config = pred_args.copy()

    supported_rag_params = RAGPipelineModel.get_field_names()

    rag_params = {k: v for k, v in model_config.items() if k in supported_rag_params}

    return rag_params


def _create_conn_string(connection_args: dict) -> str:
    """
    Creates a PostgreSQL connection string from connection args.
    """
    user = connection_args.get('user')
    host = connection_args.get('host')
    port = connection_args.get('port')
    password = connection_args.get('password')
    dbname = connection_args.get('database')

    if password:
        return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    else:
        return f"postgresql://{user}@{host}:{port}/{dbname}"


def _get_knowledge_base(knowledge_base_name: str, project_id, executor) -> db.KnowledgeBase:

    kb = executor.session.kb_controller.get(knowledge_base_name, project_id)

    return kb


def _build_vector_store_config_from_knowledge_base(rag_params: Dict, knowledge_base: KnowledgeBase, executor) -> Dict:
    """
    build vector store config from knowledge base
    """

    vector_store_config = rag_params['vector_store_config'].copy()

    vector_store_type = knowledge_base.vector_database.engine
    vector_store_config['vector_store_type'] = vector_store_type

    if vector_store_type == VectorStoreType.CHROMA.value:
        # For chromadb used, we get persist_directory
        vector_store_folder_name = knowledge_base.vector_database.data['persist_directory']
        integration_handler = executor.session.integration_controller.get_data_handler(
            knowledge_base.vector_database.name
        )
        persist_dir = integration_handler.handler_storage.folder_get(vector_store_folder_name)
        vector_store_config['persist_directory'] = persist_dir

    elif vector_store_type == VectorStoreType.PGVECTOR.value:
        # For pgvector, we get connection string
        #todo requires further testing
        connection_params = knowledge_base.vector_database.data
        vector_store_config['connection_string'] = _create_conn_string(connection_params)

    else:
        raise ValueError(f"Invalid vector store type: {vector_store_type}. "
                         f"Only {[v.name for v in VectorStoreType]} are currently supported.")

    return vector_store_config


def _build_retrieval_tool(tool: dict, pred_args: dict, skill: db.Skills):
    """
    Builds a retrieval tool i.e RAG
    """
    # build RAG config

    tools_config = tool['config']

    # we update the config with the pred_args to allow for custom config
    tools_config.update(pred_args)

    rag_params = _get_rag_params(tools_config)

    if 'vector_store_config' not in rag_params:
        rag_params['vector_store_config'] = {}
        logger.warning(f'No collection_name specified for the retrieval tool, '
                       f"using default collection_name: '{DEFAULT_COLLECTION_NAME}'"
                       f'\nWarning: If this collection does not exist, no data will be retrieved')

    if 'source' in tool:
        kb_name = tool['source']
        executor = skill_tool.get_command_executor()
        kb = _get_knowledge_base(kb_name, skill.project_id, executor)

        if not kb:
            raise ValueError(f"Knowledge base not found: {kb_name}")

        rag_params['vector_store_config'] = _build_vector_store_config_from_knowledge_base(rag_params, kb, executor)

    # Can run into weird validation errors when unpacking rag_params directly into constructor.
    rag_config = RAGPipelineModel(
        embeddings_model=rag_params['embeddings_model']
    )
    if 'documents' in rag_params:
        rag_config.documents = rag_params['documents']
    if 'vector_store_config' in rag_params:
        rag_config.vector_store_config = rag_params['vector_store_config']
    if 'db_connection_string' in rag_params:
        rag_config.db_connection_string = rag_params['db_connection_string']
    if 'table_name' in rag_params:
        rag_config.table_name = rag_params['table_name']
    if 'llm' in rag_params:
        rag_config.llm = rag_params['llm']
    if 'rag_prompt_template' in rag_params:
        rag_config.rag_prompt_template = rag_params['rag_prompt_template']
    if 'retriever_prompt_template' in rag_params:
        rag_config.retriever_prompt_template = rag_params['retriever_prompt_template']

    # build retriever
    rag_pipeline = RAG(rag_config)

    # create RAG tool
    return Tool(
        func=rag_pipeline,
        name=tool['name'],
        description=tool['description']
    )


def langchain_tools_from_skill(skill, pred_args, llm):
    # Makes Langchain compatible tools from a skill
    tools = skill_tool.get_tools_from_skill(skill, llm)

    all_tools = []
    for tool in tools:
        if skill.type == SkillType.RETRIEVAL.value:
            all_tools.append(_build_retrieval_tool(tool, pred_args, skill))
            continue
        if isinstance(tool, dict):
            all_tools.append(Tool(
                name=tool['name'],
                func=tool['func'],
                description=tool['description'],
                return_direct=True
            ))
            continue
        all_tools.append(tool)
    return all_tools

def get_skills(pred_args):
    return pred_args.get('skills', [])

# Collector
def setup_tools(llm, model_kwargs, pred_args, default_agent_tools):

    toolkit = pred_args['tools'] if pred_args.get('tools') is not None else default_agent_tools

    standard_tools = []
    function_tools = []

    for tool in toolkit:
        if isinstance(tool, str):
            standard_tools.append(tool)
        else:
            # user defined custom functions
            function_tools.append(tool)

    tools = []
    skills = get_skills(pred_args)
    for skill in skills:
        tools += langchain_tools_from_skill(skill, pred_args, llm)

    if len(tools) == 0:
        tools = _setup_standard_tools(standard_tools, llm, model_kwargs)

    if model_kwargs.get('serper_api_key', False):
        search = GoogleSerperAPIWrapper(serper_api_key=model_kwargs.pop('serper_api_key'))
        tools.append(Tool(
            name="Intermediate Answer (serper.dev)",
            func=search.run,
            description="useful for when you need to search the internet (note: in general, use this as a last resort)"  # noqa
        ))

    for tool in function_tools:
        tools.append(Tool(
            name=tool['name'],
            func=tool['func'],
            description=tool['description'],
        ))

    return tools


# Helpers
def summarize_if_overflowed(data, llm, max_tokens, budget_multiplier=0.8) -> str:
    """
        This helper retries with a summarized version of the
        output if the previous call fails due to the token limit being exceeded.

        We trigger summarization when the token count exceeds the limit times a multiplier to be conservative.
    """
    # tokenize data for length check
    # note: this is a rough estimate, as the tokenizer used in each LLM may be different
    encoding = tiktoken.get_encoding("gpt2")
    n_tokens = len(encoding.encode(data))

    # map-reduce given token budget
    if n_tokens > max_tokens * budget_multiplier:
        # map
        map_template = """The following is a set of documents
                {docs}
                Based on this list of docs, please identify the main themes
                Helpful Answer:"""
        map_prompt = PromptTemplate.from_template(map_template)
        map_chain = LLMChain(llm=llm, prompt=map_prompt)

        # reduce
        reduce_template = """The following is set of summaries:
                {doc_summaries}
                Take these and distill it into a final, consolidated summary of the main themes.
                Helpful Answer:"""
        reduce_prompt = PromptTemplate.from_template(reduce_template)
        reduce_chain = LLMChain(llm=llm, prompt=reduce_prompt)
        combine_documents_chain = StuffDocumentsChain(
            llm_chain=reduce_chain, document_variable_name="doc_summaries"
        )
        reduce_documents_chain = ReduceDocumentsChain(
            combine_documents_chain=combine_documents_chain,
            collapse_documents_chain=combine_documents_chain,
            token_max=max_tokens * budget_multiplier,  # applies for each group of documents
        )
        map_reduce_chain = MapReduceDocumentsChain(
            llm_chain=map_chain,
            reduce_documents_chain=reduce_documents_chain,
            document_variable_name="docs",
            return_intermediate_steps=False,
        )
        # split
        text_splitter = CharacterTextSplitter.from_tiktoken_encoder(chunk_size=1000, chunk_overlap=0)
        docs = text_splitter.create_documents([data])
        split_docs = text_splitter.split_documents(docs)

        # run chain
        data = map_reduce_chain.run(split_docs)
    return data
