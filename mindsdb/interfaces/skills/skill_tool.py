import enum
from collections import defaultdict
from typing import List, Optional
from langchain_core.language_models import BaseChatModel
from langchain.embeddings.base import Embeddings

from mindsdb_sql.parser.ast import Select, BinaryOperation, Identifier, Constant, Star

from mindsdb.integrations.libs.vectordatabase_handler import TableField
from mindsdb.interfaces.storage import db
from mindsdb.utilities import log
from mindsdb.utilities.cache import get_cache

from .sql_agent import SQLAgent

_DEFAULT_TOP_K_SIMILARITY_SEARCH = 5
_DEFAULT_SQL_LLM_MODEL = 'gpt-3.5-turbo'
_MAX_CACHE_SIZE = 1000

logger = log.getLogger(__name__)


class SkillType(enum.Enum):
    TEXT2SQL_LEGACY = 'text2sql'
    TEXT2SQL = 'sql'
    KNOWLEDGE_BASE = 'knowledge_base'
    RETRIEVAL = 'retrieval'


class SkillToolController:
    def __init__(self):
        self.command_executor = None

    def get_command_executor(self):
        if self.command_executor is None:
            from mindsdb.api.executor.command_executor import ExecuteCommands
            from mindsdb.api.executor.controllers import SessionController  # Top-level import produces circular import in some cases TODO: figure out a fix without losing runtime improvements (context: see #9304)  # noqa

            sql_session = SessionController()
            sql_session.database = 'mindsdb'

            self.command_executor = ExecuteCommands(sql_session)
        return self.command_executor

    def get_sql_agent(
            self,
            database: str,
            include_tables: Optional[List[str]] = None,
            ignore_tables: Optional[List[str]] = None,
            sample_rows_in_table_info: int = 3,
    ):
        return SQLAgent(
            self.get_command_executor(),
            database,
            include_tables,
            ignore_tables,
            sample_rows_in_table_info,
            cache=get_cache('agent', max_size=_MAX_CACHE_SIZE)
        )

    def _make_text_to_sql_tools(self, skills: List[db.Skills], llm) -> list:
        '''
           Uses SQLAgent to execute tool
        '''
        # To prevent dependency on Langchain unless an actual tool uses it.
        try:
            from mindsdb.interfaces.agents.mindsdb_database_agent import MindsDBSQL
            from mindsdb.interfaces.skills.custom.text2sql.mindsdb_sql_toolkit import MindsDBSQLToolkit
            from langchain_community.tools.sql_database.tool import QuerySQLDataBaseTool
        except ImportError:
            raise ImportError(
                'To use the text-to-SQL skill, please install langchain with `pip install mindsdb[langchain]`')

        tables_list = []
        for skill in skills:
            database = skill.params['database']
            for table in skill.params['tables']:
                tables_list.append(f'{database}.{table}')

        # use list databases
        database = ','.join(set(s.params['database'] for s in skills))
        db = MindsDBSQL(
            engine=self.get_command_executor(),
            database=database,
            metadata=self.get_command_executor().session.integration_controller,
            include_tables=tables_list
        )

        # Users probably don't need to configure this for now.
        sql_database_tools = MindsDBSQLToolkit(db=db, llm=llm).get_tools()
        descriptions = []
        for skill in skills:
            description = skill.params.get('description', '')
            if description:
                descriptions.append(description)

        for i, tool in enumerate(sql_database_tools):
            if isinstance(tool, QuerySQLDataBaseTool):
                # Add our own custom description so our agent knows when to query this table.
                tool.description = (
                    f'Use this tool if you need data about {" OR ".join(descriptions)}. '
                    'Use the conversation context to decide which table to query. '
                    f'These are the available tables: {",".join(tables_list)}.\n'
                    f'ALWAYS consider these special cases:\n'
                    f'- For TIMESTAMP type columns, make sure you include the time portion in your query (e.g. WHERE date_column = "2020-01-01 12:00:00")'
                    f'Here are the rest of the instructions:\n'
                    f'{tool.description}'
                )
                sql_database_tools[i] = tool
        return sql_database_tools

    def _make_retrieval_tools(self, skill: db.Skills, llm, embedding_model):
        """
        creates advanced retrieval tool i.e. RAG
        """
        params = skill.params
        config = params.get('config', {})
        if 'llm' not in config:
            # Set LLM if not explicitly provided in configs.
            config['llm'] = llm
        tool = dict(
            name=params.get('name', skill.name),
            source=params.get('source', None),
            config=config,
            description=f'You must use this tool to get more context or information '
                        f'to answer a question about {params["description"]}. '
                        f'The input should be the exact question the user is asking.',
            type=skill.type
        )
        pred_args = {}
        pred_args['embedding_model'] = embedding_model
        pred_args['llm'] = llm

        from .retrieval_tool import build_retrieval_tool
        return build_retrieval_tool(tool, pred_args, skill)

    def _get_rag_query_function(self, skill: db.Skills):

        session_controller = self.get_command_executor().session

        def _answer_question(question: str) -> str:
            knowledge_base_name = skill.params['source']

            # make select in KB table
            query = Select(
                targets=[Star()],
                where=BinaryOperation(op='=', args=[
                    Identifier(TableField.CONTENT.value), Constant(question)
                ]),
                limit=Constant(_DEFAULT_TOP_K_SIMILARITY_SEARCH),
            )
            kb_table = session_controller.kb_controller.get_table(knowledge_base_name, skill.project_id)

            res = kb_table.select_query(query)
            return '\n'.join(res.content)

        return _answer_question

    def _make_knowledge_base_tools(self, skill: db.Skills) -> dict:
        # To prevent dependency on Langchain unless an actual tool uses it.
        description = skill.params.get('description', '')

        logger.warning("This skill is deprecated and will be removed in the future. "
                       "Please use `retrieval` skill instead ")

        return dict(
            name='Knowledge Base Retrieval',
            func=self._get_rag_query_function(skill),
            description=f'Use this tool to get more context or information to answer a question about {description}. The input should be the exact question the user is asking.',
            type=skill.type
        )

    def get_tools_from_skills(self, skills: List[db.Skills], llm: BaseChatModel, embedding_model: Embeddings) -> dict:
        """
            Creates function for skill and metadata (name, description)
        Args:
            skills (Skills): Skills to make a tool from
            llm: LLM which will be used by skills
            embedding_model: this model is used by retrieval skill

        Returns:
            dict with keys: name, description, func
        """

        # group skills by type
        skills_group = defaultdict(list)
        for skill in skills:
            try:
                skill_type = SkillType(skill.type)
            except ValueError:
                raise NotImplementedError(
                    f'skill of type {skill.type} is not supported as a tool, supported types are: {list(SkillType._member_names_)}')

            if skill_type == SkillType.TEXT2SQL_LEGACY:
                skill_type = SkillType.TEXT2SQL
            skills_group[skill_type].append(skill)

        tools = {}
        for skill_type, skills in skills_group.items():
            if skill_type == SkillType.TEXT2SQL:
                tools[skill_type] = self._make_text_to_sql_tools(skills, llm)
            if skill_type == SkillType.KNOWLEDGE_BASE:
                tools[skill_type] = [
                    self._make_knowledge_base_tools(skill)
                    for skill in skills
                ]
            if skill_type == SkillType.RETRIEVAL:
                tools[skill_type] = [
                    self._make_retrieval_tools(skill, llm, embedding_model)
                    for skill in skills
                ]
        return tools


skill_tool = SkillToolController()
