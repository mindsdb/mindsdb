import enum
import inspect
from dataclasses import dataclass
from collections import defaultdict
from typing import List, Dict, Optional

from langchain_core.embeddings import Embeddings
from langchain_core.language_models import BaseChatModel
from mindsdb_sql_parser.ast import Select, BinaryOperation, Identifier, Constant, Star

from mindsdb.utilities import log
from mindsdb.utilities.cache import get_cache
from mindsdb.interfaces.storage import db
from mindsdb.interfaces.skills.sql_agent import SQLAgent
from mindsdb.integrations.libs.vectordatabase_handler import TableField


_DEFAULT_TOP_K_SIMILARITY_SEARCH = 5
_MAX_CACHE_SIZE = 1000

logger = log.getLogger(__name__)


class SkillType(enum.Enum):
    TEXT2SQL_LEGACY = 'text2sql'
    TEXT2SQL = 'sql'
    KNOWLEDGE_BASE = 'knowledge_base'
    RETRIEVAL = 'retrieval'


@dataclass
class SkillData:
    """Storage for skill's data

    Attributes:
        name (str): name of the skill
        type (str): skill's type (SkillType)
        params (dict): skill's attributes
        project_id (int): id of the project
        agent_tables_list (Optional[List[str]]): the restriction on available tables for an agent using the skill
    """
    name: str
    type: str
    params: dict
    project_id: int
    agent_tables_list: Optional[List[str]]

    @property
    def restriction_on_tables(self) -> Optional[Dict[str, set]]:
        """Schemas and tables which agent+skill may use. The result is intersections of skill's and agent's tables lists.

        Returns:
            Optional[Dict[str, set]]: allowed schemas and tables. Schemas - are keys in dict, tables - are values.
                if result is None, then there are no restrictions

        Raises:
            ValueError: if there is no intersection between skill's and agent's list.
                This means that all tables restricted for use.
        """
        def list_to_map(input: List) -> Dict:
            agent_tables_map = defaultdict(set)
            for x in input:
                if isinstance(x, str):
                    table_name = x
                    schema_name = None
                elif isinstance(x, dict):
                    table_name = x['table']
                    schema_name = x.get('schema')
                else:
                    raise ValueError(f'Unexpected value in tables list: {x}')
                agent_tables_map[schema_name].add(table_name)
            return agent_tables_map

        agent_tables_map = list_to_map(self.agent_tables_list or [])
        skill_tables_map = list_to_map(self.params.get('tables', []))

        if len(agent_tables_map) > 0 and len(skill_tables_map) > 0:
            if len(set(agent_tables_map) & set(skill_tables_map)) == 0:
                raise ValueError("Skill's and agent's allowed tables list have no shared schemas.")

            intersection_tables_map = defaultdict(set)
            has_intersection = False
            for schema_name in agent_tables_map:
                if schema_name not in skill_tables_map:
                    continue
                intersection_tables_map[schema_name] = agent_tables_map[schema_name] & skill_tables_map[schema_name]
                if len(intersection_tables_map[schema_name]) > 0:
                    has_intersection = True
            if has_intersection is False:
                raise ValueError("Skill's and agent's allowed tables list have no shared tables.")
            return intersection_tables_map
        if len(skill_tables_map) > 0:
            return skill_tables_map
        if len(agent_tables_map) > 0:
            return agent_tables_map
        return None


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

    def _make_text_to_sql_tools(self, skills: List[db.Skills], llm) -> List:
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

        command_executor = self.get_command_executor()

        tables_list = []
        for skill in skills:
            database = skill.params['database']
            restriction_on_tables = skill.restriction_on_tables
            if restriction_on_tables is None:
                handler = command_executor.session.integration_controller.get_data_handler(database)
                if 'all' in inspect.signature(handler.get_tables).parameters:
                    response = handler.get_tables(all=True)
                else:
                    response = handler.get_tables()
                # no restrictions
                if 'table_schema' in response.data_frame.columns:
                    for _, row in response.data_frame.iterrows():
                        tables_list.append(f"{database}.{row['table_schema']}.{row['table_name']}")
                else:
                    for _, row in response.data_frame.iterrows():
                        tables_list.append(f"{database}.{row['table_name']}")
                continue
            for schema_name, tables in restriction_on_tables.items():
                for table in tables:
                    if schema_name is None:
                        tables_list.append(f'{database}.{table}')
                    else:
                        tables_list.append(f'{database}.{schema_name}.{table}')

        sql_agent = SQLAgent(
            command_executor=command_executor,
            databases=list(set(s.params['database'] for s in skills)),
            databases_struct={
                skill.params['database']: skill.restriction_on_tables
                for skill in skills
            },
            include_tables=tables_list,
            ignore_tables=None,
            sample_rows_in_table_info=3,
            cache=get_cache('agent', max_size=_MAX_CACHE_SIZE)
        )
        db = MindsDBSQL.custom_init(
            sql_agent=sql_agent
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
                original_description = tool.description
                tool.description = ''
                if len(descriptions) > 0:
                    tool.description += f'Use this tool if you need data about {" OR ".join(descriptions)}.\n'
                tool.description += 'Use the conversation context to decide which table to query.\n'
                if len(tables_list) > 0:
                    f'These are the available tables: {",".join(tables_list)}.\n'
                tool.description += (
                    'ALWAYS consider these special cases:\n'
                    ' - For TIMESTAMP type columns, make sure you include the time portion in your query (e.g. WHERE date_column = "2020-01-01 12:00:00")\n'
                    'Here are the rest of the instructions:\n'
                    f'{original_description}'
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

    def get_tools_from_skills(self, skills_data: List[SkillData], llm: BaseChatModel, embedding_model: Embeddings) -> dict:
        """Creates function for skill and metadata (name, description)

        Args:
            skills_data (List[SkillData]): Skills to make a tool from
            llm (BaseChatModel): LLM which will be used by skills
            embedding_model (Embeddings): this model is used by retrieval skill

        Returns:
            dict: with keys: name, description, func
        """

        # group skills by type
        skills_group = defaultdict(list)
        for skill in skills_data:
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
            elif skill_type == SkillType.KNOWLEDGE_BASE:
                tools[skill_type] = [
                    self._make_knowledge_base_tools(skill)
                    for skill in skills
                ]
            elif skill_type == SkillType.RETRIEVAL:
                tools[skill_type] = [
                    self._make_retrieval_tools(skill, llm, embedding_model)
                    for skill in skills
                ]
        return tools


skill_tool = SkillToolController()
