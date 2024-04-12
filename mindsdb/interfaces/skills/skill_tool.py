import enum
from typing import List, Optional

from mindsdb_sql.parser.ast import Select, BinaryOperation, Identifier, Constant, Star

from mindsdb.integrations.libs.vectordatabase_handler import TableField
from mindsdb.interfaces.storage import db
from .sql_agent import SQLAgent

_DEFAULT_TOP_K_SIMILARITY_SEARCH = 5


class SkillType(enum.Enum):
    TEXT2SQL = 'text2sql'
    KNOWLEDGE_BASE = 'knowledge_base'
    RETRIEVAL = 'retrieval'


class SkillToolController:
    def __init__(self):
        self.command_executor = None

    def get_command_executor(self):
        if self.command_executor is None:
            from mindsdb.api.executor.controllers import SessionController
            from mindsdb.api.executor.command_executor import ExecuteCommands

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
        )

    def _make_text_to_sql_tools(self, skill: db.Skills) -> dict:
        '''
           Uses SQLAgent to execute tool
        '''

        database = skill.params['database']
        tables = skill.params['tables']

        sql_agent = SQLAgent(
            self.get_command_executor(),
            database,
            include_tables=tables
        )

        description = (
            "Use the conversation context to decide which table to query. "
            "Input to this tool is a detailed and correct SQL query, output is a result from the database. "
            "If the query is not correct, an error message will be returned. "
            "If an error is returned, rewrite the query, check the query, and try again. "
            f"These are the available tables: {','.join(tables)}\n"
        )
        for table in tables:
            description += f'Table name: "{table}", columns {sql_agent.get_table_columns(table)}\n'

        return dict(
            name='sql_db_query',
            func=sql_agent.query_safe,
            description=description,
            type=skill.type
        )

    def _make_retrieval_tools(self, skill: db.Skills) -> dict:
        """
        creates advanced retrieval tool i.e. RAG
        """
        params = skill.params
        return dict(
            name=params.get('name', skill.name),
            config=params.get('retriever_config', {}),
            description=f'You must use this tool to get more context or information '
                        f'to answer a question about {params["description"]}. '
                        f'The input should be the exact question the user is asking.',
            type=skill.type
        )

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

        return dict(
            name='Knowledge Base Retrieval',
            func=self._get_rag_query_function(skill),
            description=f'Use this tool to get more context or information to answer a question about {description}. The input should be the exact question the user is asking.',
            type=skill.type
        )

    def get_tool_from_skill(self, skill: db.Skills) -> dict:
        """
            Creates function for skill and metadata (name, description)
        Args:
            skill (Skills): Skill to make a tool from

        Returns:
            dict with keys: name, description, func
        """

        try:
            skill_type = SkillType(skill.type)
        except ValueError:
            raise NotImplementedError(
                f'skill of type {skill.type} is not supported as a tool, supported types are: {list(SkillType._member_names_)}')

        if skill_type == SkillType.TEXT2SQL:
            return self._make_text_to_sql_tools(skill)
        if skill_type == SkillType.KNOWLEDGE_BASE:
            return self._make_knowledge_base_tools(skill)
        if skill_type == SkillType.RETRIEVAL:
            return self._make_retrieval_tools(skill)


skill_tool = SkillToolController()
