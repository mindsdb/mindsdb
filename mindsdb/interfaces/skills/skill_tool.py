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
from mindsdb.utilities.config import config
from mindsdb.interfaces.storage import db
from mindsdb.interfaces.skills.sql_agent import SQLAgent
from mindsdb.integrations.libs.vectordatabase_handler import TableField
from mindsdb.interfaces.agents.constants import DEFAULT_TEXT2SQL_DATABASE

_DEFAULT_TOP_K_SIMILARITY_SEARCH = 5
_MAX_CACHE_SIZE = 1000

logger = log.getLogger(__name__)


class SkillType(enum.Enum):
    TEXT2SQL_LEGACY = "text2sql"
    TEXT2SQL = "sql"
    KNOWLEDGE_BASE = "knowledge_base"
    RETRIEVAL = "retrieval"


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
                    table_name = x["table"]
                    schema_name = x.get("schema")
                else:
                    raise ValueError(f"Unexpected value in tables list: {x}")
                agent_tables_map[schema_name].add(table_name)
            return agent_tables_map

        agent_tables_map = list_to_map(self.agent_tables_list or [])
        skill_tables_map = list_to_map(self.params.get("tables", []))

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
            from mindsdb.api.executor.controllers import (
                SessionController,
            )  # Top-level import produces circular import in some cases TODO: figure out a fix without losing runtime improvements (context: see #9304)  # noqa

            sql_session = SessionController()
            sql_session.database = config.get("default_project")

            self.command_executor = ExecuteCommands(sql_session)
        return self.command_executor

    def _make_text_to_sql_tools(self, skills: List[db.Skills], llm) -> List:
        """
        Uses SQLAgent to execute tool
        """
        # To prevent dependency on Langchain unless an actual tool uses it.
        try:
            from mindsdb.interfaces.agents.mindsdb_database_agent import MindsDBSQL
            from mindsdb.interfaces.skills.custom.text2sql.mindsdb_sql_toolkit import MindsDBSQLToolkit
        except ImportError:
            raise ImportError(
                "To use the text-to-SQL skill, please install langchain with `pip install mindsdb[langchain]`"
            )

        command_executor = self.get_command_executor()

        def escape_table_name(name: str) -> str:
            name = name.strip(" `")
            return f"`{name}`"

        tables_list = []
        knowledge_bases_list = []
        ignore_knowledge_bases_list = []

        # Track databases extracted from dot notation
        extracted_databases = set()

        # Initialize knowledge_base_database with default value
        knowledge_base_database = DEFAULT_TEXT2SQL_DATABASE  # Default to mindsdb project

        # First pass: collect all database and knowledge base parameters
        for skill in skills:
            # Update knowledge_base_database if specified in any skill
            if skill.params.get("knowledge_base_database"):
                knowledge_base_database = skill.params.get("knowledge_base_database")

            # Extract databases from include_tables with dot notation
            if skill.params.get("include_tables"):
                include_tables = skill.params.get("include_tables")
                if isinstance(include_tables, str):
                    include_tables = [t.strip() for t in include_tables.split(",")]

                # Extract database names from dot notation
                for table in include_tables:
                    if "." in table:
                        db_name = table.split(".")[0]
                        extracted_databases.add(db_name)

            # Extract databases from include_knowledge_bases with dot notation
            if skill.params.get("include_knowledge_bases"):
                include_kbs = skill.params.get("include_knowledge_bases")
                if isinstance(include_kbs, str):
                    include_kbs = [kb.strip() for kb in include_kbs.split(",")]

                # Extract database names from dot notation
                for kb in include_kbs:
                    if "." in kb:
                        db_name = kb.split(".")[0]
                        if db_name != knowledge_base_database:
                            # Only update if it's different from the default
                            knowledge_base_database = db_name

        # Second pass: collect all tables and knowledge base restrictions
        for skill in skills:
            # Get database for tables (this is an actual database connection)
            database = skill.params.get("database", DEFAULT_TEXT2SQL_DATABASE)

            # Add databases extracted from dot notation if no explicit database is provided
            if not database and extracted_databases:
                # Use the first extracted database if no explicit database is provided
                database = next(iter(extracted_databases))
                # Update the skill params with the extracted database
                skill.params["database"] = database

            # Extract knowledge base restrictions if they exist in the skill params
            if skill.params.get("include_knowledge_bases"):
                # Convert to list if it's a string
                include_kbs = skill.params.get("include_knowledge_bases")
                if isinstance(include_kbs, str):
                    include_kbs = [kb.strip() for kb in include_kbs.split(",")]

                # Process each knowledge base name
                for kb in include_kbs:
                    # If it doesn't have a dot, prefix it with the knowledge_base_database
                    if "." not in kb:
                        knowledge_bases_list.append(f"{knowledge_base_database}.{kb}")
                    else:
                        knowledge_bases_list.append(kb)

            # Collect ignore_knowledge_bases
            if skill.params.get("ignore_knowledge_bases"):
                # Convert to list if it's a string
                ignore_kbs = skill.params.get("ignore_knowledge_bases")
                if isinstance(ignore_kbs, str):
                    ignore_kbs = [kb.strip() for kb in ignore_kbs.split(",")]

                # Process each knowledge base name to ignore
                for kb in ignore_kbs:
                    # If it doesn't have a dot, prefix it with the knowledge_base_database
                    if "." not in kb:
                        ignore_knowledge_bases_list.append(f"{knowledge_base_database}.{kb}")
                    else:
                        ignore_knowledge_bases_list.append(kb)

            # Skip if no database specified
            if not database:
                continue

            # Process include_tables with dot notation
            if skill.params.get("include_tables"):
                include_tables = skill.params.get("include_tables")
                if isinstance(include_tables, str):
                    include_tables = [t.strip() for t in include_tables.split(",")]

                for table in include_tables:
                    # If table already has a database prefix, use it as is
                    if "." in table:
                        # Check if the table already has backticks
                        if "`" in table:
                            tables_list.append(table)
                        else:
                            # Apply escape_table_name only to the table part
                            parts = table.split(".")
                            if len(parts) == 2:
                                # Format: database.table
                                tables_list.append(f"{parts[0]}.{escape_table_name(parts[1])}")
                            elif len(parts) == 3:
                                # Format: database.schema.table
                                tables_list.append(f"{parts[0]}.{parts[1]}.{escape_table_name(parts[2])}")
                            else:
                                # Unusual format, escape the whole thing
                                tables_list.append(escape_table_name(table))
                    else:
                        # Otherwise, prefix with the database
                        tables_list.append(f"{database}.{escape_table_name(table)}")

                # Skip further table processing if include_tables is specified
                continue

            restriction_on_tables = skill.restriction_on_tables

            if restriction_on_tables is None and database:
                try:
                    handler = command_executor.session.integration_controller.get_data_handler(database)
                    if "all" in inspect.signature(handler.get_tables).parameters:
                        response = handler.get_tables(all=True)
                    else:
                        response = handler.get_tables()
                    # no restrictions
                    columns = [c.lower() for c in response.data_frame.columns]
                    name_idx = columns.index("table_name") if "table_name" in columns else 0

                    if "table_schema" in response.data_frame.columns:
                        for _, row in response.data_frame.iterrows():
                            tables_list.append(f"{database}.{row['table_schema']}.{escape_table_name(row[name_idx])}")
                    else:
                        for table_name in response.data_frame.iloc[:, name_idx]:
                            tables_list.append(f"{database}.{escape_table_name(table_name)}")
                except Exception as e:
                    logger.warning(f"Could not get tables from database {database}: {str(e)}")
                continue

            # Handle table restrictions
            if restriction_on_tables and database:
                for schema_name, tables in restriction_on_tables.items():
                    for table in tables:
                        # Check if the table already has dot notation (e.g., 'postgresql_conn.home_rentals')
                        if "." in table:
                            # Table already has database prefix, add it directly
                            tables_list.append(escape_table_name(table))
                        else:
                            # No dot notation, apply schema and database as needed
                            if schema_name is None:
                                tables_list.append(f"{database}.{escape_table_name(table)}")
                            else:
                                tables_list.append(f"{database}.{schema_name}.{escape_table_name(table)}")
                continue

        # Remove duplicates from lists
        tables_list = list(set(tables_list))
        knowledge_bases_list = list(set(knowledge_bases_list))
        ignore_knowledge_bases_list = list(set(ignore_knowledge_bases_list))

        # Determine knowledge base parameters to pass to SQLAgent
        include_knowledge_bases = knowledge_bases_list if knowledge_bases_list else None
        ignore_knowledge_bases = ignore_knowledge_bases_list if ignore_knowledge_bases_list else None

        # If both include and ignore lists exist, include takes precedence
        if include_knowledge_bases:
            ignore_knowledge_bases = None

        # # Get all databases from skills and extracted databases
        # all_databases = list(set([s.params.get('database', DEFAULT_TEXT2SQL_DATABASE) for s in skills if s.params.get('database')] + list(extracted_databases)))
        #
        #
        # # If no databases were specified or extracted, use 'mindsdb' as a default
        # if not all_databases:
        #     all_databases = [DEFAULT_TEXT2SQL_DATABASE]
        #

        all_databases = []
        # Filter out None values
        all_databases = [db for db in all_databases if db is not None]

        # Create a databases_struct dictionary that includes all extracted databases
        databases_struct = {}

        # First, add databases from skills with explicit database parameters
        for skill in skills:
            if skill.params.get("database"):
                databases_struct[skill.params["database"]] = skill.restriction_on_tables

        # Then, add all extracted databases with no restrictions
        for db_name in extracted_databases:
            if db_name not in databases_struct:
                databases_struct[db_name] = None

        sql_agent = SQLAgent(
            command_executor=command_executor,
            databases=all_databases,
            databases_struct=databases_struct,
            include_tables=tables_list,
            ignore_tables=None,
            include_knowledge_bases=include_knowledge_bases,
            ignore_knowledge_bases=ignore_knowledge_bases,
            knowledge_base_database=knowledge_base_database,
            sample_rows_in_table_info=3,
            cache=get_cache("agent", max_size=_MAX_CACHE_SIZE),
        )
        db = MindsDBSQL.custom_init(sql_agent=sql_agent)
        should_include_kb_tools = include_knowledge_bases is not None and len(include_knowledge_bases) > 0
        should_include_tables_tools = len(databases_struct) > 0 or len(tables_list) > 0
        toolkit = MindsDBSQLToolkit(
            db=db,
            llm=llm,
            include_tables_tools=should_include_tables_tools,
            include_knowledge_base_tools=should_include_kb_tools,
        )
        return toolkit.get_tools()

    def _make_retrieval_tools(self, skill: db.Skills, llm, embedding_model):
        """
        creates advanced retrieval tool i.e. RAG
        """
        params = skill.params
        config = params.get("config", {})
        if "llm" not in config:
            # Set LLM if not explicitly provided in configs.
            config["llm"] = llm
        tool = dict(
            name=params.get("name", skill.name),
            source=params.get("source", None),
            config=config,
            description=f"You must use this tool to get more context or information "
            f"to answer a question about {params['description']}. "
            f"The input should be the exact question the user is asking.",
            type=skill.type,
        )
        pred_args = {}
        pred_args["llm"] = llm

        from .retrieval_tool import build_retrieval_tools

        return build_retrieval_tools(tool, pred_args, skill)

    def _get_rag_query_function(self, skill: db.Skills):
        session_controller = self.get_command_executor().session

        def _answer_question(question: str) -> str:
            knowledge_base_name = skill.params["source"]

            # make select in KB table
            query = Select(
                targets=[Star()],
                where=BinaryOperation(op="=", args=[Identifier(TableField.CONTENT.value), Constant(question)]),
                limit=Constant(_DEFAULT_TOP_K_SIMILARITY_SEARCH),
            )
            kb_table = session_controller.kb_controller.get_table(knowledge_base_name, skill.project_id)

            res = kb_table.select_query(query)
            # Handle both chunk_content and content column names
            if hasattr(res, "chunk_content"):
                return "\n".join(res.chunk_content)
            elif hasattr(res, "content"):
                return "\n".join(res.content)
            else:
                return "No content or chunk_content found in knowledge base response"

        return _answer_question

    def _make_knowledge_base_tools(self, skill: db.Skills) -> dict:
        # To prevent dependency on Langchain unless an actual tool uses it.
        description = skill.params.get("description", "")

        logger.warning(
            "This skill is deprecated and will be removed in the future. Please use `retrieval` skill instead "
        )

        return dict(
            name="Knowledge Base Retrieval",
            func=self._get_rag_query_function(skill),
            description=f"Use this tool to get more context or information to answer a question about {description}. The input should be the exact question the user is asking.",
            type=skill.type,
        )

    def get_tools_from_skills(
        self, skills_data: List[SkillData], llm: BaseChatModel, embedding_model: Embeddings
    ) -> dict:
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
                    f"skill of type {skill.type} is not supported as a tool, supported types are: {list(SkillType._member_names_)}"
                )

            if skill_type == SkillType.TEXT2SQL_LEGACY:
                skill_type = SkillType.TEXT2SQL
            skills_group[skill_type].append(skill)

        tools = {}
        for skill_type, skills in skills_group.items():
            if skill_type == SkillType.TEXT2SQL:
                tools[skill_type] = self._make_text_to_sql_tools(skills, llm)
            elif skill_type == SkillType.KNOWLEDGE_BASE:
                tools[skill_type] = [self._make_knowledge_base_tools(skill) for skill in skills]
            elif skill_type == SkillType.RETRIEVAL:
                tools[skill_type] = []
                for skill in skills:
                    tools[skill_type] += self._make_retrieval_tools(skill, llm, embedding_model)
        return tools


skill_tool = SkillToolController()
