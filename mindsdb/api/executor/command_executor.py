import datetime
from pathlib import Path
from textwrap import dedent
from typing import Optional
from functools import reduce

import pandas as pd
from mindsdb_evaluator.accuracy.general import evaluate_accuracy
from mindsdb_sql import parse_sql
from mindsdb_sql.planner.utils import query_traversal
from mindsdb_sql.parser.ast import (
    Alter,
    ASTNode,
    BinaryOperation,
    CommitTransaction,
    Constant,
    CreateTable,
    Delete,
    Describe,
    DropDatabase,
    DropTables,
    DropView,
    Explain,
    Function,
    Identifier,
    Insert,
    NativeQuery,
    Operation,
    RollbackTransaction,
    Select,
    Set,
    Show,
    Star,
    StartTransaction,
    Union,
    Update,
    Use,
    Variable,
    Tuple,
)

# typed models
from mindsdb_sql.parser.dialects.mindsdb import (
    CreateAgent,
    CreateAnomalyDetectionModel,
    CreateChatBot,
    CreateDatabase,
    CreateJob,
    CreateKnowledgeBase,
    CreateMLEngine,
    CreatePredictor,
    CreateSkill,
    CreateTrigger,
    CreateView,
    DropAgent,
    DropChatBot,
    DropDatasource,
    DropJob,
    DropKnowledgeBase,
    DropMLEngine,
    DropPredictor,
    DropSkill,
    DropTrigger,
    Evaluate,
    FinetunePredictor,
    RetrainPredictor,
    UpdateAgent,
    UpdateChatBot,
    UpdateSkill
)

import mindsdb.utilities.profiler as profiler
from mindsdb.api.executor import Column, SQLQuery, ResultSet
from mindsdb.api.executor.data_types.answer import ExecuteAnswer
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import (
    CHARSET_NUMBERS,
    SERVER_VARIABLES,
    TYPES,
)

from .exceptions import (
    ExecutorException,
    BadDbError,
    NotSupportedYet,
    WrongArgumentError,
    TableNotExistError,
)
from mindsdb.api.executor.utilities.functions import download_file
from mindsdb.api.executor.utilities.sql import query_df
from mindsdb.integrations.libs.const import (
    HANDLER_CONNECTION_ARG_TYPE,
    PREDICTOR_STATUS,
)
from mindsdb.integrations.libs.response import HandlerStatusResponse
from mindsdb.interfaces.chatbot.chatbot_controller import ChatBotController
from mindsdb.interfaces.database.projects import ProjectController
from mindsdb.interfaces.jobs.jobs_controller import JobsController
from mindsdb.interfaces.model.functions import (
    get_model_record,
    get_model_records,
    get_predictor_integration,
)
from mindsdb.interfaces.query_context.context_controller import query_context_controller
from mindsdb.interfaces.triggers.triggers_controller import TriggersController
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.functions import mark_process, resolve_model_identifier, get_handler_install_message
from mindsdb.utilities.exception import EntityExistsError, EntityNotExistsError
from mindsdb.utilities import log
from mindsdb.api.mysql.mysql_proxy.utilities import ErParseError

logger = log.getLogger(__name__)


def _get_show_where(
    statement: ASTNode,
    from_name: Optional[str] = None,
    like_name: Optional[str] = None,
    initial: Optional[ASTNode] = None,
) -> ASTNode:
    """combine all possible show filters to single 'where' condition
    SHOW category [FROM name] [LIKE filter] [WHERE filter]

    Args:
        statement (ASTNode): 'show' query statement
        from_name (str): name of column for 'from' filter
        like_name (str): name of column for 'like' filter,
        initial (ASTNode): initial 'where' filter
    Returns:
        ASTNode: 'where' statemnt
    """
    where = []
    if initial is not None:
        where.append(initial)
    if statement.from_table is not None and from_name is not None:
        where.append(
            BinaryOperation(
                "=",
                args=[Identifier(from_name), Constant(statement.from_table.parts[-1])],
            )
        )
    if statement.like is not None and like_name is not None:
        where.append(
            BinaryOperation(
                "like", args=[Identifier(like_name), Constant(statement.like)]
            )
        )
    if statement.where is not None:
        where.append(statement.where)

    if len(where) > 0:
        return reduce(
            lambda prev, next: BinaryOperation("and", args=[prev, next]), where
        )
    return None


class ExecuteCommands:
    def __init__(self, session, context=None):
        if context is None:
            context = {}

        self.context = context
        self.session = session

        self.charset_text_type = CHARSET_NUMBERS["utf8_general_ci"]
        self.datahub = session.datahub

    @profiler.profile()
    def execute_command(self, statement, database_name: str = None) -> ExecuteAnswer:
        sql = None
        if isinstance(statement, ASTNode):
            sql = statement.to_string()
        sql_lower = sql.lower()

        if database_name is None:
            database_name = self.session.database

        if type(statement) is CreateDatabase:
            return self.answer_create_database(statement)
        elif type(statement) is CreateMLEngine:
            name = statement.name.parts[-1]

            return self.answer_create_ml_engine(
                name,
                handler=statement.handler,
                params=statement.params,
                if_not_exists=getattr(statement, "if_not_exists", False)
            )
        elif type(statement) is DropMLEngine:
            return self.answer_drop_ml_engine(statement)
        elif type(statement) is DropPredictor:
            return self.answer_drop_model(statement, database_name)

        elif type(statement) is DropTables:
            return self.answer_drop_tables(statement, database_name)
        elif type(statement) is DropDatasource or type(statement) is DropDatabase:
            return self.answer_drop_database(statement)
        elif type(statement) is Describe:
            # NOTE in sql 'describe table' is same as 'show columns'
            obj_type = statement.type

            if obj_type is None or obj_type.upper() in ('MODEL', 'PREDICTOR'):
                return self.answer_describe_predictor(statement.value, database_name)
            else:
                return self.answer_describe_object(obj_type.upper(), statement.value, database_name)

        elif type(statement) is RetrainPredictor:
            return self.answer_retrain_predictor(statement, database_name)
        elif type(statement) is FinetunePredictor:
            return self.answer_finetune_predictor(statement, database_name)
        elif type(statement) is Show:
            sql_category = statement.category.lower()
            if hasattr(statement, "modes"):
                if isinstance(statement.modes, list) is False:
                    statement.modes = []
                statement.modes = [x.upper() for x in statement.modes]
            if sql_category == "ml_engines":
                new_statement = Select(
                    targets=[Star()],
                    from_table=Identifier(parts=["information_schema", "ml_engines"]),
                    where=_get_show_where(statement, like_name="name"),
                )

                query = SQLQuery(new_statement, session=self.session, database=database_name)
                return self.answer_select(query)
            elif sql_category == "handlers":
                new_statement = Select(
                    targets=[Star()],
                    from_table=Identifier(parts=["information_schema", "handlers"]),
                    where=_get_show_where(statement, like_name="name"),
                )

                query = SQLQuery(new_statement, session=self.session, database=database_name)
                return self.answer_select(query)
            elif sql_category == "plugins":
                if statement.where is not None or statement.like:
                    raise ExecutorException(
                        "'SHOW PLUGINS' query should be used without filters"
                    )
                new_statement = Select(
                    targets=[Star()],
                    from_table=Identifier(parts=["information_schema", "PLUGINS"]),
                )
                query = SQLQuery(new_statement, session=self.session, database=database_name)
                return self.answer_select(query)
            elif sql_category in ("databases", "schemas"):
                new_statement = Select(
                    targets=[Identifier(parts=["NAME"], alias=Identifier("Database"))],
                    from_table=Identifier(parts=["information_schema", "DATABASES"]),
                    where=_get_show_where(statement, like_name="Database"),
                )

                if "FULL" in statement.modes:
                    new_statement.targets.extend(
                        [
                            Identifier(parts=["TYPE"], alias=Identifier("TYPE")),
                            Identifier(parts=["ENGINE"], alias=Identifier("ENGINE")),
                        ]
                    )

                query = SQLQuery(new_statement, session=self.session, database=database_name)
                return self.answer_select(query)
            elif sql_category in ("tables", "full tables"):
                schema = database_name or "mindsdb"
                if (
                    statement.from_table is not None
                    and statement.in_table is not None
                ):
                    raise ExecutorException(
                        "You have an error in your SQL syntax: 'from' and 'in' cannot be used together"
                    )

                if statement.from_table is not None:
                    schema = statement.from_table.parts[-1]
                    statement.from_table = None
                if statement.in_table is not None:
                    schema = statement.in_table.parts[-1]
                    statement.in_table = None

                table_types = [Constant(t) for t in ['MODEL', 'BASE TABLE', 'SYSTEM VIEW', 'VIEW']]
                where = BinaryOperation(
                    "and",
                    args=[
                        BinaryOperation("=", args=[Identifier("table_schema"), Constant(schema)]),
                        BinaryOperation("in", args=[Identifier("table_type"), Tuple(table_types)])
                    ]
                )

                new_statement = Select(
                    targets=[
                        Identifier(
                            parts=["table_name"],
                            alias=Identifier(f"Tables_in_{schema}"),
                        )
                    ],
                    from_table=Identifier(parts=["information_schema", "TABLES"]),
                    where=_get_show_where(
                        statement, like_name=f"Tables_in_{schema}", initial=where
                    ),
                )

                if "FULL" in statement.modes:
                    new_statement.targets.append(
                        Identifier(parts=["TABLE_TYPE"], alias=Identifier("Table_type"))
                    )

                query = SQLQuery(new_statement, session=self.session, database=database_name)
                return self.answer_select(query)
            elif sql_category in (
                "variables",
                "session variables",
                "session status",
                "global variables",
            ):
                new_statement = Select(
                    targets=[
                        Identifier(parts=["Variable_name"]),
                        Identifier(parts=["Value"]),
                    ],
                    from_table=Identifier(parts=["dataframe"]),
                    where=_get_show_where(statement, like_name="Variable_name"),
                )

                data = {}
                is_session = "session" in sql_category
                for var_name, var_data in SERVER_VARIABLES.items():
                    var_name = var_name.replace("@@", "")
                    if is_session and var_name.startswith("session.") is False:
                        continue
                    if var_name.startswith("session.") or var_name.startswith(
                        "GLOBAL."
                    ):
                        name = var_name.replace("session.", "").replace("GLOBAL.", "")
                        data[name] = var_data[0]
                    elif var_name not in data:
                        data[var_name] = var_data[0]

                df = pd.DataFrame(data.items(), columns=["Variable_name", "Value"])
                df2 = query_df(df, new_statement)

                return ExecuteAnswer(
                    data=ResultSet().from_df(df2, table_name="session_variables")
                )
            elif sql_category == "search_path":
                return ExecuteAnswer(
                    data=ResultSet(
                        columns=[
                            Column(name="search_path", table_name="search_path", type="str")
                        ],
                        values=[['"$user", public']]
                    )
                )
            elif "show status like 'ssl_version'" in sql_lower:
                return ExecuteAnswer(
                    data=ResultSet(
                        columns=[
                            Column(
                                name="Value", table_name="session_variables", type="str"
                            ),
                            Column(
                                name="Value", table_name="session_variables", type="str"
                            ),
                        ],
                        values=[["Ssl_version", "TLSv1.1"]],
                    )
                )
            elif sql_category in ("function status", "procedure status"):
                # SHOW FUNCTION STATUS WHERE Db = 'MINDSDB';
                # SHOW PROCEDURE STATUS WHERE Db = 'MINDSDB'
                # SHOW FUNCTION STATUS WHERE Db = 'MINDSDB' AND Name LIKE '%';
                return self.answer_function_status()
            elif sql_category in ("index", "keys", "indexes"):
                # INDEX | INDEXES | KEYS are synonyms
                # https://dev.mysql.com/doc/refman/8.0/en/show-index.html
                new_statement = Select(
                    targets=[
                        Identifier("TABLE_NAME", alias=Identifier("Table")),
                        Identifier("NON_UNIQUE", alias=Identifier("Non_unique")),
                        Identifier("INDEX_NAME", alias=Identifier("Key_name")),
                        Identifier("SEQ_IN_INDEX", alias=Identifier("Seq_in_index")),
                        Identifier("COLUMN_NAME", alias=Identifier("Column_name")),
                        Identifier("COLLATION", alias=Identifier("Collation")),
                        Identifier("CARDINALITY", alias=Identifier("Cardinality")),
                        Identifier("SUB_PART", alias=Identifier("Sub_part")),
                        Identifier("PACKED", alias=Identifier("Packed")),
                        Identifier("NULLABLE", alias=Identifier("Null")),
                        Identifier("INDEX_TYPE", alias=Identifier("Index_type")),
                        Identifier("COMMENT", alias=Identifier("Comment")),
                        Identifier("INDEX_COMMENT", alias=Identifier("Index_comment")),
                        Identifier("IS_VISIBLE", alias=Identifier("Visible")),
                        Identifier("EXPRESSION", alias=Identifier("Expression")),
                    ],
                    from_table=Identifier(parts=["information_schema", "STATISTICS"]),
                    where=statement.where,
                )
                query = SQLQuery(new_statement, session=self.session, database=database_name)
                return self.answer_select(query)
            # FIXME if have answer on that request, then DataGrip show warning '[S0022] Column 'Non_unique' not found.'
            elif "show create table" in sql_lower:
                # SHOW CREATE TABLE `MINDSDB`.`predictors`
                table = sql[sql.rfind(".") + 1:].strip(" .;\n\t").replace("`", "")
                return self.answer_show_create_table(table)
            elif sql_category in ("character set", "charset"):
                new_statement = Select(
                    targets=[
                        Identifier("CHARACTER_SET_NAME", alias=Identifier("Charset")),
                        Identifier(
                            "DEFAULT_COLLATE_NAME", alias=Identifier("Description")
                        ),
                        Identifier(
                            "DESCRIPTION", alias=Identifier("Default collation")
                        ),
                        Identifier("MAXLEN", alias=Identifier("Maxlen")),
                    ],
                    from_table=Identifier(
                        parts=["INFORMATION_SCHEMA", "CHARACTER_SETS"]
                    ),
                    where=_get_show_where(statement, like_name="CHARACTER_SET_NAME"),
                )
                query = SQLQuery(new_statement, session=self.session, database=database_name)
                return self.answer_select(query)
            elif sql_category == "warnings":
                return self.answer_show_warnings()
            elif sql_category == "engines":
                new_statement = Select(
                    targets=[Star()],
                    from_table=Identifier(parts=["information_schema", "ENGINES"]),
                )
                query = SQLQuery(new_statement, session=self.session, database=database_name)
                return self.answer_select(query)
            elif sql_category == "collation":
                new_statement = Select(
                    targets=[
                        Identifier("COLLATION_NAME", alias=Identifier("Collation")),
                        Identifier("CHARACTER_SET_NAME", alias=Identifier("Charset")),
                        Identifier("ID", alias=Identifier("Id")),
                        Identifier("IS_DEFAULT", alias=Identifier("Default")),
                        Identifier("IS_COMPILED", alias=Identifier("Compiled")),
                        Identifier("SORTLEN", alias=Identifier("Sortlen")),
                        Identifier("PAD_ATTRIBUTE", alias=Identifier("Pad_attribute")),
                    ],
                    from_table=Identifier(parts=["INFORMATION_SCHEMA", "COLLATIONS"]),
                    where=_get_show_where(statement, like_name="Collation"),
                )
                query = SQLQuery(new_statement, session=self.session, database=database_name)
                return self.answer_select(query)
            elif sql_category == "table status":
                # TODO improve it
                # SHOW TABLE STATUS LIKE 'table'
                table_name = None
                if statement.like is not None:
                    table_name = statement.like
                # elif condition == 'from' and type(expression) == Identifier:
                #     table_name = expression.parts[-1]
                if table_name is None:
                    err_str = f"Can't determine table name in query: {sql}"
                    logger.warning(err_str)
                    raise TableNotExistError(err_str)
                return self.answer_show_table_status(table_name)
            elif sql_category == "columns":
                is_full = statement.modes is not None and "full" in statement.modes
                return self.answer_show_columns(
                    statement.from_table,
                    statement.where,
                    statement.like,
                    is_full=is_full,
                    database_name=database_name,
                )

            elif sql_category in ("agents", "jobs", "skills", "chatbots", "triggers", "views",
                                  "knowledge_bases", "knowledge bases", "predictors", "models"):

                if sql_category == "knowledge bases":
                    sql_category = "knowledge_bases"

                if sql_category == "predictors":
                    sql_category = "models"

                db_name = database_name
                if statement.from_table is not None:
                    db_name = statement.from_table.parts[-1]

                where = BinaryOperation(op='=', args=[Identifier('project'), Constant(db_name)])

                select_statement = Select(
                    targets=[Star()],
                    from_table=Identifier(
                        parts=["information_schema", sql_category]
                    ),
                    where=_get_show_where(statement, like_name="name", initial=where),
                )
                query = SQLQuery(select_statement, session=self.session)
                return self.answer_select(query)

            elif sql_category == "projects":
                where = BinaryOperation(op='=', args=[Identifier('type'), Constant('project')])
                select_statement = Select(
                    targets=[Identifier(parts=["NAME"], alias=Identifier('project'))],
                    from_table=Identifier(
                        parts=["information_schema", "DATABASES"]
                    ),
                    where=_get_show_where(statement, like_name="project", from_name="project", initial=where),
                )

                query = SQLQuery(select_statement, session=self.session)
                return self.answer_select(query)
            else:
                raise NotSupportedYet(f"Statement not implemented: {sql}")
        elif type(statement) in (
            StartTransaction,
            CommitTransaction,
            RollbackTransaction,
        ):
            return ExecuteAnswer()
        elif type(statement) is Set:
            category = (statement.category or "").lower()
            if category == "" and isinstance(statement.name, Identifier):
                param = statement.name.parts[0].lower()

                value = None
                if isinstance(statement.value, Constant):
                    value = statement.value.value

                if param == "profiling":
                    self.session.profiling = value in (1, True)
                    if self.session.profiling is True:
                        profiler.enable()
                    else:
                        profiler.disable()
                elif param == "predictor_cache":
                    self.session.predictor_cache = value in (1, True)
                elif param == "context":
                    if value in (0, False, None):
                        # drop context
                        query_context_controller.drop_query_context(None)
                elif param == "show_secrets":
                    self.session.show_secrets = value in (1, True)

                return ExecuteAnswer()
            elif category == "autocommit":
                return ExecuteAnswer()
            elif category == "names":
                # set names utf8;
                charsets = {
                    "utf8": CHARSET_NUMBERS["utf8_general_ci"],
                    "utf8mb4": CHARSET_NUMBERS["utf8mb4_general_ci"],
                }
                self.charset = statement.value.value
                self.charset_text_type = charsets.get(self.charset)
                if self.charset_text_type is None:
                    logger.warning(
                        f"Unknown charset: {self.charset}. Setting up 'utf8_general_ci' as charset text type."
                    )
                    self.charset_text_type = CHARSET_NUMBERS["utf8_general_ci"]
                return ExecuteAnswer(
                    state_track=[
                        ["character_set_client", self.charset],
                        ["character_set_connection", self.charset],
                        ["character_set_results", self.charset],
                    ],
                )
            elif category == "active":
                return self.answer_update_model_version(statement.value, database_name)

            else:
                logger.warning(
                    f"SQL statement is not processable, return OK package: {sql}"
                )
                return ExecuteAnswer()
        elif type(statement) is Use:
            db_name = statement.value.parts[-1]
            self.change_default_db(db_name)
            return ExecuteAnswer()
        elif type(statement) in (
            CreatePredictor,
            CreateAnomalyDetectionModel,  # we may want to specialize these in the future
        ):
            return self.answer_create_predictor(statement, database_name)
        elif type(statement) is CreateView:
            return self.answer_create_view(statement, database_name)
        elif type(statement) is DropView:
            return self.answer_drop_view(statement, database_name)
        elif type(statement) is Delete:
            SQLQuery(statement, session=self.session, execute=True, database=database_name)
            return ExecuteAnswer()

        elif type(statement) is Insert:
            SQLQuery(statement, session=self.session, execute=True, database=database_name)
            return ExecuteAnswer()
        elif type(statement) is Update:
            SQLQuery(statement, session=self.session, execute=True, database=database_name)
            return ExecuteAnswer()
        elif (
            type(statement) is Alter
            and ("disable keys" in sql_lower)
            or ("enable keys" in sql_lower)
        ):
            return ExecuteAnswer()
        elif type(statement) is Select:
            if statement.from_table is None:
                return self.answer_single_row_select(statement, database_name)
            query = SQLQuery(statement, session=self.session, database=database_name)
            return self.answer_select(query)
        elif type(statement) is Union:
            query = SQLQuery(statement, session=self.session, database=database_name)
            return self.answer_select(query)
        elif type(statement) is Explain:
            return self.answer_show_columns(statement.target, database_name=database_name)
        elif type(statement) is CreateTable:
            return self.answer_create_table(statement, database_name)
        # -- jobs --
        elif type(statement) is CreateJob:
            return self.answer_create_job(statement, database_name)
        elif type(statement) is DropJob:
            return self.answer_drop_job(statement, database_name)
        # -- triggers --
        elif type(statement) is CreateTrigger:
            return self.answer_create_trigger(statement, database_name)
        elif type(statement) is DropTrigger:
            return self.answer_drop_trigger(statement, database_name)
        # -- chatbots
        elif type(statement) is CreateChatBot:
            return self.answer_create_chatbot(statement, database_name)
        elif type(statement) is UpdateChatBot:
            return self.answer_update_chatbot(statement, database_name)
        elif type(statement) is DropChatBot:
            return self.answer_drop_chatbot(statement, database_name)
        elif type(statement) is CreateKnowledgeBase:
            return self.answer_create_kb(statement, database_name)
        elif type(statement) is DropKnowledgeBase:
            return self.answer_drop_kb(statement, database_name)
        elif type(statement) is CreateSkill:
            return self.answer_create_skill(statement, database_name)
        elif type(statement) is DropSkill:
            return self.answer_drop_skill(statement, database_name)
        elif type(statement) is UpdateSkill:
            return self.answer_update_skill(statement, database_name)
        elif type(statement) is CreateAgent:
            return self.answer_create_agent(statement, database_name)
        elif type(statement) is DropAgent:
            return self.answer_drop_agent(statement, database_name)
        elif type(statement) is UpdateAgent:
            return self.answer_update_agent(statement, database_name)
        elif type(statement) is Evaluate:
            statement.data = parse_sql(statement.query_str, dialect="mindsdb")
            return self.answer_evaluate_metric(statement, database_name)
        else:
            logger.warning(f"Unknown SQL statement: {sql}")
            raise NotSupportedYet(f"Unknown SQL statement: {sql}")

    def answer_create_trigger(self, statement, database_name):
        triggers_controller = TriggersController()

        name = statement.name
        trigger_name = statement.name.parts[-1]
        project_name = name.parts[-2] if len(name.parts) > 1 else database_name

        triggers_controller.add(
            trigger_name,
            project_name,
            statement.table,
            statement.query_str,
            statement.columns,
        )
        return ExecuteAnswer()

    def answer_drop_trigger(self, statement, database_name):
        triggers_controller = TriggersController()

        name = statement.name
        trigger_name = statement.name.parts[-1]
        project_name = name.parts[-2] if len(name.parts) > 1 else database_name

        triggers_controller.delete(trigger_name, project_name)

        return ExecuteAnswer()

    def answer_create_job(self, statement: CreateJob, database_name):
        jobs_controller = JobsController()

        name = statement.name
        job_name = name.parts[-1]
        project_name = name.parts[-2] if len(name.parts) > 1 else database_name

        try:
            jobs_controller.create(job_name, project_name, statement)
        except EntityExistsError:
            if getattr(statement, "if_not_exists", False) is False:
                raise

        return ExecuteAnswer()

    def answer_drop_job(self, statement, database_name):
        jobs_controller = JobsController()

        name = statement.name
        job_name = name.parts[-1]
        project_name = name.parts[-2] if len(name.parts) > 1 else database_name
        try:
            jobs_controller.delete(job_name, project_name)
        except EntityNotExistsError:
            if statement.if_exists is False:
                raise
        except Exception as e:
            raise e

        return ExecuteAnswer()

    def answer_create_chatbot(self, statement, database_name):
        chatbot_controller = ChatBotController()

        name = statement.name
        project_name = name.parts[-2] if len(name.parts) > 1 else database_name
        is_running = statement.params.pop("is_running", True)

        database = self.session.integration_controller.get(statement.database.parts[-1])
        if database is None:
            raise ExecutorException(f"Database not found: {statement.database}")

        # Database ID cannot be null
        database_id = database["id"] if database is not None else -1

        model_name = None
        if statement.model is not None:
            model_name = statement.model.parts[-1]

        agent_name = None
        if statement.agent is not None:
            agent_name = statement.agent.parts[-1]
        chatbot_controller.add_chatbot(
            name.parts[-1],
            project_name=project_name,
            model_name=model_name,
            agent_name=agent_name,
            database_id=database_id,
            is_running=is_running,
            params=statement.params,
        )
        return ExecuteAnswer()

    def answer_update_chatbot(self, statement, database_name):
        chatbot_controller = ChatBotController()

        name = statement.name
        name_no_project = name.parts[-1]
        project_name = name.parts[-2] if len(name.parts) > 1 else database_name

        # From SET keyword parameters
        updated_name = statement.params.pop("name", None)
        model_name = statement.params.pop("model", None)
        agent_name = statement.params.pop("agent", None)
        database_name = statement.params.pop("database", None)
        is_running = statement.params.pop("is_running", None)

        database_id = None
        if database_name is not None:
            database = self.session.integration_controller.get(database_name)
            if database is None:
                raise ExecutorException(f"Database with name {database_name} not found")
            database_id = database["id"]

        updated_chatbot = chatbot_controller.update_chatbot(
            name_no_project,
            project_name=project_name,
            name=updated_name,
            model_name=model_name,
            agent_name=agent_name,
            database_id=database_id,
            is_running=is_running,
            params=statement.params,
        )
        if updated_chatbot is None:
            raise ExecutorException(f"Chatbot with name {name_no_project} not found")
        return ExecuteAnswer()

    def answer_drop_chatbot(self, statement, database_name):
        chatbot_controller = ChatBotController()

        name = statement.name
        project_name = name.parts[-2] if len(name.parts) > 1 else database_name

        chatbot_controller.delete_chatbot(name.parts[-1], project_name=project_name)
        return ExecuteAnswer()

    def answer_evaluate_metric(self, statement, database_name):
        try:
            sqlquery = SQLQuery(statement.data, session=self.session, database=database_name)
        except Exception as e:
            raise Exception(
                f'Nested query failed to execute with error: "{e}", please check and try again.'
            )
        result = sqlquery.fetch('dataframe')
        df = result["result"]
        df.columns = [
            str(t.alias) if hasattr(t, "alias") else str(t.parts[-1])
            for t in statement.data.targets
        ]

        for col in ["actual", "prediction"]:
            assert (
                col in df.columns
            ), f"`{col}` column was not provided, please try again."
            assert (
                df[col].isna().sum() == 0
            ), f"There are missing values in the `{col}` column, please try again."

        metric_name = statement.name.parts[-1]
        target_series = df.pop("prediction")
        using_clause = statement.using if statement.using is not None else {}
        metric_value = evaluate_accuracy(
            df,
            target_series,
            metric_name,
            target="actual",
            ts_analysis=using_clause.get("ts_analysis", {}),  # will be deprecated soon
            n_decimals=using_clause.get("n_decimals", 3),
        )  # 3 decimals by default
        return ExecuteAnswer(
            data=ResultSet(
                columns=[Column(name=metric_name, table_name="", type="str")],
                values=[[metric_value]],
            )
        )

    def answer_describe_object(self, obj_type: str, obj_name: Identifier, database_name: str):

        project_objects = ("AGENTS", "JOBS", "SKILLS", "CHATBOTS", "TRIGGERS", "VIEWS",
                           "KNOWLEDGE_BASES", "PREDICTORS", "MODELS")

        global_objects = ("DATABASES", "PROJECTS", "HANDLERS", "ML_ENGINES")

        all_objects = project_objects + global_objects

        # is not plural?
        if obj_type not in all_objects:
            if obj_type + 'S' in all_objects:
                obj_type = obj_type + 'S'
            elif obj_type + 'ES' in all_objects:
                obj_type = obj_type + 'ES'
            else:
                raise WrongArgumentError(f'Unknown describe type: {obj_type}')

        name = obj_name.parts[-1]
        where = BinaryOperation(op='=', args=[
            Identifier('name'),
            Constant(name)
        ])

        if obj_type in project_objects:
            where = BinaryOperation(op='and', args=[
                where,
                BinaryOperation(op='=', args=[Identifier('project'), Constant(database_name)])
            ])

        select_statement = Select(
            targets=[Star()],
            from_table=Identifier(
                parts=["information_schema", obj_type]
            ),

            where=where,
        )
        query = SQLQuery(select_statement, session=self.session)
        return self.answer_select(query)

    def answer_describe_predictor(self, obj_name, database_name):
        value = obj_name.parts.copy()
        # project.model.version.?attrs
        parts = value[:3]
        attrs = value[3:]
        model_info = self._get_model_info(Identifier(parts=parts), except_absent=False, database_name=database_name)
        if model_info is None:
            # project.model.?attrs
            parts = value[:2]
            attrs = value[2:]
            model_info = self._get_model_info(Identifier(parts=parts), except_absent=False, database_name=database_name)
            if model_info is None:
                # model.?attrs
                parts = value[:1]
                attrs = value[1:]
                model_info = self._get_model_info(Identifier(parts=parts), except_absent=False, database_name=database_name)

        if model_info is None:
            raise ExecutorException(f"Model not found: {obj_name}")

        if len(attrs) == 1:
            attrs = attrs[0]
        elif len(attrs) == 0:
            attrs = None

        df = self.session.model_controller.describe_model(
            self.session,
            model_info["project_name"],
            model_info["model_record"].name,
            attribute=attrs,
            version=model_info['model_record'].version
        )

        return ExecuteAnswer(
            data=ResultSet().from_df(df, table_name="")
        )

    def _get_model_info(self, identifier, except_absent=True, database_name=None):
        if len(identifier.parts) == 1:
            identifier.parts = [database_name, identifier.parts[0]]

        database_name, model_name, model_version = resolve_model_identifier(identifier)
        if database_name is None:
            database_name = database_name

        if model_name is None:
            if except_absent:
                raise Exception(f"Model not found: {identifier.to_string()}")
            else:
                return

        model_record = get_model_record(
            name=model_name,
            project_name=database_name,
            except_absent=except_absent,
            version=model_version,
            active=True if model_version is None else None,
        )
        if not model_record:
            return None
        return {"model_record": model_record, "project_name": database_name}

    def _sync_predictor_check(self, phase_name):
        """Checks if there is already a predictor retraining or fine-tuning
        Do not allow to run retrain if there is another model in training process in less that 1h
        """
        is_cloud = self.session.config.get("cloud", False)
        if is_cloud and ctx.user_class == 0:
            models = get_model_records(active=None)
            shortest_training = None
            for model in models:
                if (
                    model.status
                    in (PREDICTOR_STATUS.GENERATING, PREDICTOR_STATUS.TRAINING)
                    and model.training_start_at is not None
                    and model.training_stop_at is None
                ):
                    training_time = datetime.datetime.now() - model.training_start_at
                    if shortest_training is None or training_time < shortest_training:
                        shortest_training = training_time

            if (
                shortest_training is not None
                and shortest_training < datetime.timedelta(hours=1)
            ):
                raise ExecutorException(
                    f"Can't start {phase_name} process while any other predictor is in status 'training' or 'generating'"
                )

    def answer_retrain_predictor(self, statement, database_name):
        model_record = self._get_model_info(statement.name, database_name=database_name)["model_record"]

        if statement.query_str is None:
            if model_record.data_integration_ref is not None:
                if model_record.data_integration_ref["type"] == "integration":
                    integration = self.session.integration_controller.get_by_id(
                        model_record.data_integration_ref["id"]
                    )
                    if integration is None:
                        raise EntityNotExistsError(
                            "The database from which the model was trained no longer exists"
                        )
        elif statement.integration_name is None:
            # set to current project
            statement.integration_name = Identifier(database_name)

        ml_handler = None
        if statement.using is not None:
            # repack using with lower names
            statement.using = {k.lower(): v for k, v in statement.using.items()}

            if "engine" in statement.using:
                ml_integration_name = statement.using.pop("engine")
                ml_handler = self.session.integration_controller.get_ml_handler(
                    ml_integration_name
                )

        # use current ml handler
        if ml_handler is None:
            integration_record = get_predictor_integration(model_record)
            if integration_record is None:
                raise EntityNotExistsError("ML engine model was trained with does not esxists")
            ml_handler = self.session.integration_controller.get_ml_handler(
                integration_record.name
            )

        self._sync_predictor_check(phase_name="retrain")
        df = self.session.model_controller.retrain_model(statement, ml_handler)

        return ExecuteAnswer(
            data=ResultSet().from_df(df)
        )

    @profiler.profile()
    @mark_process("learn")
    def answer_finetune_predictor(self, statement, database_name):
        model_record = self._get_model_info(statement.name, database_name=database_name)["model_record"]

        if statement.using is not None:
            # repack using with lower names
            statement.using = {k.lower(): v for k, v in statement.using.items()}

        if statement.query_str is not None and statement.integration_name is None:
            # set to current project
            statement.integration_name = Identifier(database_name)

        # use current ml handler
        integration_record = get_predictor_integration(model_record)
        if integration_record is None:
            raise Exception(
                "The ML engine that the model was trained with does not exist."
            )
        ml_handler = self.session.integration_controller.get_ml_handler(
            integration_record.name
        )

        self._sync_predictor_check(phase_name="finetune")
        df = self.session.model_controller.finetune_model(statement, ml_handler)

        return ExecuteAnswer(
            data=ResultSet().from_df(df)
        )

    def _create_integration(self, name: str, engine: str, connection_args: dict):
        # we have connection checkers not for any db. So do nothing if fail
        # TODO return rich error message

        if connection_args is None:
            connection_args = {}
        status = HandlerStatusResponse(success=False)

        storage = None
        try:
            handler_meta = self.session.integration_controller.get_handler_meta(engine)
            if handler_meta.get("import", {}).get("success") is not True:
                raise ExecutorException(f"The '{engine}' handler isn't installed.\n" + get_handler_install_message(engine))

            accept_connection_args = handler_meta.get("connection_args")
            if accept_connection_args is not None and connection_args is not None:
                for arg_name, arg_value in connection_args.items():
                    if arg_name not in accept_connection_args:
                        continue
                    arg_meta = accept_connection_args[arg_name]
                    arg_type = arg_meta.get("type")
                    if arg_type == HANDLER_CONNECTION_ARG_TYPE.PATH:
                        # arg may be one of:
                        # str: '/home/file.pem'
                        # dict: {'path': '/home/file.pem'}
                        # dict: {'url': 'https://host.com/file'}
                        arg_value = connection_args[arg_name]
                        if isinstance(arg_value, (str, dict)) is False:
                            raise ExecutorException(f"Unknown type of arg: '{arg_value}'")
                        if isinstance(arg_value, str) or "path" in arg_value:
                            path = (
                                arg_value
                                if isinstance(arg_value, str)
                                else arg_value["path"]
                            )
                            if Path(path).is_file() is False:
                                raise ExecutorException(f"File not found at: '{path}'")
                        elif "url" in arg_value:
                            path = download_file(arg_value["url"])
                        else:
                            raise ExecutorException(
                                f"Argument '{arg_name}' must be path or url to the file"
                            )
                        connection_args[arg_name] = path

            handler = self.session.integration_controller.create_tmp_handler(
                name=name,
                engine=engine,
                connection_args=connection_args
            )
            status = handler.check_connection()
            if status.copy_storage:
                storage = handler.handler_storage.export_files()
        except Exception as e:
            status.error_message = str(e)

        if status.success is False:
            raise ExecutorException(f"Can't connect to db: {status.error_message}")

        integration = self.session.integration_controller.get(name)
        if integration is not None:
            raise EntityExistsError('Database already exists', name)
        try:
            integration = ProjectController().get(name=name)
        except EntityNotExistsError:
            pass
        if integration is not None:
            raise EntityExistsError('Project exists with this name', name)

        self.session.integration_controller.add(name, engine, connection_args)
        if storage:
            handler = self.session.integration_controller.get_data_handler(name, connect=False)
            handler.handler_storage.import_files(storage)

    def answer_create_ml_engine(self, name: str, handler: str, params: dict = None, if_not_exists=False):

        integrations = self.session.integration_controller.get_all()
        if name in integrations:
            if not if_not_exists:
                raise EntityExistsError('Integration already exists', name)
            else:
                return ExecuteAnswer()

        handler_module_meta = self.session.integration_controller.get_handler_meta(handler)

        if handler_module_meta is None:
            raise ExecutorException(f"There is no engine '{handler}'")

        params_out = {}
        if params:
            for key, value in params.items():
                # convert ast types to string
                if isinstance(value, (Constant, Identifier)):
                    value = value.to_string()
                params_out[key] = value

        try:
            self.session.integration_controller.add(
                name=name, engine=handler, connection_args=params_out
            )
        except Exception as e:
            msg = str(e)
            if type(e) in (ImportError, ModuleNotFoundError):
                msg = dedent(
                    f"""\
                    The '{handler_module_meta['name']}' handler cannot be used. Reason is:
                        {handler_module_meta['import']['error_message']}
                """
                )
                is_cloud = self.session.config.get("cloud", False)
                if is_cloud is False and "No module named" in handler_module_meta['import']['error_message']:
                    logger.info(get_handler_install_message(handler_module_meta['name']))
            ast_drop = DropMLEngine(name=Identifier(name))
            self.answer_drop_ml_engine(ast_drop)
            logger.info(msg)
            raise ExecutorException(msg)

        return ExecuteAnswer()

    def answer_drop_ml_engine(self, statement: ASTNode):
        name = statement.name.parts[-1]
        integrations = self.session.integration_controller.get_all()
        if name not in integrations:
            if not statement.if_exists:
                raise EntityNotExistsError('Integration does not exists', name)
            else:
                return ExecuteAnswer()
        self.session.integration_controller.delete(name)
        return ExecuteAnswer()

    def answer_create_database(self, statement: ASTNode):
        """create new handler (datasource/integration in old terms)
        Args:
            statement (ASTNode): data for creating database/project
        """

        if len(statement.name.parts) != 1:
            raise Exception("Database name should contain only 1 part.")

        database_name = statement.name.parts[0]
        engine = statement.engine
        if engine is None:
            engine = "mindsdb"
        engine = engine.lower()
        connection_args = statement.parameters

        if engine == "mindsdb":
            try:
                ProjectController().add(database_name)
            except EntityExistsError:
                if statement.if_not_exists is False:
                    raise
        else:
            try:
                self._create_integration(database_name, engine, connection_args)
            except EntityExistsError:
                if getattr(statement, "if_not_exists", False) is False:
                    raise

        return ExecuteAnswer()

    def answer_drop_database(self, statement):
        if len(statement.name.parts) != 1:
            raise Exception("Database name should contain only 1 part.")
        db_name = statement.name.parts[0]
        try:
            self.session.database_controller.delete(db_name)
        except EntityNotExistsError:
            if statement.if_exists is not True:
                raise
        return ExecuteAnswer()

    def answer_drop_tables(self, statement, database_name):
        """answer on 'drop table [if exists] {name}'
        Args:
            statement: ast
        """

        for table in statement.tables:
            if len(table.parts) > 1:
                db_name = table.parts[0]
                table = Identifier(parts=table.parts[1:])
            else:
                db_name = database_name

            dn = self.session.datahub[db_name]
            if db_name is not None:
                dn.drop_table(table, if_exists=statement.if_exists)

            elif db_name in self.session.database_controller.get_dict(filter_type="project"):
                # TODO do we need feature: delete object from project via drop table?

                project = self.session.database_controller.get_project(db_name)
                project_tables = {
                    key: val
                    for key, val in project.get_tables().items()
                    if val.get("deletable") is True
                }
                table_name = table.to_string()

                if table_name in project_tables:
                    self.session.model_controller.delete_model(
                        table_name, project_name=db_name
                    )
                elif statement.if_exists is False:
                    raise ExecutorException(
                        f"Cannot delete a table from database '{db_name}': table does not exists"
                    )
            else:
                raise ExecutorException(
                    f"Cannot delete a table from database '{db_name}'"
                )

        return ExecuteAnswer()

    def answer_create_view(self, statement, database_name):
        project_name = database_name
        # TEMP
        if isinstance(statement.name, Identifier):
            parts = statement.name.parts
        else:
            parts = statement.name.split(".")

        view_name = parts[-1]
        if len(parts) == 2:
            project_name = parts[0]

        query_str = statement.query_str
        query = parse_sql(query_str, dialect="mindsdb")

        if isinstance(statement.from_table, Identifier):
            query = Select(
                targets=[Star()],
                from_table=NativeQuery(
                    integration=statement.from_table, query=statement.query_str
                ),
            )
            query_str = str(query)

        if isinstance(query, Select):
            # check create view sql
            query.limit = Constant(1)

            query_context_controller.set_context(
                query_context_controller.IGNORE_CONTEXT
            )
            try:
                sqlquery = SQLQuery(query, session=self.session, database=database_name)
                if sqlquery.fetch()["success"] is not True:
                    raise ExecutorException("Wrong view query")
            finally:
                query_context_controller.release_context(
                    query_context_controller.IGNORE_CONTEXT
                )

        project = self.session.database_controller.get_project(project_name)
        try:
            project.create_view(view_name, query=query_str)
        except EntityExistsError:
            if getattr(statement, "if_not_exists", False) is False:
                raise
        return ExecuteAnswer()

    def answer_drop_view(self, statement, database_name):
        names = statement.names

        for name in names:
            view_name = name.parts[-1]
            if len(name.parts) > 1:
                db_name = name.parts[0]
            else:
                db_name = database_name
            project = self.session.database_controller.get_project(db_name)

            try:
                project.drop_view(view_name)
            except EntityNotExistsError:
                if statement.if_exists is not True:
                    raise

        return ExecuteAnswer()

    def answer_create_kb(self, statement: CreateKnowledgeBase, database_name: str):
        project_name = (
            statement.name.parts[0]
            if len(statement.name.parts) > 1
            else database_name
        )

        if statement.storage is not None:
            if len(statement.storage.parts) != 2:
                raise ExecutorException(
                    f"Invalid vectordatabase table name: {statement.storage}"
                    "Need the form 'database_name.table_name'"
                )

        if statement.from_query is not None:
            # TODO: implement this
            raise ExecutorException(
                "Create a knowledge base from a select is not supported yet"
            )

        kb_name = statement.name.parts[-1]

        # create the knowledge base
        _ = self.session.kb_controller.add(
            name=kb_name,
            project_name=project_name,
            embedding_model=statement.model,
            storage=statement.storage,
            params=statement.params,
            if_not_exists=statement.if_not_exists,
        )

        return ExecuteAnswer()

    def answer_drop_kb(self, statement: DropKnowledgeBase, database_name: str):
        name = statement.name.parts[-1]
        project_name = (
            statement.name.parts[0]
            if len(statement.name.parts) > 1
            else database_name
        )

        # delete the knowledge base
        self.session.kb_controller.delete(
            name=name,
            project_name=project_name,
            if_exists=statement.if_exists,
        )

        return ExecuteAnswer()

    def answer_create_skill(self, statement, database_name):
        name = statement.name.parts[-1]
        project_name = (
            statement.name.parts[0]
            if len(statement.name.parts) > 1
            else database_name
        )

        try:
            _ = self.session.skills_controller.add_skill(
                name,
                project_name,
                statement.type,
                statement.params
            )
        except ValueError as e:
            # Project does not exist or skill already exists.
            raise ExecutorException(str(e))

        return ExecuteAnswer()

    def answer_drop_skill(self, statement, database_name):
        name = statement.name.parts[-1]
        project_name = (
            statement.name.parts[0]
            if len(statement.name.parts) > 1
            else database_name
        )

        try:
            self.session.skills_controller.delete_skill(name, project_name)
        except ValueError as e:
            # Project does not exist or skill does not exist.
            raise ExecutorException(str(e))

        return ExecuteAnswer()

    def answer_update_skill(self, statement, database_name):
        name = statement.name.parts[-1]
        project_name = (
            statement.name.parts[0]
            if len(statement.name.parts) > 1
            else database_name
        )

        type = statement.params.pop('type', None)
        try:
            _ = self.session.skills_controller.update_skill(
                name,
                project_name=project_name,
                type=type,
                params=statement.params
            )
        except ValueError as e:
            # Project does not exist or skill does not exist.
            raise ExecutorException(str(e))

        return ExecuteAnswer()

    def answer_create_agent(self, statement, database_name):
        name = statement.name.parts[-1]
        project_name = (
            statement.name.parts[0]
            if len(statement.name.parts) > 1
            else database_name
        )

        skills = statement.params.pop('skills', [])
        provider = statement.params.pop('provider', None)
        try:
            _ = self.session.agents_controller.add_agent(
                name=name,
                project_name=project_name,
                model_name=statement.model,
                skills=skills,
                provider=provider,
                params=statement.params
            )
        except ValueError as e:
            # Project does not exist or agent already exists.
            raise ExecutorException(str(e))

        return ExecuteAnswer()

    def answer_drop_agent(self, statement, database_name):
        name = statement.name.parts[-1]
        project_name = (
            statement.name.parts[0]
            if len(statement.name.parts) > 1
            else database_name
        )

        try:
            self.session.agents_controller.delete_agent(name, project_name)
        except ValueError as e:
            # Project does not exist or agent does not exist.
            raise ExecutorException(str(e))

        return ExecuteAnswer()

    def answer_update_agent(self, statement, database_name):
        name = statement.name.parts[-1]
        project_name = (
            statement.name.parts[0]
            if len(statement.name.parts) > 1
            else database_name
        )

        model = statement.params.pop('model', None)
        skills_to_add = statement.params.pop('skills_to_add', [])
        skills_to_remove = statement.params.pop('skills_to_remove', [])
        try:
            _ = self.session.agents_controller.update_agent(
                name,
                project_name=project_name,
                model_name=model,
                skills_to_add=skills_to_add,
                skills_to_remove=skills_to_remove,
                params=statement.params
            )
        except ValueError as e:
            # Project does not exist or agent does not exist.
            raise ExecutorException(str(e))

        return ExecuteAnswer()

    @mark_process("learn")
    def answer_create_predictor(self, statement: CreatePredictor, database_name):
        integration_name = database_name

        # allow creation in non-active projects, e.g. 'create mode proj.model' works whether `proj` is active or not
        if len(statement.name.parts) > 1:
            integration_name = statement.name.parts[0]
        model_name = statement.name.parts[-1]
        statement.name.parts = [integration_name.lower(), model_name]

        ml_integration_name = "lightwood"  # default
        if statement.using is not None:
            # repack using with lower names
            statement.using = {k.lower(): v for k, v in statement.using.items()}

            ml_integration_name = statement.using.pop("engine", ml_integration_name)

        if statement.query_str is not None and statement.integration_name is None:
            # set to current project
            statement.integration_name = Identifier(database_name)

        try:
            ml_handler = self.session.integration_controller.get_ml_handler(
                ml_integration_name
            )
        except EntityNotExistsError:
            # not exist, try to create it with same name as handler
            self.answer_create_ml_engine(ml_integration_name, handler=ml_integration_name)

            ml_handler = self.session.integration_controller.get_ml_handler(
                ml_integration_name
            )

        if getattr(statement, "is_replace", False) is True:
            # try to delete
            try:
                self.session.model_controller.delete_model(
                    model_name,
                    project_name=integration_name
                )
            except EntityNotExistsError:
                pass

        try:
            df = self.session.model_controller.create_model(statement, ml_handler)

            return ExecuteAnswer(data=ResultSet().from_df(df))
        except EntityExistsError:
            if getattr(statement, "if_not_exists", False) is True:
                return ExecuteAnswer()
            raise

    def answer_show_columns(
        self,
        target: Identifier,
        where: Optional[Operation] = None,
        like: Optional[str] = None,
        is_full=False,
        database_name=None,
    ):
        if len(target.parts) > 1:
            db = target.parts[0]
        elif isinstance(database_name, str) and len(database_name) > 0:
            db = database_name
        else:
            db = "mindsdb"
        table_name = target.parts[-1]

        new_where = BinaryOperation(
            "and",
            args=[
                BinaryOperation("=", args=[Identifier("TABLE_SCHEMA"), Constant(db)]),
                BinaryOperation(
                    "=", args=[Identifier("TABLE_NAME"), Constant(table_name)]
                ),
            ],
        )
        if where is not None:
            new_where = BinaryOperation("and", args=[new_where, where])
        if like is not None:
            like = BinaryOperation("like", args=[Identifier("View"), Constant(like)])
            new_where = BinaryOperation("and", args=[new_where, like])

        targets = [
            Identifier("COLUMN_NAME", alias=Identifier("Field")),
            Identifier("COLUMN_TYPE", alias=Identifier("Type")),
            Identifier("IS_NULLABLE", alias=Identifier("Null")),
            Identifier("COLUMN_KEY", alias=Identifier("Key")),
            Identifier("COLUMN_DEFAULT", alias=Identifier("Default")),
            Identifier("EXTRA", alias=Identifier("Extra")),
        ]
        if is_full:
            targets.extend(
                [
                    Constant("COLLATION", alias=Identifier("Collation")),
                    Constant("PRIVILEGES", alias=Identifier("Privileges")),
                    Constant("COMMENT", alias=Identifier("Comment")),
                ]
            )
        new_statement = Select(
            targets=targets,
            from_table=Identifier(parts=["information_schema", "COLUMNS"]),
            where=new_where,
        )

        query = SQLQuery(new_statement, session=self.session, database=database_name)
        return self.answer_select(query)

    def answer_single_row_select(self, statement, database_name):

        def adapt_query(node, is_table, **kwargs):
            if is_table:
                return

            if isinstance(node, Identifier):
                if node.parts[-1].lower() == "session_user":
                    return Constant(self.session.username, alias=node)
                if node.parts[-1].lower() == '$$':
                    # NOTE: sinve version 9.0 mysql client sends query 'select $$'.
                    # Connection can be continued only if answer is parse error.
                    raise ErParseError(
                        "You have an error in your SQL syntax; check the manual that corresponds to your server "
                        "version for the right syntax to use near '$$' at line 1"
                    )

            if isinstance(node, Function):
                function_name = node.op.lower()

                functions_results = {
                    "database": database_name,
                    "current_user": self.session.username,
                    "user": self.session.username,
                    "version": "8.0.17",
                    "current_schema": "public",
                    "connection_id": self.context.get('connection_id')
                }
                if function_name in functions_results:
                    return Constant(functions_results[function_name], alias=Identifier(parts=[function_name]))

            if isinstance(node, Variable):
                var_name = node.value
                column_name = f"@@{var_name}"
                result = SERVER_VARIABLES.get(column_name)
                if result is None:
                    logger.error(f"Unknown variable: {column_name}")
                    raise Exception(f"Unknown variable '{var_name}'")
                else:
                    return Constant(result[0], alias=Identifier(parts=[column_name]))

        query_traversal(statement, adapt_query)

        statement.from_table = Identifier('t')
        df = query_df(pd.DataFrame([[0]]), statement, session=self.session)

        return ExecuteAnswer(
            data=ResultSet().from_df(df, table_name="")
        )

    def answer_show_create_table(self, table):
        columns = [
            Column(table_name="", name="Table", type=TYPES.MYSQL_TYPE_VAR_STRING),
            Column(
                table_name="", name="Create Table", type=TYPES.MYSQL_TYPE_VAR_STRING
            ),
        ]
        return ExecuteAnswer(
            data=ResultSet(
                columns=columns,
                values=[[table, f"create table {table} ()"]],
            )
        )

    def answer_function_status(self):
        columns = [
            Column(
                name="Db",
                alias="Db",
                table_name="schemata",
                table_alias="ROUTINES",
                type="str",
                database="mysql",
                charset=self.charset_text_type,
            ),
            Column(
                name="Db",
                alias="Db",
                table_name="routines",
                table_alias="ROUTINES",
                type="str",
                database="mysql",
                charset=self.charset_text_type,
            ),
            Column(
                name="Type",
                alias="Type",
                table_name="routines",
                table_alias="ROUTINES",
                type="str",
                database="mysql",
                charset=CHARSET_NUMBERS["utf8_bin"],
            ),
            Column(
                name="Definer",
                alias="Definer",
                table_name="routines",
                table_alias="ROUTINES",
                type="str",
                database="mysql",
                charset=CHARSET_NUMBERS["utf8_bin"],
            ),
            Column(
                name="Modified",
                alias="Modified",
                table_name="routines",
                table_alias="ROUTINES",
                type=TYPES.MYSQL_TYPE_TIMESTAMP,
                database="mysql",
                charset=CHARSET_NUMBERS["binary"],
            ),
            Column(
                name="Created",
                alias="Created",
                table_name="routines",
                table_alias="ROUTINES",
                type=TYPES.MYSQL_TYPE_TIMESTAMP,
                database="mysql",
                charset=CHARSET_NUMBERS["binary"],
            ),
            Column(
                name="Security_type",
                alias="Security_type",
                table_name="routines",
                table_alias="ROUTINES",
                type=TYPES.MYSQL_TYPE_STRING,
                database="mysql",
                charset=CHARSET_NUMBERS["utf8_bin"],
            ),
            Column(
                name="Comment",
                alias="Comment",
                table_name="routines",
                table_alias="ROUTINES",
                type=TYPES.MYSQL_TYPE_BLOB,
                database="mysql",
                charset=CHARSET_NUMBERS["utf8_bin"],
            ),
            Column(
                name="character_set_client",
                alias="character_set_client",
                table_name="character_sets",
                table_alias="ROUTINES",
                type=TYPES.MYSQL_TYPE_VAR_STRING,
                database="mysql",
                charset=self.charset_text_type,
            ),
            Column(
                name="collation_connection",
                alias="collation_connection",
                table_name="collations",
                table_alias="ROUTINES",
                type=TYPES.MYSQL_TYPE_VAR_STRING,
                database="mysql",
                charset=self.charset_text_type,
            ),
            Column(
                name="Database Collation",
                alias="Database Collation",
                table_name="collations",
                table_alias="ROUTINES",
                type=TYPES.MYSQL_TYPE_VAR_STRING,
                database="mysql",
                charset=self.charset_text_type,
            ),
        ]

        return ExecuteAnswer(data=ResultSet(columns=columns))

    def answer_show_table_status(self, table_name):
        # NOTE at this moment parsed statement only like `SHOW TABLE STATUS LIKE 'table'`.
        # NOTE some columns has {'database': 'mysql'}, other not. That correct. This is how real DB sends messages.
        columns = [
            {
                "database": "mysql",
                "table_name": "tables",
                "name": "Name",
                "alias": "Name",
                "type": TYPES.MYSQL_TYPE_VAR_STRING,
                "charset": self.charset_text_type,
            },
            {
                "database": "",
                "table_name": "tables",
                "name": "Engine",
                "alias": "Engine",
                "type": TYPES.MYSQL_TYPE_VAR_STRING,
                "charset": self.charset_text_type,
            },
            {
                "database": "",
                "table_name": "tables",
                "name": "Version",
                "alias": "Version",
                "type": TYPES.MYSQL_TYPE_LONGLONG,
                "charset": CHARSET_NUMBERS["binary"],
            },
            {
                "database": "mysql",
                "table_name": "tables",
                "name": "Row_format",
                "alias": "Row_format",
                "type": TYPES.MYSQL_TYPE_VAR_STRING,
                "charset": self.charset_text_type,
            },
            {
                "database": "",
                "table_name": "tables",
                "name": "Rows",
                "alias": "Rows",
                "type": TYPES.MYSQL_TYPE_LONGLONG,
                "charset": CHARSET_NUMBERS["binary"],
            },
            {
                "database": "",
                "table_name": "tables",
                "name": "Avg_row_length",
                "alias": "Avg_row_length",
                "type": TYPES.MYSQL_TYPE_LONGLONG,
                "charset": CHARSET_NUMBERS["binary"],
            },
            {
                "database": "",
                "table_name": "tables",
                "name": "Data_length",
                "alias": "Data_length",
                "type": TYPES.MYSQL_TYPE_LONGLONG,
                "charset": CHARSET_NUMBERS["binary"],
            },
            {
                "database": "",
                "table_name": "tables",
                "name": "Max_data_length",
                "alias": "Max_data_length",
                "type": TYPES.MYSQL_TYPE_LONGLONG,
                "charset": CHARSET_NUMBERS["binary"],
            },
            {
                "database": "",
                "table_name": "tables",
                "name": "Index_length",
                "alias": "Index_length",
                "type": TYPES.MYSQL_TYPE_LONGLONG,
                "charset": CHARSET_NUMBERS["binary"],
            },
            {
                "database": "",
                "table_name": "tables",
                "name": "Data_free",
                "alias": "Data_free",
                "type": TYPES.MYSQL_TYPE_LONGLONG,
                "charset": CHARSET_NUMBERS["binary"],
            },
            {
                "database": "",
                "table_name": "tables",
                "name": "Auto_increment",
                "alias": "Auto_increment",
                "type": TYPES.MYSQL_TYPE_LONGLONG,
                "charset": CHARSET_NUMBERS["binary"],
            },
            {
                "database": "",
                "table_name": "tables",
                "name": "Create_time",
                "alias": "Create_time",
                "type": TYPES.MYSQL_TYPE_TIMESTAMP,
                "charset": CHARSET_NUMBERS["binary"],
            },
            {
                "database": "",
                "table_name": "tables",
                "name": "Update_time",
                "alias": "Update_time",
                "type": TYPES.MYSQL_TYPE_TIMESTAMP,
                "charset": CHARSET_NUMBERS["binary"],
            },
            {
                "database": "",
                "table_name": "tables",
                "name": "Check_time",
                "alias": "Check_time",
                "type": TYPES.MYSQL_TYPE_TIMESTAMP,
                "charset": CHARSET_NUMBERS["binary"],
            },
            {
                "database": "mysql",
                "table_name": "tables",
                "name": "Collation",
                "alias": "Collation",
                "type": TYPES.MYSQL_TYPE_VAR_STRING,
                "charset": self.charset_text_type,
            },
            {
                "database": "",
                "table_name": "tables",
                "name": "Checksum",
                "alias": "Checksum",
                "type": TYPES.MYSQL_TYPE_LONGLONG,
                "charset": CHARSET_NUMBERS["binary"],
            },
            {
                "database": "",
                "table_name": "tables",
                "name": "Create_options",
                "alias": "Create_options",
                "type": TYPES.MYSQL_TYPE_VAR_STRING,
                "charset": self.charset_text_type,
            },
            {
                "database": "",
                "table_name": "tables",
                "name": "Comment",
                "alias": "Comment",
                "type": TYPES.MYSQL_TYPE_BLOB,
                "charset": self.charset_text_type,
            },
        ]
        columns = [Column(**d) for d in columns]
        data = [
            [
                table_name,  # Name
                "InnoDB",  # Engine
                10,  # Version
                "Dynamic",  # Row_format
                1,  # Rows
                16384,  # Avg_row_length
                16384,  # Data_length
                0,  # Max_data_length
                0,  # Index_length
                0,  # Data_free
                None,  # Auto_increment
                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Create_time
                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Update_time
                None,  # Check_time
                "utf8mb4_0900_ai_ci",  # Collation
                None,  # Checksum
                "",  # Create_options
                "",  # Comment
            ]
        ]
        return ExecuteAnswer(data=ResultSet(columns=columns, values=data))

    def answer_show_warnings(self):
        columns = [
            {
                "database": "",
                "table_name": "",
                "name": "Level",
                "alias": "Level",
                "type": TYPES.MYSQL_TYPE_VAR_STRING,
                "charset": self.charset_text_type,
            },
            {
                "database": "",
                "table_name": "",
                "name": "Code",
                "alias": "Code",
                "type": TYPES.MYSQL_TYPE_LONG,
                "charset": CHARSET_NUMBERS["binary"],
            },
            {
                "database": "",
                "table_name": "",
                "name": "Message",
                "alias": "Message",
                "type": TYPES.MYSQL_TYPE_VAR_STRING,
                "charset": self.charset_text_type,
            },
        ]
        columns = [Column(**d) for d in columns]
        return ExecuteAnswer(data=ResultSet(columns=columns))

    def answer_create_table(self, statement, database_name):
        SQLQuery(statement, session=self.session, execute=True, database=database_name)
        return ExecuteAnswer()

    def answer_select(self, query):
        data = query.fetch()

        return ExecuteAnswer(data=data["result"])

    def answer_update_model_version(self, model_version, database_name):
        if not isinstance(model_version, Identifier):
            raise ExecutorException(f'Please define version: {model_version}')

        model_parts = model_version.parts
        version = model_parts[-1]
        if version.isdigit():
            version = int(version)
        else:
            raise ExecutorException(f'Unknown version: {version}')

        if len(model_parts) == 3:
            project_name, model_name = model_parts[:2]
        elif len(model_parts) == 2:
            model_name = model_parts[0]
            project_name = database_name
        else:
            raise ExecutorException(f'Unknown model: {model_version}')

        self.session.model_controller.set_model_active_version(project_name, model_name, version)
        return ExecuteAnswer()

    def answer_drop_model(self, statement, database_name):

        model_parts = statement.name.parts
        version = None

        # with version?
        if model_parts[-1].isdigit():
            version = int(model_parts[-1])
            model_parts = model_parts[:-1]

        if len(model_parts) == 2:
            project_name, model_name = model_parts
        elif len(model_parts) == 1:
            model_name = model_parts[0]
            project_name = database_name
        else:
            raise ExecutorException(f'Unknown model: {statement.name}')

        if version is not None:
            # delete version
            try:
                self.session.model_controller.delete_model_version(project_name, model_name, version)
            except EntityNotExistsError as e:
                if not statement.if_exists:
                    raise e
        else:
            # drop model
            try:
                project = self.session.database_controller.get_project(project_name)
                project.drop_model(model_name)
            except Exception as e:
                if not statement.if_exists:
                    raise e

        return ExecuteAnswer()

    def change_default_db(self, db_name):
        # That fix for bug in mssql: it keeps connection for a long time, but after some time mssql can
        # send packet with COM_INIT_DB=null. In this case keep old database name as default.
        if db_name != "null":
            if self.session.database_controller.exists(db_name):
                self.session.database = db_name
            else:
                raise BadDbError(f"Database {db_name} does not exists")
