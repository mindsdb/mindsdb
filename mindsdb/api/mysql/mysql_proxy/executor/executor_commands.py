import datetime
from typing import Optional
from pathlib import Path
from functools import reduce

import pandas as pd
from mindsdb_sql.parser.dialects.mindsdb import (
    CreateDatasource,
    RetrainPredictor,
    CreatePredictor,
    AdjustPredictor,
    CreateMLEngine,
    DropMLEngine,
    DropDatasource,
    DropPredictor,
    CreateView,
    CreateJob,
    DropJob,
)
from mindsdb_sql import parse_sql
from mindsdb_sql.parser.dialects.mysql import Variable
from mindsdb_sql.parser.ast import (
    RollbackTransaction,
    CommitTransaction,
    StartTransaction,
    BinaryOperation,
    DropDatabase,
    NullConstant,
    NativeQuery,
    Describe,
    Constant,
    Function,
    Explain,
    Delete,
    Insert,
    Select,
    Star,
    Show,
    Set,
    Use,
    Alter,
    Update,
    CreateTable,
    Identifier,
    DropTables,
    Operation,
    ASTNode,
    DropView,
    Union,
)
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender

from mindsdb.api.mysql.mysql_proxy.utilities.sql import query_df
from mindsdb.api.mysql.mysql_proxy.utilities import log
from mindsdb.api.mysql.mysql_proxy.utilities import (
    SqlApiException,
    ErBadDbError,
    ErBadTableError,
    ErTableExistError,
    ErNotSupportedYet,
    ErSqlWrongArguments,
)
from mindsdb.api.mysql.mysql_proxy.utilities.functions import download_file
from mindsdb.api.mysql.mysql_proxy.classes.sql_query import SQLQuery, Column
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import (
    CHARSET_NUMBERS,
    TYPES,
    SERVER_VARIABLES,
)
from mindsdb.api.mysql.mysql_proxy.executor.data_types import ExecuteAnswer, ANSWER_TYPE
from mindsdb.integrations.libs.response import HandlerStatusResponse
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE
from mindsdb.interfaces.model.functions import (
    get_model_record,
    get_model_records,
    get_predictor_integration,
)
from mindsdb.integrations.libs.const import PREDICTOR_STATUS
from mindsdb.interfaces.database.projects import ProjectController
from mindsdb.interfaces.jobs.jobs_controller import JobsController
from mindsdb.utilities.context import context as ctx


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
    def __init__(self, session, executor):
        self.session = session
        self.executor = executor

        self.charset_text_type = CHARSET_NUMBERS["utf8_general_ci"]
        self.datahub = session.datahub

    def execute_command(self, statement):
        sql = None
        if self.executor is None:
            if isinstance(statement, ASTNode):
                sql = statement.to_string()
            sql_lower = sql.lower()
        else:
            sql = self.executor.sql
            sql_lower = self.executor.sql_lower

        if type(statement) == CreateDatasource:
            return self.answer_create_database(statement)
        elif type(statement) == CreateMLEngine:
            return self.answer_create_ml_engine(statement)
        elif type(statement) == DropMLEngine:
            return self.answer_drop_ml_engine(statement)
        elif type(statement) == DropPredictor:
            database_name = self.session.database
            if len(statement.name.parts) > 1:
                database_name = statement.name.parts[0].lower()
            model_name = statement.name.parts[-1]

            try:
                project = self.session.database_controller.get_project(database_name)
                project.drop_table(model_name)
            except Exception as e:
                if not statement.if_exists:
                    raise e
            return ExecuteAnswer(ANSWER_TYPE.OK)
        elif type(statement) == DropTables:
            return self.answer_drop_tables(statement)
        elif type(statement) == DropDatasource or type(statement) == DropDatabase:
            return self.answer_drop_database(statement)
        elif type(statement) == Describe:
            # NOTE in sql 'describe table' is same as 'show columns'
            return self.answer_describe_predictor(statement)
        elif type(statement) == RetrainPredictor:
            return self.answer_retrain_predictor(statement)
        elif type(statement) == AdjustPredictor:
            return self.answer_adjust_predictor(statement)
        elif type(statement) == Show:
            sql_category = statement.category.lower()
            if hasattr(statement, "modes"):
                if isinstance(statement.modes, list) is False:
                    statement.modes = []
                statement.modes = [x.upper() for x in statement.modes]
            if sql_category in ("predictors", "models"):
                where = BinaryOperation("=", args=[Constant(1), Constant(1)])

                new_statement = Select(
                    targets=[Star()],
                    from_table=Identifier(parts=["information_schema", "models"]),
                    where=_get_show_where(
                        statement, from_name="project", like_name="name"
                    ),
                )
                query = SQLQuery(new_statement, session=self.session)
                return self.answer_select(query)
            elif sql_category == "ml_engines":
                new_statement = Select(
                    targets=[Star()],
                    from_table=Identifier(parts=["information_schema", "ml_engines"]),
                    where=_get_show_where(statement, like_name="name"),
                )

                query = SQLQuery(new_statement, session=self.session)
                return self.answer_select(query)
            elif sql_category == "handlers":
                new_statement = Select(
                    targets=[Star()],
                    from_table=Identifier(parts=["information_schema", "handlers"]),
                    where=_get_show_where(statement, like_name="name"),
                )

                query = SQLQuery(new_statement, session=self.session)
                return self.answer_select(query)
            elif sql_category == "plugins":
                if statement.where is not None or statement.like:
                    raise SqlApiException(
                        "'SHOW PLUGINS' query should be used without filters"
                    )
                new_statement = Select(
                    targets=[Star()],
                    from_table=Identifier(parts=["information_schema", "PLUGINS"]),
                )
                query = SQLQuery(new_statement, session=self.session)
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

                query = SQLQuery(new_statement, session=self.session)
                return self.answer_select(query)
            elif sql_category in ("tables", "full tables"):
                schema = self.session.database or "mindsdb"
                if statement.from_table is not None:
                    schema = statement.from_table.parts[-1]
                    statement.from_table = None
                where = BinaryOperation(
                    "and",
                    args=[
                        BinaryOperation(
                            "=", args=[Identifier("table_schema"), Constant(schema)]
                        ),
                        BinaryOperation(
                            "or",
                            args=[
                                BinaryOperation(
                                    "=",
                                    args=[Identifier("table_type"), Constant("MODEL")],
                                ),
                                BinaryOperation(
                                    "or",
                                    args=[
                                        BinaryOperation(
                                            "=",
                                            args=[
                                                Identifier("table_type"),
                                                Constant("BASE TABLE"),
                                            ],
                                        ),
                                        BinaryOperation(
                                            "or",
                                            args=[
                                                BinaryOperation(
                                                    "=",
                                                    args=[
                                                        Identifier("table_type"),
                                                        Constant("SYSTEM VIEW"),
                                                    ],
                                                ),
                                                BinaryOperation(
                                                    "=",
                                                    args=[
                                                        Identifier("table_type"),
                                                        Constant("VIEW"),
                                                    ],
                                                ),
                                            ],
                                        ),
                                    ],
                                ),
                            ],
                        ),
                    ],
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

                query = SQLQuery(new_statement, session=self.session)
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
                data = query_df(df, new_statement)
                data = data.values.tolist()

                columns = [
                    Column(
                        name="Variable_name", table_name="session_variables", type="str"
                    ),
                    Column(name="Value", table_name="session_variables", type="str"),
                ]

                return ExecuteAnswer(
                    answer_type=ANSWER_TYPE.TABLE, columns=columns, data=data
                )
            elif "show status like 'ssl_version'" in sql_lower:
                return ExecuteAnswer(
                    answer_type=ANSWER_TYPE.TABLE,
                    columns=[
                        Column(
                            name="Value", table_name="session_variables", type="str"
                        ),
                        Column(
                            name="Value", table_name="session_variables", type="str"
                        ),
                    ],
                    data=[["Ssl_version", "TLSv1.1"]],
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
                query = SQLQuery(new_statement, session=self.session)
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
                query = SQLQuery(new_statement, session=self.session)
                return self.answer_select(query)
            elif sql_category == "warnings":
                return self.answer_show_warnings()
            elif sql_category == "engines":
                new_statement = Select(
                    targets=[Star()],
                    from_table=Identifier(parts=["information_schema", "ENGINES"]),
                )
                query = SQLQuery(new_statement, session=self.session)
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
                query = SQLQuery(new_statement, session=self.session)
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
                    log.logger.warning(err_str)
                    raise ErTableExistError(err_str)
                return self.answer_show_table_status(table_name)
            elif sql_category == "columns":
                is_full = statement.modes is not None and "full" in statement.modes
                return self.answer_show_columns(
                    statement.from_table,
                    statement.where,
                    statement.like,
                    is_full=is_full,
                )
            else:
                raise ErNotSupportedYet(f"Statement not implemented: {sql}")
        elif type(statement) in (
            StartTransaction,
            CommitTransaction,
            RollbackTransaction,
        ):
            return ExecuteAnswer(ANSWER_TYPE.OK)
        elif type(statement) == Set:
            category = (statement.category or "").lower()
            if category == "" and type(statement.arg) == BinaryOperation:
                return ExecuteAnswer(ANSWER_TYPE.OK)
            elif category == "autocommit":
                return ExecuteAnswer(ANSWER_TYPE.OK)
            elif category == "names":
                # set names utf8;
                charsets = {
                    "utf8": CHARSET_NUMBERS["utf8_general_ci"],
                    "utf8mb4": CHARSET_NUMBERS["utf8mb4_general_ci"],
                }
                self.charset = statement.arg.parts[0]
                self.charset_text_type = charsets.get(self.charset)
                if self.charset_text_type is None:
                    log.logger.warning(
                        f"Unknown charset: {self.charset}. Setting up 'utf8_general_ci' as charset text type."
                    )
                    self.charset_text_type = CHARSET_NUMBERS["utf8_general_ci"]
                return ExecuteAnswer(
                    ANSWER_TYPE.OK,
                    state_track=[
                        ["character_set_client", self.charset],
                        ["character_set_connection", self.charset],
                        ["character_set_results", self.charset],
                    ],
                )
            else:
                log.logger.warning(
                    f"SQL statement is not processable, return OK package: {sql}"
                )
                return ExecuteAnswer(ANSWER_TYPE.OK)
        elif type(statement) == Use:
            db_name = statement.value.parts[-1]
            self.change_default_db(db_name)
            return ExecuteAnswer(ANSWER_TYPE.OK)
        elif type(statement) == CreatePredictor:
            return self.answer_create_predictor(statement)
        elif type(statement) == CreateView:
            return self.answer_create_view(statement)
        elif type(statement) == DropView:
            return self.answer_drop_view(statement)
        elif type(statement) == Delete:
            if statement.table.parts[-1].lower() == "models_versions":
                return self.answer_delete_model_version(statement)
            if (
                self.session.database != "mindsdb"
                and statement.table.parts[0] != "mindsdb"
            ):
                raise ErBadTableError(
                    "Only 'DELETE' from database 'mindsdb' is possible at this moment"
                )
            if statement.table.parts[-1] != "predictors":
                raise ErBadTableError(
                    "Only 'DELETE' from table 'mindsdb.models' is possible at this moment"
                )
            self.delete_predictor_query(statement)
            return ExecuteAnswer(ANSWER_TYPE.OK)
        elif type(statement) == Insert:
            if statement.from_select is None:
                raise ErNotSupportedYet(
                    "At this moment only 'insert from select' is supported."
                )
            else:
                SQLQuery(statement, session=self.session, execute=True)
                return ExecuteAnswer(ANSWER_TYPE.OK)
        elif type(statement) == Update:
            if statement.from_select is None:
                if statement.table.parts[-1].lower() == "models_versions":
                    return self.answer_update_model_version(statement)

                raise ErNotSupportedYet("Update is not implemented")
            else:
                SQLQuery(statement, session=self.session, execute=True)
                return ExecuteAnswer(ANSWER_TYPE.OK)
        elif (
            type(statement) == Alter
            and ("disable keys" in sql_lower)
            or ("enable keys" in sql_lower)
        ):
            return ExecuteAnswer(ANSWER_TYPE.OK)
        elif type(statement) == Select:
            if statement.from_table is None:
                return self.answer_single_row_select(statement)

            query = SQLQuery(statement, session=self.session)
            return self.answer_select(query)
        elif type(statement) == Union:
            query = SQLQuery(statement, session=self.session)
            return self.answer_select(query)
        elif type(statement) == Explain:
            return self.answer_show_columns(statement.target)
        elif type(statement) == CreateTable:
            # TODO
            return self.answer_apply_predictor(statement)
        # -- jobs --
        elif type(statement) == CreateJob:
            return self.answer_create_job(statement)
        elif type(statement) == DropJob:
            return self.answer_drop_job(statement)
        else:
            log.logger.warning(f"Unknown SQL statement: {sql}")
            raise ErNotSupportedYet(f"Unknown SQL statement: {sql}")

    def answer_create_job(self, statement):
        jobs_controller = JobsController()

        name = statement.name
        job_name = name.parts[-1]
        project_name = name.parts[-2] if len(name.parts) > 1 else self.session.database

        jobs_controller.add(job_name, project_name, statement.query_str,
                            statement.start_str, statement.end_str, statement.repeat_str)

        return ExecuteAnswer(ANSWER_TYPE.OK)

    def answer_drop_job(self, statement):
        jobs_controller = JobsController()

        name = statement.name
        job_name = name.parts[-1]
        project_name = name.parts[-2] if len(name.parts) > 1 else self.session.database
        jobs_controller.delete(job_name, project_name)

        return ExecuteAnswer(ANSWER_TYPE.OK)

    def answer_describe_predictor(self, statement):
        # describe attr
        predictor_attrs = ("model", "features", "ensemble")
        attribute = None
        if statement.value.parts[-1] in predictor_attrs:
            attribute = statement.value.parts.pop(-1)

        if len(statement.value.parts) > 1:
            project_name = statement.value.parts[0]
        else:
            project_name = self.session.database

        model_name = statement.value.parts[-1]

        df = self.session.model_controller.describe_model(self.session, project_name, model_name, attribute)

        df_dict = df.to_dict('split')

        columns = [
            Column(name=col, table_name='', type='str')
            for col in df_dict['columns']
        ]
        return ExecuteAnswer(
            answer_type=ANSWER_TYPE.TABLE,
            columns=columns,
            data=df_dict['data']
        )

    def _get_model_record(self, statement):
        if len(statement.name.parts) == 1:
            statement.name.parts = [self.session.database, statement.name.parts[0]]
        database_name, model_name = statement.name.parts

        model_record = get_model_record(
            name=model_name, project_name=database_name, except_absent=True
        )
        return model_record

    def _sync_predictor_check(self, phase_name):
        """ Checks if there is already a predictor retraining or adjusting """
        is_cloud = self.session.config.get('cloud', False)
        if is_cloud and ctx.user_class == 0:
            models = get_model_records(active=None)
            longest_training = None
            for p in models:
                if (
                        p.status in (PREDICTOR_STATUS.GENERATING, PREDICTOR_STATUS.TRAINING)
                        and p.training_start_at is not None and p.training_stop_at is None
                ):
                    training_time = datetime.datetime.now() - p.training_start_at
                    if longest_training is None or training_time > longest_training:
                        longest_training = training_time
            if longest_training is not None and longest_training > datetime.timedelta(hours=1):
                raise SqlApiException(
                    f"Can't start {phase_name} process while predictor is in status 'training' or 'generating'"
                )

    def answer_retrain_predictor(self, statement):
        model_record = self._get_model_record(statement)

        if statement.integration_name is None:
            if model_record.data_integration_ref is None:
                raise Exception("The model does not have an associated dataset")
            if model_record.data_integration_ref["type"] == "integration":
                integration = self.session.integration_controller.get_by_id(
                    model_record.data_integration_ref["id"]
                )
                if integration is None:
                    raise Exception(
                        "The database from which the model was trained no longer exists"
                    )

        ml_handler = None
        if statement.using is not None:
            # repack using with lower names
            statement.using = {k.lower(): v for k, v in statement.using.items()}

            if "engine" in statement.using:
                ml_integration_name = statement.using.pop("engine")
                ml_handler = self.session.integration_controller.get_handler(
                    ml_integration_name
                )

        # use current ml handler
        if ml_handler is None:
            integration_record = get_predictor_integration(model_record)
            if integration_record is None:
                raise Exception("ML engine model was trained with does not esxists")
            ml_handler = self.session.integration_controller.get_handler(
                integration_record.name
            )

        self._sync_predictor_check(phase_name='retrain')
        df = self.session.model_controller.retrain_model(statement, ml_handler)

        resp_dict = df.to_dict(orient='split')

        columns = [
            Column(col)
            for col in resp_dict['columns']
        ]

        return ExecuteAnswer(answer_type=ANSWER_TYPE.TABLE, columns=columns, data=resp_dict['data'])

    def answer_adjust_predictor(self, statement):
        model_record = self._get_model_record(statement)

        if statement.using is not None:
            # repack using with lower names
            statement.using = {k.lower(): v for k, v in statement.using.items()}

        # use current ml handler
        integration_record = get_predictor_integration(model_record)
        if integration_record is None:
            raise Exception('The ML engine that the model was trained with does not exist.')
        ml_handler = self.session.integration_controller.get_handler(integration_record.name)

        self._sync_predictor_check(phase_name='adjust')
        df = self.session.model_controller.adjust_model(statement, ml_handler)

        resp_dict = df.to_dict(orient='split')

        columns = [
            Column(col)
            for col in resp_dict['columns']
        ]

        return ExecuteAnswer(answer_type=ANSWER_TYPE.TABLE, columns=columns, data=resp_dict['data'])

    def _create_integration(self, name: str, engine: str, connection_args: dict):
        # we have connection checkers not for any db. So do nothing if fail
        # TODO return rich error message

        if connection_args is None:
            connection_args = {}
        status = HandlerStatusResponse(success=False)

        try:
            handlers_meta = (
                self.session.integration_controller.get_handlers_import_status()
            )
            handler_meta = handlers_meta[engine]
            if handler_meta.get("import", {}).get("success") is not True:
                raise SqlApiException(f"Handler '{engine}' can not be used")

            accept_connection_args = handler_meta.get("connection_args")
            if accept_connection_args is not None:
                for arg_name, arg_value in connection_args.items():
                    if arg_name == "as_service":
                        continue
                    if arg_name not in accept_connection_args:
                        raise SqlApiException(
                            f"Unknown connection argument: {arg_name}"
                        )
                    arg_meta = accept_connection_args[arg_name]
                    arg_type = arg_meta.get("type")
                    if arg_type == HANDLER_CONNECTION_ARG_TYPE.PATH:
                        # arg may be one of:
                        # str: '/home/file.pem'
                        # dict: {'path': '/home/file.pem'}
                        # dict: {'url': 'https://host.com/file'}
                        arg_value = connection_args[arg_name]
                        if isinstance(arg_value, (str, dict)) is False:
                            raise SqlApiException(f"Unknown type of arg: '{arg_value}'")
                        if isinstance(arg_value, str) or "path" in arg_value:
                            path = (
                                arg_value
                                if isinstance(arg_value, str)
                                else arg_value["path"]
                            )
                            if Path(path).is_file() is False:
                                raise SqlApiException(f"File not found at: '{path}'")
                        elif "url" in arg_value:
                            path = download_file(arg_value["url"])
                        else:
                            raise SqlApiException(
                                f"Argument '{arg_name}' must be path or url to the file"
                            )
                        connection_args[arg_name] = path

            handler = self.session.integration_controller.create_tmp_handler(
                handler_type=engine, connection_data=connection_args
            )
            status = handler.check_connection()
        except Exception as e:
            status.error_message = str(e)

        if status.success is False:
            raise SqlApiException(f"Can't connect to db: {status.error_message}")

        integration = self.session.integration_controller.get(name)
        if integration is not None:
            raise SqlApiException(f"Database '{name}' already exists.")

        self.session.integration_controller.add(name, engine, connection_args)

    def answer_create_ml_engine(self, statement: ASTNode):
        name = statement.name.parts[-1]
        integrations = self.session.integration_controller.get_all()
        if name in integrations:
            raise SqlApiException(f"Integration '{name}' already exists")

        handler_module_meta = (
            self.session.integration_controller.get_handlers_import_status().get(
                statement.handler
            )
        )
        if handler_module_meta is None:
            raise SqlApiException(f"There is no engine '{statement.handler}'")
        if handler_module_meta.get("import", {}).get("success") is not True:
            raise SqlApiException(f"Can't import engine '{statement.handler}'")

        self.session.integration_controller._add_integration_record(
            name=name, engine=statement.handler, connection_args=statement.params
        )

        return ExecuteAnswer(ANSWER_TYPE.OK)

    def answer_drop_ml_engine(self, statement: ASTNode):
        name = statement.name.parts[-1]
        integrations = self.session.integration_controller.get_all()
        if name not in integrations:
            raise SqlApiException(f"Integration '{name}' does not exists")
        self.session.integration_controller.delete(name)
        return ExecuteAnswer(ANSWER_TYPE.OK)

    def answer_create_database(self, statement: ASTNode):
        """create new handler (datasource/integration in old terms)
        Args:
            statement (ASTNode): data for creating database/project
        """

        database_name = statement.name
        engine = statement.engine
        if engine is None:
            engine = "mindsdb"
        engine = engine.lower()
        connection_args = statement.parameters

        if engine == "mindsdb":
            ProjectController().add(database_name)
        else:
            self._create_integration(database_name, engine, connection_args)

        return ExecuteAnswer(ANSWER_TYPE.OK)

    def answer_drop_database(self, statement):
        if len(statement.name.parts) != 1:
            raise Exception("Database name should contain only 1 part.")
        db_name = statement.name.parts[0]
        self.session.database_controller.delete(db_name)
        return ExecuteAnswer(ANSWER_TYPE.OK)

    def answer_drop_tables(self, statement):
        """answer on 'drop table [if exists] {name}'
        Args:
            statement: ast
        """
        if statement.if_exists is False:
            for table in statement.tables:
                if len(table.parts) > 1:
                    db_name = table.parts[0]
                else:
                    db_name = self.session.database
                table_name = table.parts[-1]

                if db_name == "files":
                    dn = self.session.datahub[db_name]
                    if dn.has_table(table_name) is False:
                        raise SqlApiException(
                            f"Cannot delete a table from database '{db_name}': table does not exists"
                        )
                else:
                    projects_dict = self.session.database_controller.get_dict(
                        filter_type="project"
                    )
                    if db_name not in projects_dict:
                        raise SqlApiException(
                            f"Cannot delete a table from database '{db_name}'"
                        )
                    project = self.session.database_controller.get_project(db_name)
                    project_tables = {
                        key: val
                        for key, val in project.get_tables().items()
                        if val.get("deletable") is True
                    }
                    if table_name not in project_tables:
                        raise SqlApiException(
                            f"Cannot delete a table from database '{db_name}': table does not exists"
                        )

        for table in statement.tables:
            if len(table.parts) > 1:
                db_name = table.parts[0]
            else:
                db_name = self.session.database
            table_name = table.parts[-1]

            if db_name == "files":
                dn = self.session.datahub[db_name]
                if dn.has_table(table_name):
                    self.session.datahub["files"].query(
                        DropTables(tables=[Identifier(table_name)])
                    )
            else:
                projects_dict = self.session.database_controller.get_dict(
                    filter_type="project"
                )
                if db_name not in projects_dict:
                    continue
                self.session.model_controller.delete_model(
                    table_name, project_name=db_name
                )
        return ExecuteAnswer(ANSWER_TYPE.OK)

    def answer_create_view(self, statement):
        project_name = self.session.database
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
            renderer = SqlalchemyRender("mysql")
            query_str = renderer.get_string(query, with_failback=True)

        if isinstance(query, Select):
            # check create view sql
            query.limit = Constant(1)

            sqlquery = SQLQuery(query, session=self.session)
            if sqlquery.fetch()["success"] is not True:
                raise SqlApiException("Wrong view query")

        project = self.session.database_controller.get_project(project_name)
        project.create_view(view_name, query=query_str)
        return ExecuteAnswer(answer_type=ANSWER_TYPE.OK)

    def answer_drop_view(self, statement):
        names = statement.names

        for name in names:
            view_name = name.parts[-1]
            if len(name.parts) > 1:
                db_name = name.parts[0]
            else:
                db_name = self.session.database
            project = self.session.database_controller.get_project(db_name)
            project.drop_table(view_name)

        return ExecuteAnswer(answer_type=ANSWER_TYPE.OK)

    def answer_create_predictor(self, statement):
        integration_name = self.session.database
        if len(statement.name.parts) > 1:
            integration_name = statement.name.parts[0]
        else:
            statement.name.parts = [integration_name, statement.name.parts[-1]]
        integration_name = integration_name.lower()

        ml_integration_name = "lightwood"
        if statement.using is not None:
            # repack using with lower names
            statement.using = {k.lower(): v for k, v in statement.using.items()}

            ml_integration_name = statement.using.pop("engine", ml_integration_name)

        ml_handler = self.session.integration_controller.get_handler(
            ml_integration_name
        )

        df = self.session.model_controller.create_model(statement, ml_handler)
        resp_dict = df.to_dict(orient='split')

        columns = [
            Column(col)
            for col in resp_dict['columns']
        ]

        return ExecuteAnswer(answer_type=ANSWER_TYPE.TABLE, columns=columns, data=resp_dict['data'])

    def delete_predictor_query(self, query):

        query2 = Select(
            targets=[Identifier("name")], from_table=query.table, where=query.where
        )

        sqlquery = SQLQuery(query2.to_string(), session=self.session)

        result = sqlquery.fetch(self.session.datahub)

        predictors_names = [x[0] for x in result["result"]]

        if len(predictors_names) == 0:
            raise SqlApiException("nothing to delete")

        for predictor_name in predictors_names:
            self.session.datahub["mindsdb"].delete_predictor(predictor_name)

    def answer_show_columns(
        self,
        target: Identifier,
        where: Optional[Operation] = None,
        like: Optional[str] = None,
        is_full=False,
    ):
        if len(target.parts) > 1:
            db = target.parts[0]
        elif isinstance(self.session.database, str) and len(self.session.database) > 0:
            db = self.session.database
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

        query = SQLQuery(new_statement, session=self.session)
        return self.answer_select(query)

    def answer_single_row_select(self, statement):
        columns = []
        data = []
        for target in statement.targets:
            target_type = type(target)
            if target_type == Variable:
                var_name = target.value
                column_name = f"@@{var_name}"
                column_alias = target.alias or column_name
                result = SERVER_VARIABLES.get(column_name)
                if result is None:
                    log.logger.error(f"Unknown variable: {column_name}")
                    raise Exception(f"Unknown variable '{var_name}'")
                else:
                    result = result[0]
            elif target_type == Function:
                function_name = target.op.lower()
                if function_name == "connection_id":
                    return self.answer_connection_id()

                functions_results = {
                    # 'connection_id': self.executor.sqlserver.connection_id,
                    "database": self.session.database,
                    "current_user": self.session.username,
                    "user": self.session.username,
                    "version": "8.0.17",
                }

                column_name = f"{target.op}()"
                column_alias = target.alias or column_name
                result = functions_results[function_name]
            elif target_type == Constant:
                result = target.value
                column_name = str(result)
                column_alias = (
                    ".".join(target.alias.parts)
                    if type(target.alias) == Identifier
                    else column_name
                )
            elif target_type == NullConstant:
                result = None
                column_name = "NULL"
                column_alias = "NULL"
            elif target_type == Identifier:
                result = ".".join(target.parts)
                raise Exception(f"Unknown column '{result}'")
            else:
                raise ErSqlWrongArguments(f"Unknown constant type: {target_type}")

            columns.append(
                Column(
                    name=column_name,
                    alias=column_alias,
                    table_name="",
                    type=TYPES.MYSQL_TYPE_VAR_STRING
                    if isinstance(result, str)
                    else TYPES.MYSQL_TYPE_LONG,
                    charset=self.charset_text_type
                    if isinstance(result, str)
                    else CHARSET_NUMBERS["binary"],
                )
            )
            data.append(result)

        return ExecuteAnswer(
            answer_type=ANSWER_TYPE.TABLE, columns=columns, data=[data]
        )

    def answer_show_create_table(self, table):
        columns = [
            Column(table_name="", name="Table", type=TYPES.MYSQL_TYPE_VAR_STRING),
            Column(
                table_name="", name="Create Table", type=TYPES.MYSQL_TYPE_VAR_STRING
            ),
        ]
        return ExecuteAnswer(
            answer_type=ANSWER_TYPE.TABLE,
            columns=columns,
            data=[[table, f"create table {table} ()"]],
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

        return ExecuteAnswer(answer_type=ANSWER_TYPE.TABLE, columns=columns, data=[])

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
        return ExecuteAnswer(answer_type=ANSWER_TYPE.TABLE, columns=columns, data=data)

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
        return ExecuteAnswer(answer_type=ANSWER_TYPE.TABLE, columns=columns, data=[])

    def answer_connection_id(self):
        columns = [
            {
                "database": "",
                "table_name": "",
                "name": "conn_id",
                "alias": "conn_id",
                "type": TYPES.MYSQL_TYPE_LONG,
                "charset": CHARSET_NUMBERS["binary"],
            }
        ]
        columns = [Column(**d) for d in columns]
        data = [[self.executor.sqlserver.connection_id]]
        return ExecuteAnswer(answer_type=ANSWER_TYPE.TABLE, columns=columns, data=data)

    def answer_apply_predictor(self, statement):
        SQLQuery(statement, session=self.session, execute=True)
        return ExecuteAnswer(ANSWER_TYPE.OK)

    def answer_select(self, query):
        data = query.fetch()

        return ExecuteAnswer(
            answer_type=ANSWER_TYPE.TABLE,
            columns=query.columns_list,
            data=data["result"],
        )

    def answer_update_model_version(self, statement):

        # get project name
        if len(statement.table.parts) > 1:
            project_name = statement.table.parts[0]
        else:
            project_name = self.session.database

        project_datanode = self.datahub.get(project_name)
        if project_datanode is None:
            raise Exception(f"Project not found: {project_name}")

        # get list of model versions using filter
        query = Select(
            targets=[Identifier("version"), Identifier("name"), Identifier("project")],
            from_table=Identifier("models_versions"),
            where=statement.where,
        )

        models, _ = project_datanode.query(query=query, session=self.session)

        # get columns for update
        kwargs = {}
        for k, v in statement.update_columns.items():
            if isinstance(v, Constant):
                v = v.value
            kwargs[k.lower()] = v
        self.session.model_controller.update_model_version(models, **kwargs)
        return ExecuteAnswer(ANSWER_TYPE.OK)

    def answer_delete_model_version(self, statement):
        # get project name
        if len(statement.table.parts) > 1:
            project_name = statement.table.parts[0]
        else:
            project_name = self.session.database

        project_datanode = self.datahub.get(project_name)
        if project_datanode is None:
            raise Exception(f"Project not found: {project_name}")

        # get list of model versions using filter
        query = Select(
            targets=[Identifier("version"), Identifier("name"), Identifier("project")],
            from_table=Identifier("models_versions"),
            where=statement.where,
        )

        models, _ = project_datanode.query(query=query, session=self.session)

        self.session.model_controller.delete_model_version(models)
        return ExecuteAnswer(ANSWER_TYPE.OK)

    def change_default_db(self, db_name):
        # That fix for bug in mssql: it keeps connection for a long time, but after some time mssql can
        # send packet with COM_INIT_DB=null. In this case keep old database name as default.
        if db_name != "null":
            if self.session.database_controller.exists(db_name):
                self.session.database = db_name
            else:
                raise ErBadDbError(f"Database {db_name} does not exists")
