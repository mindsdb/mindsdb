import json

import pandas as pd
from mindsdb_sql.parser.ast import BinaryOperation, Constant, Select
from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.interfaces.agents.agents_controller import AgentsController
from mindsdb.interfaces.jobs.jobs_controller import JobsController
from mindsdb.interfaces.skills.skills_controller import SkillsController
from mindsdb.interfaces.database.views import ViewController
from mindsdb.interfaces.database.projects import ProjectController

from mindsdb.api.executor.datahub.datanodes.system_tables import Table


def to_json(obj):
    if obj is None:
        return None
    try:
        return json.dumps(obj)
    except TypeError:
        return obj


def get_project_name(query: ASTNode = None):
    project_name = None
    if (
            isinstance(query, Select)
            and type(query.where) is BinaryOperation
            and query.where.op == '='
            and query.where.args[0].parts == ['project']
            and isinstance(query.where.args[1], Constant)
    ):
        project_name = query.where.args[1].value
    return project_name


class MdbTable(Table):

    visible: bool = True


class ModelsTable(MdbTable):
    name = "MODELS"
    columns = [
        "NAME",
        "ENGINE",
        "PROJECT",
        "ACTIVE",
        "VERSION",
        "STATUS",
        "ACCURACY",
        "PREDICT",
        "UPDATE_STATUS",
        "MINDSDB_VERSION",
        "ERROR",
        "SELECT_DATA_QUERY",
        "TRAINING_OPTIONS",
        "CURRENT_TRAINING_PHASE",
        "TOTAL_TRAINING_PHASES",
        "TRAINING_PHASE_NAME",
        "TAG",
        "CREATED_AT",
        "TRAINING_TIME",
    ]

    @classmethod
    def get_data(cls, session, inf_schema, **kwargs):
        data = []
        for project_name in inf_schema.get_projects_names():
            project = inf_schema.database_controller.get_project(name=project_name)
            project_models = project.get_models(active=None, with_secrets=session.show_secrets)
            for row in project_models:
                table_name = row["name"]
                table_meta = row["metadata"]

                data.append([
                    table_name,
                    table_meta["engine"],
                    project_name,
                    table_meta["active"],
                    table_meta["version"],
                    table_meta["status"],
                    table_meta["accuracy"],
                    table_meta["predict"],
                    table_meta["update_status"],
                    table_meta["mindsdb_version"],
                    table_meta["error"],
                    table_meta["select_data_query"],
                    to_json(table_meta["training_options"]),
                    table_meta["current_training_phase"],
                    table_meta["total_training_phases"],
                    table_meta["training_phase_name"],
                    table_meta["label"],
                    row["created_at"],
                    table_meta["training_time"],
                ])
            # TODO optimise here
            # if target_table is not None and target_table != project_name:
            #     continue

        df = pd.DataFrame(data, columns=cls.columns)
        return df


class DatabasesTable(MdbTable):
    name = "DATABASES"
    columns = ["NAME", "TYPE", "ENGINE", "CONNECTION_DATA"]

    @classmethod
    def get_data(cls, session, inf_schema, **kwargs):

        project = inf_schema.database_controller.get_list(with_secrets=session.show_secrets)
        data = [
            [x["name"], x["type"], x["engine"], to_json(x.get("connection_data"))]
            for x in project
        ]

        df = pd.DataFrame(data, columns=cls.columns)
        return df


class MLEnginesTable(MdbTable):
    name = "ML_ENGINES"
    columns = [
        "NAME", "HANDLER", "CONNECTION_DATA"
    ]

    @classmethod
    def get_data(cls, session, inf_schema, **kwargs):

        integrations = inf_schema.integration_controller.get_all(show_secrets=session.show_secrets)
        ml_integrations = {
            key: val for key, val in integrations.items() if val["type"] == "ml"
        }

        data = []
        for _key, val in ml_integrations.items():
            data.append([val["name"], val.get("engine"), to_json(val.get("connection_data"))])

        df = pd.DataFrame(data, columns=cls.columns)
        return df


class HandlersTable(MdbTable):
    name = "HANDLERS"
    columns = [
        "NAME",
        "TYPE",
        "TITLE",
        "DESCRIPTION",
        "VERSION",
        "CONNECTION_ARGS",
        "IMPORT_SUCCESS",
        "IMPORT_ERROR",
    ]

    @classmethod
    def get_data(cls, inf_schema, **kwargs):

        handlers = inf_schema.integration_controller.get_handlers_import_status()

        data = []
        for _key, val in handlers.items():
            connection_args = val.get("connection_args")
            if connection_args is not None:
                connection_args = to_json(connection_args)
            import_success = val.get("import", {}).get("success")
            import_error = val.get("import", {}).get("error_message")
            data.append(
                [
                    val["name"],
                    val.get("type"),
                    val.get("title"),
                    val.get("description"),
                    val.get("version"),
                    connection_args,
                    import_success,
                    import_error,
                ]
            )

        df = pd.DataFrame(data, columns=cls.columns)
        return df


class JobsTable(MdbTable):
    name = "JOBS"
    columns = [
        "NAME",
        "PROJECT",
        "START_AT",
        "END_AT",
        "NEXT_RUN_AT",
        "SCHEDULE_STR",
        "QUERY",
        "IF_QUERY",
        "VARIABLES",
    ]

    @classmethod
    def get_data(cls, query: ASTNode = None, **kwargs):
        jobs_controller = JobsController()

        project_name = None
        if (
            isinstance(query, Select)
            and type(query.where) is BinaryOperation
            and query.where.op == "="
            and query.where.args[0].parts == ["project"]
            and isinstance(query.where.args[1], Constant)
        ):
            project_name = query.where.args[1].value

        data = jobs_controller.get_list(project_name)

        columns = cls.columns
        columns_lower = [col.lower() for col in columns]

        # to list of lists
        data = [[row[k] for k in columns_lower] for row in data]

        return pd.DataFrame(data, columns=columns)


class TriggersTable(MdbTable):
    name = "TRIGGERS"
    columns = [
        "TRIGGER_CATALOG",
        "TRIGGER_SCHEMA",
        "TRIGGER_NAME",
        "EVENT_MANIPULATION",
        "EVENT_OBJECT_CATALOG",
        "EVENT_OBJECT_SCHEMA",
        "EVENT_OBJECT_TABLE",
        "ACTION_ORDER",
        "ACTION_CONDITION",
        "ACTION_STATEMENT",
        "ACTION_ORIENTATION",
        "ACTION_TIMING",
        "ACTION_REFERENCE_OLD_TABLE",
        "ACTION_REFERENCE_NEW_TABLE",
        "ACTION_REFERENCE_OLD_ROW",
        "ACTION_REFERENCE_NEW_ROW",
        "CREATED",
        "SQL_MODE",
        "DEFINER",
        "CHARACTER_SET_CLIENT",
        "COLLATION_CONNECTION",
        "DATABASE_COLLATION",
    ]

    mindsdb_columns = ["NAME", "PROJECT", "DATABASE", "TABLE", "QUERY", "LAST_ERROR"]

    @classmethod
    def get_data(cls, query: ASTNode = None, inf_schema=None, **kwargs):
        from mindsdb.interfaces.triggers.triggers_controller import TriggersController

        triggers_controller = TriggersController()

        project_name = None
        if (
            isinstance(query, Select)
            and type(query.where) is BinaryOperation
            and query.where.op == "="
            and query.where.args[0].parts == ["project"]
            and isinstance(query.where.args[1], Constant)
        ):
            project_name = query.where.args[1].value

        data = triggers_controller.get_list(project_name)

        columns = cls.mindsdb_columns
        if inf_schema.session.api_type == 'sql':
            columns = columns + cls.columns
        columns_lower = [col.lower() for col in columns]

        # to list of lists
        data = [[row.get(k) for k in columns_lower] for row in data]

        return pd.DataFrame(data, columns=columns)


class ChatbotsTable(MdbTable):
    name = 'CHATBOTS'
    columns = [
        "NAME",
        "PROJECT",
        "DATABASE",
        "MODEL_NAME",
        "PARAMS",
        "IS_RUNNING",
        "LAST_ERROR",
        "WEBHOOK_TOKEN",
    ]

    @classmethod
    def get_data(cls, query: ASTNode = None, **kwargs):
        from mindsdb.interfaces.chatbot.chatbot_controller import ChatBotController

        chatbot_controller = ChatBotController()

        project_name = None
        if (
            isinstance(query, Select)
            and type(query.where) is BinaryOperation
            and query.where.op == "="
            and query.where.args[0].parts == ["project"]
            and isinstance(query.where.args[1], Constant)
        ):
            project_name = query.where.args[1].value

        chatbot_data = chatbot_controller.get_chatbots(project_name)

        columns = cls.columns
        columns_lower = [col.lower() for col in columns]

        # to list of lists
        data = []
        for row in chatbot_data:
            row['params'] = to_json(row['params'])
            data.append([row[k] for k in columns_lower])

        return pd.DataFrame(data, columns=columns)


class KBTable(MdbTable):
    name = 'KNOWLEDGE_BASES'
    columns = ["NAME", "PROJECT", "MODEL", "STORAGE", "PARAMS"]

    @classmethod
    def get_data(cls, query: ASTNode = None, inf_schema=None, **kwargs):
        project_name = get_project_name(query)

        from mindsdb.interfaces.knowledge_base.controller import KnowledgeBaseController
        controller = KnowledgeBaseController(inf_schema.session)
        kb_list = controller.list(project_name)

        data = []

        for kb in kb_list:
            vector_database_name = kb['vector_database'] or ''

            data.append((
                kb['name'],
                kb['project_name'],
                kb['embedding_model'],
                vector_database_name + '.' + kb['vector_database_table'],
                to_json(kb['params']),
            ))

        return pd.DataFrame(data, columns=cls.columns)


class SkillsTable(MdbTable):
    name = 'SKILLS'
    columns = ["NAME", "PROJECT", "TYPE", "PARAMS"]

    @classmethod
    def get_data(cls, query: ASTNode = None, **kwargs):
        skills_controller = SkillsController()

        project_name = get_project_name(query)

        all_skills = skills_controller.get_skills(project_name)

        project_controller = ProjectController()
        project_names = {p.id: p.name for p in project_controller.get_list()}

        # NAME, PROJECT, TYPE, PARAMS
        data = [(s.name, project_names[s.project_id], s.type, s.params) for s in all_skills]
        return pd.DataFrame(data, columns=cls.columns)


class AgentsTable(MdbTable):
    name = 'AGENTS'
    columns = [
        "NAME",
        "PROJECT",
        "MODEL_NAME",
        "SKILLS",
        "PARAMS"
    ]

    @classmethod
    def get_data(cls, query: ASTNode = None, inf_schema=None, **kwargs):
        agents_controller = AgentsController()

        project_name = get_project_name(query)
        all_agents = agents_controller.get_agents(project_name)

        project_controller = ProjectController()
        project_names = {
            i.id: i.name
            for i in project_controller.get_list()
        }

        # NAME, PROJECT, MODEL, SKILLS, PARAMS
        data = [
            (
                a.name,
                project_names[a.project_id],
                a.model_name,
                list(map(lambda s: s.name, a.skills)),
                to_json(a.params)
            )
            for a in all_agents
        ]
        return pd.DataFrame(data, columns=cls.columns)


class ViewsTable(MdbTable):
    name = 'VIEWS'
    columns = ["NAME", "PROJECT", "QUERY"]

    @classmethod
    def get_data(cls, query: ASTNode = None, **kwargs):

        project_name = get_project_name(query)

        data = ViewController().list(project_name)

        columns_lower = [col.lower() for col in cls.columns]

        # to list of lists
        data = [[row[k] for k in columns_lower] for row in data]

        return pd.DataFrame(data, columns=cls.columns)
