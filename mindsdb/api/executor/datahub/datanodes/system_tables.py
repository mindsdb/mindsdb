
import pandas as pd
from mindsdb_sql_parser.ast import BinaryOperation, Constant, Identifier, Select
from mindsdb_sql_parser.ast.base import ASTNode
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions

from mindsdb.api.executor.datahub.classes.tables_row import (
    TABLES_ROW_TYPE,
    TablesRow,
)
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class Table:

    deletable: bool = False
    visible: bool = False
    kind: str = 'table'


class SchemataTable(Table):
    name = 'SCHEMATA'
    columns = [
        "CATALOG_NAME",
        "SCHEMA_NAME",
        "DEFAULT_CHARACTER_SET_NAME",
        "DEFAULT_COLLATION_NAME",
        "SQL_PATH",
    ]

    @classmethod
    def get_data(cls, inf_schema=None, **kwargs):

        databases_meta = inf_schema.session.database_controller.get_list()
        data = [
            ["def", x["name"], "utf8mb4", "utf8mb4_0900_ai_ci", None]
            for x in databases_meta
        ]

        df = pd.DataFrame(data, columns=cls.columns)
        return df


class TablesTable(Table):
    name = 'TABLES'

    columns = [
        "TABLE_CATALOG",
        "TABLE_SCHEMA",
        "TABLE_NAME",
        "TABLE_TYPE",
        "ENGINE",
        "VERSION",
        "ROW_FORMAT",
        "TABLE_ROWS",
        "AVG_ROW_LENGTH",
        "DATA_LENGTH",
        "MAX_DATA_LENGTH",
        "INDEX_LENGTH",
        "DATA_FREE",
        "AUTO_INCREMENT",
        "CREATE_TIME",
        "UPDATE_TIME",
        "CHECK_TIME",
        "TABLE_COLLATION",
        "CHECKSUM",
        "CREATE_OPTIONS",
        "TABLE_COMMENT",
    ]

    @classmethod
    def get_data(cls, query: ASTNode = None, inf_schema=None, **kwargs):

        target_table = None
        if (
            type(query) is Select
            and type(query.where) is BinaryOperation
            and query.where.op == "and"
        ):
            for arg in query.where.args:
                if (
                    type(arg) is BinaryOperation
                    and arg.op == "="
                    and type(arg.args[0]) is Identifier
                    and arg.args[0].parts[-1].upper() == "TABLE_SCHEMA"
                    and type(arg.args[1]) is Constant
                ):
                    target_table = arg.args[1].value
                    break

        data = []
        for name in inf_schema.tables.keys():
            if target_table is not None and target_table != name:
                continue
            row = TablesRow(TABLE_TYPE=TABLES_ROW_TYPE.SYSTEM_VIEW, TABLE_NAME=name)
            data.append(row.to_list())

        for ds_name, ds in inf_schema.persis_datanodes.items():
            if target_table is not None and target_table != ds_name:
                continue
            if hasattr(ds, 'get_tables_rows'):
                ds_tables = ds.get_tables_rows()
            else:
                ds_tables = ds.get_tables()
            if len(ds_tables) == 0:
                continue
            elif isinstance(ds_tables[0], dict):
                ds_tables = [
                    TablesRow(
                        TABLE_TYPE=TABLES_ROW_TYPE.BASE_TABLE, TABLE_NAME=x["name"]
                    )
                    for x in ds_tables
                ]
            elif (
                isinstance(ds_tables, list)
                and len(ds_tables) > 0
                and isinstance(ds_tables[0], str)
            ):
                ds_tables = [
                    TablesRow(TABLE_TYPE=TABLES_ROW_TYPE.BASE_TABLE, TABLE_NAME=x)
                    for x in ds_tables
                ]
            for row in ds_tables:
                row.TABLE_SCHEMA = ds_name
                data.append(row.to_list())

        for ds_name in inf_schema.get_integrations_names():
            if target_table is not None and target_table != ds_name:
                continue
            try:
                ds = inf_schema.get(ds_name)
                ds_tables = ds.get_tables()
                for row in ds_tables:
                    row.TABLE_SCHEMA = ds_name
                    data.append(row.to_list())
            except Exception:
                logger.error(f"Can't get tables from '{ds_name}'")

        for project_name in inf_schema.get_projects_names():
            if target_table is not None and target_table != project_name:
                continue
            project_dn = inf_schema.get(project_name)
            project_tables = project_dn.get_tables()
            for row in project_tables:
                row.TABLE_SCHEMA = project_name
                data.append(row.to_list())

        df = pd.DataFrame(data, columns=cls.columns)
        return df


class ColumnsTable(Table):

    name = 'COLUMNS'
    columns = [
        "TABLE_CATALOG",
        "TABLE_SCHEMA",
        "TABLE_NAME",
        "COLUMN_NAME",
        "ORDINAL_POSITION",
        "COLUMN_DEFAULT",
        "IS_NULLABLE",
        "DATA_TYPE",
        "CHARACTER_MAXIMUM_LENGTH",
        "CHARACTER_OCTET_LENGTH",
        "NUMERIC_PRECISION",
        "NUMERIC_SCALE",
        "DATETIME_PRECISION",
        "CHARACTER_SET_NAME",
        "COLLATION_NAME",
        "COLUMN_TYPE",
        "COLUMN_KEY",
        "EXTRA",
        "PRIVILEGES",
        "COLUMN_COMMENT",
        "GENERATION_EXPRESSION",
    ]

    @classmethod
    def get_data(cls, inf_schema=None, query: ASTNode = None, **kwargs):

        # NOTE there is a lot of types in mysql, but listed below should be enough for our purposes
        row_templates = {
            "text": [
                "def",
                "SCHEMA_NAME",
                "TABLE_NAME",
                "COLUMN_NAME",
                "COL_INDEX",
                None,
                "YES",
                "varchar",
                1024,
                3072,
                None,
                None,
                None,
                "utf8",
                "utf8_bin",
                "varchar(1024)",
                None,
                None,
                "select",
                None,
                None,
            ],
            "timestamp": [
                "def",
                "SCHEMA_NAME",
                "TABLE_NAME",
                "COLUMN_NAME",
                "COL_INDEX",
                "CURRENT_TIMESTAMP",
                "YES",
                "timestamp",
                None,
                None,
                None,
                None,
                0,
                None,
                None,
                "timestamp",
                None,
                None,
                "select",
                None,
                None,
            ],
            "bigint": [
                "def",
                "SCHEMA_NAME",
                "TABLE_NAME",
                "COLUMN_NAME",
                "COL_INDEX",
                None,
                "YES",
                "bigint",
                None,
                None,
                20,
                0,
                None,
                None,
                None,
                "bigint unsigned",
                None,
                None,
                "select",
                None,
                None,
            ],
            "float": [
                "def",
                "SCHEMA_NAME",
                "TABLE_NAME",
                "COLUMN_NAME",
                "COL_INDEX",
                None,
                "YES",
                "float",
                None,
                None,
                12,
                0,
                None,
                None,
                None,
                "float",
                None,
                None,
                "select",
                None,
                None,
            ],
        }

        result = []

        databases = None
        conditions = extract_comparison_conditions(query.where)
        for op, arg1, arg2 in conditions:
            if arg1.lower() == 'table_schema':
                if op == '=':
                    databases = [arg2]
                elif op == 'in':
                    if not isinstance(arg2, list):
                        arg2 = [arg2]
                    databases = arg2

        if databases is None:
            databases = ['information_schema', 'mindsdb', 'files']

        for db_name in databases:
            tables = {}
            if db_name == 'information_schema':
                for table_name, table in inf_schema.tables.items():
                    tables[table_name] = [
                        {'name': name} for name in table.columns
                    ]
            else:
                dn = inf_schema.get(db_name)
                if dn is None:
                    continue
                for table_row in dn.get_tables():
                    tables[table_row.TABLE_NAME] = dn.get_table_columns(table_row.TABLE_NAME)

            for table_name, table_columns in tables.items():
                for i, column in enumerate(table_columns):
                    column_name = column['name']
                    column_type = column.get('type', 'text')
                    if column_type in ('double precision', 'real', 'numeric'):
                        column_type = 'float'
                    elif column_type in ('integer', 'smallint', 'int'):
                        column_type = 'bigint'
                    elif column_type in ('timestamp without time zone', 'timestamp with time zone', 'date'):
                        column_type = 'timestamp'
                    elif column_type not in row_templates:
                        column_type = 'text'
                    result_row = row_templates[column_type].copy()
                    result_row[1] = db_name
                    result_row[2] = table_name
                    result_row[3] = column_name
                    result_row[4] = i
                    result.append(result_row)

        df = pd.DataFrame(result, columns=cls.columns)
        return df


class EventsTable(Table):
    name = "EVENTS"

    columns = [
        "EVENT_CATALOG",
        "EVENT_SCHEMA",
        "EVENT_NAME",
        "DEFINER",
        "TIME_ZONE",
        "EVENT_BODY",
        "EVENT_DEFINITION",
        "EVENT_TYPE",
        "EXECUTE_AT",
        "INTERVAL_VALUE",
        "INTERVAL_FIELD",
        "SQL_MODE",
        "STARTS",
        "ENDS",
        "STATUS",
        "ON_COMPLETION",
        "CREATED",
        "LAST_ALTERED",
        "LAST_EXECUTED",
        "EVENT_COMMENT",
        "ORIGINATOR",
        "CHARACTER_SET_CLIENT",
        "COLLATION_CONNECTION",
        "DATABASE_COLLATION",
    ]


class RoutinesTable(Table):
    name = "ROUTINE"
    columns = [
        "SPECIFIC_NAME",
        "ROUTINE_CATALOG",
        "ROUTINE_SCHEMA",
        "ROUTINE_NAME",
        "ROUTINE_TYPE",
        "DATA_TYPE",
        "CHARACTER_MAXIMUM_LENGTH",
        "CHARACTER_OCTET_LENGTH",
        "NUMERIC_PRECISION",
        "NUMERIC_SCALE",
        "DATETIME_PRECISION",
        "CHARACTER_SET_NAME",
        "COLLATION_NAME",
        "DTD_IDENTIFIER",
        "ROUTINE_BODY",
        "ROUTINE_DEFINITION",
        "EXTERNAL_NAME",
        "EXTERNAL_LANGUAGE",
        "PARAMETER_STYLE",
        "IS_DETERMINISTIC",
        "SQL_DATA_ACCESS",
        "SQL_PATH",
        "SECURITY_TYPE",
        "CREATED",
        "LAST_ALTERED",
        "SQL_MODE",
        "ROUTINE_COMMENT",
        "DEFINER",
        "CHARACTER_SET_CLIENT",
        "COLLATION_CONNECTION",
        "DATABASE_COLLATION",
    ]


class PluginsTable(Table):
    name = "PLUGINS"
    columns = [
        "PLUGIN_NAME",
        "PLUGIN_VERSION",
        "PLUGIN_STATUS",
        "PLUGIN_TYPE",
        "PLUGIN_TYPE_VERSION",
        "PLUGIN_LIBRARY",
        "PLUGIN_LIBRARY_VERSION",
        "PLUGIN_AUTHOR",
        "PLUGIN_DESCRIPTION",
        "PLUGIN_LICENSE",
        "LOAD_OPTION",
        "PLUGIN_MATURITY",
        "PLUGIN_AUTH_VERSION",
    ]


class EnginesTable(Table):
    name = "ENGINES"
    columns = ["ENGINE", "SUPPORT", "COMMENT", "TRANSACTIONS", "XA", "SAVEPOINTS"]

    @classmethod
    def get_data(cls, **kwargs):
        data = [
            [
                "InnoDB",
                "DEFAULT",
                "Supports transactions, row-level locking, and foreign keys",
                "YES",
                "YES",
                "YES",
            ]
        ]

        df = pd.DataFrame(data, columns=cls.columns)
        return df


class KeyColumnUsageTable(Table):
    name = "KEY_COLUMN_USAGE"
    columns = [
        "CONSTRAINT_CATALOG",
        "CONSTRAINT_SCHEMA",
        "CONSTRAINT_NAME",
        "TABLE_CATALOG",
        "TABLE_SCHEMA",
        "TABLE_NAME",
        "COLUMN_NAME",
        "ORDINAL_POSITION",
        "POSITION_IN_UNIQUE_CONSTRAINT",
        "REFERENCED_TABLE_SCHEMA",
        "REFERENCED_TABLE_NAME",
        "REFERENCED_COLUMN_NAME",
    ]


class StatisticsTable(Table):
    name = "STATISTICS"
    columns = [
        "TABLE_CATALOG",
        "TABLE_SCHEMA",
        "TABLE_NAME",
        "NON_UNIQUE",
        "INDEX_SCHEMA",
        "INDEX_NAME",
        "SEQ_IN_INDEX",
        "COLUMN_NAME",
        "COLLATION",
        "CARDINALITY",
        "SUB_PART",
        "PACKED",
        "NULLABLE",
        "INDEX_TYPE",
        "COMMENT",
        "INDEX_COMMENT",
        "IS_VISIBLE",
        "EXPRESSION",
    ]


class CharacterSetsTable(Table):
    name = "CHARACTER_SETS"
    columns = [
        "CHARACTER_SET_NAME",
        "DEFAULT_COLLATE_NAME",
        "DESCRIPTION",
        "MAXLEN",
    ]

    @classmethod
    def get_data(cls, **kwargs):
        data = [
            ["utf8", "UTF-8 Unicode", "utf8_general_ci", 3],
            ["latin1", "cp1252 West European", "latin1_swedish_ci", 1],
            ["utf8mb4", "UTF-8 Unicode", "utf8mb4_general_ci", 4],
        ]

        df = pd.DataFrame(data, columns=cls.columns)
        return df


class CollationsTable(Table):
    name = "COLLATIONS"

    columns = [
        "COLLATION_NAME",
        "CHARACTER_SET_NAME",
        "ID",
        "IS_DEFAULT",
        "IS_COMPILED",
        "SORTLEN",
        "PAD_ATTRIBUTE",
    ]

    @classmethod
    def get_data(cls, **kwargs):
        data = [
            ["utf8_general_ci", "utf8", 33, "Yes", "Yes", 1, "PAD SPACE"],
            ["latin1_swedish_ci", "latin1", 8, "Yes", "Yes", 1, "PAD SPACE"],
        ]

        df = pd.DataFrame(data, columns=cls.columns)
        return df
