from typing import Optional, Literal
from dataclasses import dataclass, fields

import pandas as pd
from mindsdb_sql_parser.ast.base import ASTNode

from mindsdb.utilities import log
from mindsdb.utilities.config import config
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.integrations.libs.response import INF_SCHEMA_COLUMNS_NAMES
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import MYSQL_DATA_TYPE, MYSQL_DATA_TYPE_COLUMNS_DEFAULT
from mindsdb.api.executor.datahub.classes.tables_row import TABLES_ROW_TYPE, TablesRow


logger = log.getLogger(__name__)


def _get_scope(query):
    databases, tables = None, None
    try:
        conditions = extract_comparison_conditions(query.where)
    except NotImplementedError:
        return databases, tables
    for op, arg1, arg2 in conditions:
        if op == '=':
            scope = [arg2]
        elif op == 'in':
            if not isinstance(arg2, list):
                arg2 = [arg2]
            scope = arg2
        else:
            continue

        if arg1.lower() == 'table_schema':
            databases = scope
        elif arg1.lower() == 'table_name':
            tables = scope
    return databases, tables


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

        databases, _ = _get_scope(query)

        data = []
        for name in inf_schema.tables.keys():
            if databases is not None and name not in databases:
                continue
            row = TablesRow(TABLE_TYPE=TABLES_ROW_TYPE.SYSTEM_VIEW, TABLE_NAME=name)
            data.append(row.to_list())

        for ds_name, ds in inf_schema.persis_datanodes.items():
            if databases is not None and ds_name not in databases:
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
            if databases is not None and ds_name not in databases:
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
            if databases is not None and project_name not in databases:
                continue

            project_dn = inf_schema.get(project_name)
            project_tables = project_dn.get_tables()
            for row in project_tables:
                row.TABLE_SCHEMA = project_name
                data.append(row.to_list())

        df = pd.DataFrame(data, columns=cls.columns)
        return df


def infer_mysql_type(original_type: str) -> MYSQL_DATA_TYPE:
    """Infer MySQL data type from original type string from a database.

    Args:
        original_type (str): The original type string from a database.

    Returns:
        MYSQL_DATA_TYPE: The inferred MySQL data type.
    """
    match original_type.lower():
        case 'double precision' | 'real' | 'numeric' | 'float':
            data_type = MYSQL_DATA_TYPE.FLOAT
        case 'integer' | 'smallint' | 'int' | 'bigint':
            data_type = MYSQL_DATA_TYPE.BIGINT
        case 'timestamp without time zone' | 'timestamp with time zone' | 'date' | 'timestamp':
            data_type = MYSQL_DATA_TYPE.DATETIME
        case _:
            data_type = MYSQL_DATA_TYPE.VARCHAR
    return data_type


@dataclass(slots=True, kw_only=True)
class ColumnsTableRow:
    """Represents a row in the MindsDB's internal INFORMATION_SCHEMA.COLUMNS table.
    This class follows the MySQL-compatible COLUMNS table structure.

    Detailed field descriptions can be found in MySQL documentation:
    https://dev.mysql.com/doc/refman/8.4/en/information-schema-columns-table.html

    NOTE: The order of attributes is significant and matches the MySQL column order.
    """
    TABLE_CATALOG: Literal['def'] = 'def'
    TABLE_SCHEMA: Optional[str] = None
    TABLE_NAME: Optional[str] = None
    COLUMN_NAME: Optional[str] = None
    ORDINAL_POSITION: int = 0
    COLUMN_DEFAULT: Optional[str] = None
    IS_NULLABLE: Literal['YES', 'NO'] = 'YES'
    DATA_TYPE: str = MYSQL_DATA_TYPE.VARCHAR.value
    CHARACTER_MAXIMUM_LENGTH: Optional[int] = None
    CHARACTER_OCTET_LENGTH: Optional[int] = None
    NUMERIC_PRECISION: Optional[int] = None
    NUMERIC_SCALE: Optional[int] = None
    DATETIME_PRECISION: Optional[int] = None
    CHARACTER_SET_NAME: Optional[str] = None
    COLLATION_NAME: Optional[str] = None
    COLUMN_TYPE: Optional[str] = None
    COLUMN_KEY: Optional[str] = None
    EXTRA: Optional[str] = None
    PRIVILEGES: str = 'select'
    COLUMN_COMMENT: Optional[str] = None
    GENERATION_EXPRESSION: Optional[str] = None
    SRS_ID: Optional[str] = None
    # MindsDB's specific columns:
    ORIGINAL_TYPE: Optional[str] = None

    @classmethod
    def from_is_columns_row(cls, table_schema: str, table_name: str, row: pd.Series) -> 'ColumnsTableRow':
        """Transform row from response of `handler.get_columns(...)` to internal information_schema.columns row.

        Args:
            table_schema (str): The name of the schema of the table which columns are described.
            table_name (str): The name of the table which columns are described.
            row (pd.Series): A row from the response of `handler.get_columns(...)`.

        Returns:
            ColumnsTableRow: A row in the MindsDB's internal INFORMATION_SCHEMA.COLUMNS table.
        """
        original_type: str = row[INF_SCHEMA_COLUMNS_NAMES.DATA_TYPE] or ''
        data_type: MYSQL_DATA_TYPE | None = row[INF_SCHEMA_COLUMNS_NAMES.MYSQL_DATA_TYPE]
        if isinstance(data_type, MYSQL_DATA_TYPE) is False:
            data_type = infer_mysql_type(original_type)

        # region set default values depend on type
        defaults = MYSQL_DATA_TYPE_COLUMNS_DEFAULT.get(data_type)
        if defaults is not None:
            for key, value in defaults.items():
                if key in row and row[key] is None:
                    row[key] = value

        # region determine COLUMN_TYPE - it is text representation of DATA_TYPE with additioan attributes
        match data_type:
            case MYSQL_DATA_TYPE.DECIMAL:
                column_type = f'decimal({row[INF_SCHEMA_COLUMNS_NAMES.NUMERIC_PRECISION]},{INF_SCHEMA_COLUMNS_NAMES.NUMERIC_SCALE})'
            case MYSQL_DATA_TYPE.VARCHAR:
                column_type = f'varchar({row[INF_SCHEMA_COLUMNS_NAMES.CHARACTER_MAXIMUM_LENGTH]})'
            case MYSQL_DATA_TYPE.VARBINARY:
                column_type = f'varbinary({row[INF_SCHEMA_COLUMNS_NAMES.CHARACTER_MAXIMUM_LENGTH]})'
            case MYSQL_DATA_TYPE.BIT | MYSQL_DATA_TYPE.BINARY | MYSQL_DATA_TYPE.CHAR:
                column_type = f'{data_type.value.lower()}(1)'
            case MYSQL_DATA_TYPE.BOOL | MYSQL_DATA_TYPE.BOOLEAN:
                column_type = 'tinyint(1)'
            case _:
                column_type = data_type.value.lower()
        # endregion

        # BOOLean types had 'tinyint' DATA_TYPE in MySQL
        if data_type in (MYSQL_DATA_TYPE.BOOL, MYSQL_DATA_TYPE.BOOLEAN):
            data_type = 'tinyint'
        else:
            data_type = data_type.value.lower()

        return cls(
            TABLE_SCHEMA=table_schema,
            TABLE_NAME=table_name,
            COLUMN_NAME=row[INF_SCHEMA_COLUMNS_NAMES.COLUMN_NAME],
            ORDINAL_POSITION=row[INF_SCHEMA_COLUMNS_NAMES.ORDINAL_POSITION],
            COLUMN_DEFAULT=row[INF_SCHEMA_COLUMNS_NAMES.COLUMN_DEFAULT],
            IS_NULLABLE=row[INF_SCHEMA_COLUMNS_NAMES.IS_NULLABLE],
            DATA_TYPE=data_type,
            CHARACTER_MAXIMUM_LENGTH=row[INF_SCHEMA_COLUMNS_NAMES.CHARACTER_MAXIMUM_LENGTH],
            CHARACTER_OCTET_LENGTH=row[INF_SCHEMA_COLUMNS_NAMES.CHARACTER_OCTET_LENGTH],
            NUMERIC_PRECISION=row[INF_SCHEMA_COLUMNS_NAMES.NUMERIC_PRECISION],
            NUMERIC_SCALE=row[INF_SCHEMA_COLUMNS_NAMES.NUMERIC_SCALE],
            DATETIME_PRECISION=row[INF_SCHEMA_COLUMNS_NAMES.DATETIME_PRECISION],
            CHARACTER_SET_NAME=row[INF_SCHEMA_COLUMNS_NAMES.CHARACTER_SET_NAME],
            COLLATION_NAME=row[INF_SCHEMA_COLUMNS_NAMES.COLLATION_NAME],
            COLUMN_TYPE=column_type,
            ORIGINAL_TYPE=original_type
        )

    def __post_init__(self):
        """Check if all mandatory fields are filled.
        """
        mandatory_fields = ['TABLE_SCHEMA', 'TABLE_NAME', 'COLUMN_NAME']
        if any(getattr(self, field_name) is None for field_name in mandatory_fields):
            raise ValueError('One of mandatory fields is missed when creating ColumnsTableRow')


class ColumnsTable(Table):
    name = 'COLUMNS'
    columns = [field.name for field in fields(ColumnsTableRow)]

    @classmethod
    def get_data(cls, inf_schema=None, query: ASTNode = None, **kwargs) -> pd.DataFrame:
        databases, tables_names = _get_scope(query)

        if databases is None:
            databases = [
                'information_schema',
                config.get('default_project'),
                'files'
            ]

        result = []
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

                if tables_names is None:
                    tables_names = [t.TABLE_NAME for t in dn.get_tables()]
                for table_name in tables_names:
                    tables[table_name] = dn.get_table_columns_df(table_name)

            for table_name, table_columns_df in tables.items():
                for _, row in table_columns_df.iterrows():
                    result.append(
                        ColumnsTableRow.from_is_columns_row(
                            table_schema=db_name,
                            table_name=table_name,
                            row=row
                        )
                    )

        return pd.DataFrame(result, columns=cls.columns)


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
