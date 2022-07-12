from functools import partial

import pandas as pd

from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.ast import BinaryOperation, Select, Identifier, Constant

from mindsdb.api.mysql.mysql_proxy.utilities.sql import query_df
from mindsdb.api.mysql.mysql_proxy.classes.sql_query import get_all_tables
from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.datanode import DataNode
from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.mindsdb_datanode import MindsDBDataNode
from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.integration_datanode import IntegrationDataNode
from mindsdb.api.mysql.mysql_proxy.datahub.classes.tables_row import TablesRow, TABLES_ROW_TYPE
from mindsdb.api.mysql.mysql_proxy.utilities import exceptions as exc


class InformationSchemaDataNode(DataNode):
    type = 'INFORMATION_SCHEMA'

    information_schema = {
        'SCHEMATA': ['CATALOG_NAME', 'SCHEMA_NAME', 'DEFAULT_CHARACTER_SET_NAME', 'DEFAULT_COLLATION_NAME', 'SQL_PATH'],
        'TABLES': ['TABLE_CATALOG', 'TABLE_SCHEMA', 'TABLE_NAME', 'TABLE_TYPE', 'ENGINE', 'VERSION', 'ROW_FORMAT', 'TABLE_ROWS', 'AVG_ROW_LENGTH', 'DATA_LENGTH', 'MAX_DATA_LENGTH', 'INDEX_LENGTH', 'DATA_FREE', 'AUTO_INCREMENT', 'CREATE_TIME', 'UPDATE_TIME', 'CHECK_TIME', 'TABLE_COLLATION', 'CHECKSUM', 'CREATE_OPTIONS', 'TABLE_COMMENT'],
        'COLUMNS': ['TABLE_CATALOG', 'TABLE_SCHEMA', 'TABLE_NAME', 'COLUMN_NAME', 'ORDINAL_POSITION', 'COLUMN_DEFAULT', 'IS_NULLABLE', 'DATA_TYPE', 'CHARACTER_MAXIMUM_LENGTH', 'CHARACTER_OCTET_LENGTH', 'NUMERIC_PRECISION', 'NUMERIC_SCALE', 'DATETIME_PRECISION', 'CHARACTER_SET_NAME', 'COLLATION_NAME', 'COLUMN_TYPE', 'COLUMN_KEY', 'EXTRA', 'PRIVILEGES', 'COLUMN_COMMENT', 'GENERATION_EXPRESSION'],
        'EVENTS': ['EVENT_CATALOG', 'EVENT_SCHEMA', 'EVENT_NAME', 'DEFINER', 'TIME_ZONE', 'EVENT_BODY', 'EVENT_DEFINITION', 'EVENT_TYPE', 'EXECUTE_AT', 'INTERVAL_VALUE', 'INTERVAL_FIELD', 'SQL_MODE', 'STARTS', 'ENDS', 'STATUS', 'ON_COMPLETION', 'CREATED', 'LAST_ALTERED', 'LAST_EXECUTED', 'EVENT_COMMENT', 'ORIGINATOR', 'CHARACTER_SET_CLIENT', 'COLLATION_CONNECTION', 'DATABASE_COLLATION'],
        'ROUTINES': ['SPECIFIC_NAME', 'ROUTINE_CATALOG', 'ROUTINE_SCHEMA', 'ROUTINE_NAME', 'ROUTINE_TYPE', 'DATA_TYPE', 'CHARACTER_MAXIMUM_LENGTH', 'CHARACTER_OCTET_LENGTH', 'NUMERIC_PRECISION', 'NUMERIC_SCALE', 'DATETIME_PRECISION', 'CHARACTER_SET_NAME', 'COLLATION_NAME', 'DTD_IDENTIFIER', 'ROUTINE_BODY', 'ROUTINE_DEFINITION', 'EXTERNAL_NAME', 'EXTERNAL_LANGUAGE', 'PARAMETER_STYLE', 'IS_DETERMINISTIC', 'SQL_DATA_ACCESS', 'SQL_PATH', 'SECURITY_TYPE', 'CREATED', 'LAST_ALTERED', 'SQL_MODE', 'ROUTINE_COMMENT', 'DEFINER', 'CHARACTER_SET_CLIENT', 'COLLATION_CONNECTION', 'DATABASE_COLLATION'],
        'TRIGGERS': ['TRIGGER_CATALOG', 'TRIGGER_SCHEMA', 'TRIGGER_NAME', 'EVENT_MANIPULATION', 'EVENT_OBJECT_CATALOG', 'EVENT_OBJECT_SCHEMA', 'EVENT_OBJECT_TABLE', 'ACTION_ORDER', 'ACTION_CONDITION', 'ACTION_STATEMENT', 'ACTION_ORIENTATION', 'ACTION_TIMING', 'ACTION_REFERENCE_OLD_TABLE', 'ACTION_REFERENCE_NEW_TABLE', 'ACTION_REFERENCE_OLD_ROW', 'ACTION_REFERENCE_NEW_ROW', 'CREATED', 'SQL_MODE', 'DEFINER', 'CHARACTER_SET_CLIENT', 'COLLATION_CONNECTION', 'DATABASE_COLLATION'],
        'PLUGINS': ['PLUGIN_NAME', 'PLUGIN_VERSION', 'PLUGIN_STATUS', 'PLUGIN_TYPE', 'PLUGIN_TYPE_VERSION', 'PLUGIN_LIBRARY', 'PLUGIN_LIBRARY_VERSION', 'PLUGIN_AUTHOR', 'PLUGIN_DESCRIPTION', 'PLUGIN_LICENSE', 'LOAD_OPTION', 'PLUGIN_MATURITY', 'PLUGIN_AUTH_VERSION'],
        'ENGINES': ['ENGINE', 'SUPPORT', 'COMMENT', 'TRANSACTIONS', 'XA', 'SAVEPOINTS'],
        'KEY_COLUMN_USAGE': ['CONSTRAINT_CATALOG', 'CONSTRAINT_SCHEMA', 'CONSTRAINT_NAME', 'TABLE_CATALOG', 'TABLE_SCHEMA', 'TABLE_NAME', 'COLUMN_NAME', 'ORDINAL_POSITION', 'POSITION_IN_UNIQUE_CONSTRAINT', 'REFERENCED_TABLE_SCHEMA', 'REFERENCED_TABLE_NAME', 'REFERENCED_COLUMN_NAME'],
        'STATISTICS': ['TABLE_CATALOG', 'TABLE_SCHEMA', 'TABLE_NAME', 'NON_UNIQUE', 'INDEX_SCHEMA', 'INDEX_NAME', 'SEQ_IN_INDEX', 'COLUMN_NAME', 'COLLATION', 'CARDINALITY', 'SUB_PART', 'PACKED', 'NULLABLE', 'INDEX_TYPE', 'COMMENT', 'INDEX_COMMENT', 'IS_VISIBLE', 'EXPRESSION'],
        'CHARACTER_SETS': ['CHARACTER_SET_NAME', 'DEFAULT_COLLATE_NAME', 'DESCRIPTION', 'MAXLEN'],
        'COLLATIONS': ['COLLATION_NAME', 'CHARACTER_SET_NAME', 'ID', 'IS_DEFAULT', 'IS_COMPILED', 'SORTLEN', 'PAD_ATTRIBUTE'],
    }

    def __init__(self, session):
        self.session = session
        self.integration_controller = session.integration_controller
        self.view_interface = session.view_interface
        self.persis_datanodes = {
            'mindsdb': MindsDBDataNode(
                session.model_interface,
                session.integration_controller
            ),
            'files': IntegrationDataNode(
                'files',
                ds_type='file',
                integration_controller=self.session.integration_controller
            ),
            'views': IntegrationDataNode(
                'views',
                ds_type='view',
                integration_controller=self.session.integration_controller
            )
        }

        self.get_dataframe_funcs = {
            'TABLES': self._get_tables,
            'COLUMNS': self._get_columns,
            'SCHEMATA': self._get_schemata,
            'ENGINES': self._get_engines,
            'CHARACTER_SETS': self._get_charsets,
            'COLLATIONS': self._get_collations,
        }
        for table_name in self.information_schema:
            if table_name not in self.get_dataframe_funcs:
                self.get_dataframe_funcs[table_name] = partial(self._get_empty_table, table_name)

    def __getitem__(self, key):
        return self.get(key)

    def get(self, name):
        name_lower = name.lower()

        if name.lower() == 'information_schema':
            return self

        if name_lower in self.persis_datanodes:
            return self.persis_datanodes[name_lower]

        integration_names = self.integration_controller.get_all().keys()
        for integration_name in integration_names:
            if integration_name.lower() == name_lower:
                datasource = self.integration_controller.get(name=integration_name)
                return IntegrationDataNode(
                    integration_name,
                    ds_type=datasource['engine'],
                    integration_controller=self.session.integration_controller
                )

        return None

    def has_table(self, tableName):
        tn = tableName.upper()
        if tn in self.information_schema:
            return True
        return False

    def get_table_columns(self, tableName):
        tn = tableName.upper()
        if tn in self.information_schema:
            return self.information_schema[tn]
        raise exc.ErTableExistError(f'Table information_schema.{tableName} does not exists')

    def get_integrations_names(self):
        integration_names = self.integration_controller.get_all().keys()
        # remove files and views from list to prevent doubling in 'select from INFORMATION_SCHEMA.TABLES'
        return [
            x.lower()
            for x in integration_names
            if x not in ('files', 'views')
        ]

    def _get_tables(self, query: ASTNode = None):
        columns = self.information_schema['TABLES']

        target_table = None
        if type(query) == Select and type(query.where) == BinaryOperation and query.where.op == 'and':
            for arg in query.where.args:
                if (
                    type(arg) == BinaryOperation and arg.op == '='
                    and type(arg.args[0]) == Identifier
                    and arg.args[0].parts[0].upper() == 'TABLES'
                    and arg.args[0].parts[1].upper() == 'TABLE_SCHEMA'
                    and type(arg.args[1]) == Constant
                ):
                    target_table = arg.args[1].value
                    break

        data = []
        for name in self.information_schema.keys():
            if target_table is not None and target_table != name:
                continue
            row = TablesRow(TABLE_TYPE=TABLES_ROW_TYPE.SYSTEM_VIEW, TABLE_NAME=name)
            data.append(row.to_list())

        for ds_name, ds in self.persis_datanodes.items():
            if target_table is not None and target_table != ds_name:
                continue
            ds_tables = ds.get_tables()
            if len(ds_tables) == 0:
                continue
            elif isinstance(ds_tables[0], dict):
                ds_tables = [TablesRow(TABLE_TYPE=TABLES_ROW_TYPE.BASE_TABLE, TABLE_NAME=x['name']) for x in ds_tables]
            elif isinstance(ds_tables, list) and len(ds_tables) > 0 and isinstance(ds_tables[0], str):
                ds_tables = [TablesRow(TABLE_TYPE=TABLES_ROW_TYPE.BASE_TABLE, TABLE_NAME=x) for x in ds_tables]
            for row in ds_tables:
                row.TABLE_SCHEMA = ds_name
                data.append(row.to_list())

        for ds_name in self.get_integrations_names():
            if target_table is not None and target_table != ds_name:
                continue
            try:
                ds = self.get(ds_name)
                ds_tables = ds.get_tables()
                for row in ds_tables:
                    row.TABLE_SCHEMA = ds_name
                    data.append(row.to_list())
            except Exception:
                print(f"Can't get tables from '{ds_name}'")

        df = pd.DataFrame(data, columns=columns)
        return df

    def _get_columns(self, query: ASTNode = None):
        columns = self.information_schema['COLUMNS']

        # NOTE there is a lot of types in mysql, but listed below should be enough for our purposes
        row_templates = {
            'text': ['def', 'SCHEMA_NAME', 'TABLE_NAME', 'COLUMN_NAME', 'COL_INDEX', None, 'YES', 'varchar', 1024, 3072, None, None, None, 'utf8', 'utf8_bin', 'varchar(1024)', None, None, 'select', None, None],
            'timestamp': ['def', 'SCHEMA_NAME', 'TABLE_NAME', 'COLUMN_NAME', 'COL_INDEX', 'CURRENT_TIMESTAMP', 'YES', 'timestamp', None, None, None, None, 0, None, None, 'timestamp', None, None, 'select', None, None],
            'bigint': ['def', 'SCHEMA_NAME', 'TABLE_NAME', 'COLUMN_NAME', 'COL_INDEX', None, 'YES', 'bigint', None, None, 20, 0, None, None, None, 'bigint unsigned', None, None, 'select', None, None],
            'float': ['def', 'SCHEMA_NAME', 'TABLE_NAME', 'COLUMN_NAME', 'COL_INDEX', None, 'YES', 'float', None, None, 12, 0, None, None, None, 'float', None, None, 'select', None, None]
        }

        result = []

        for table_name in self.information_schema:
            table_columns = self.information_schema[table_name]
            for i, column_name in enumerate(table_columns):
                result_row = row_templates['text'].copy()
                result_row[1] = 'information_schema'
                result_row[2] = table_name
                result_row[3] = column_name
                result_row[4] = i
                result.append(result_row)

        mindsb_dn = self.get('MINDSDB')
        for table_name in mindsb_dn.get_tables():
            table_columns = mindsb_dn.get_table_columns(table_name)
            for i, column_name in enumerate(table_columns):
                result_row = row_templates['text'].copy()
                result_row[1] = 'mindsdb'
                result_row[2] = table_name
                result_row[3] = column_name
                result_row[4] = i
                result.append(result_row)

        mindsb_dn = self.get('FILES')
        for table_name in mindsb_dn.get_tables():
            table_columns = mindsb_dn.get_table_columns(table_name)
            for i, column_name in enumerate(table_columns):
                result_row = row_templates['text'].copy()
                result_row[1] = 'files'
                result_row[2] = table_name
                result_row[3] = column_name
                result_row[4] = i
                result.append(result_row)

        df = pd.DataFrame(result, columns=columns)
        return df

    def _get_schemata(self, query: ASTNode = None):
        columns = self.information_schema['SCHEMATA']
        data = [
            ['def', 'information_schema', 'utf8', 'utf8_general_ci', None]
        ]

        # permanent databases
        data.append(['def', 'mindsdb', 'utf8mb4', 'utf8mb4_0900_ai_ci', None])

        integration_names = self.integration_controller.get_all().keys()
        for database_name in integration_names:
            data.append(['def', database_name, 'utf8mb4', 'utf8mb4_0900_ai_ci', None])

        df = pd.DataFrame(data, columns=columns)
        return df

    def _get_engines(self, query: ASTNode = None):
        columns = self.information_schema['ENGINES']
        data = [['InnoDB', 'DEFAULT', 'Supports transactions, row-level locking, and foreign keys', 'YES', 'YES', 'YES']]

        df = pd.DataFrame(data, columns=columns)
        return df

    def _get_charsets(self, query: ASTNode = None):
        columns = self.information_schema['CHARACTER_SETS']
        data = [
            ['utf8', 'UTF-8 Unicode', 'utf8_general_ci', 3],
            ['latin1', 'cp1252 West European', 'latin1_swedish_ci', 1],
            ['utf8mb4', 'UTF-8 Unicode', 'utf8mb4_general_ci', 4]
        ]

        df = pd.DataFrame(data, columns=columns)
        return df

    def _get_collations(self, query: ASTNode = None):
        columns = self.information_schema['COLLATIONS']
        data = [
            ['utf8_general_ci', 'utf8', 33, 'Yes', 'Yes', 1, 'PAD SPACE'],
            ['latin1_swedish_ci', 'latin1', 8, 'Yes', 'Yes', 1, 'PAD SPACE']
        ]

        df = pd.DataFrame(data, columns=columns)
        return df

    def _get_empty_table(self, table_name, query: ASTNode = None):
        columns = self.information_schema[table_name]
        data = []

        df = pd.DataFrame(data, columns=columns)
        return df

    def query(self, query: ASTNode):
        query_tables = get_all_tables(query)

        if len(query_tables) != 1:
            raise exc.ErBadTableError(f'Only one table can be used in query to information_schema: {query}')

        table_name = query_tables[0].upper()

        if table_name not in self.get_dataframe_funcs:
            raise exc.ErNotSupportedYet('Information schema: Not implemented.')

        dataframe = self.get_dataframe_funcs[table_name](query=query)

        try:
            data = query_df(dataframe, query, session=self.session)
        except Exception as e:
            print(f'Exception! {e}')
            return [], []

        columns_info = [
            {
                'name': k,
                'type': v
            }
            for k, v in data.dtypes.items()
        ]

        return data.to_dict(orient='records'), columns_info
