from functools import partial

import pandas as pd

from mindsdb.api.mysql.mysql_proxy.utilities.sql import query_df
from mindsdb.api.mysql.mysql_proxy.classes.sql_query import get_all_tables
from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.datanode import DataNode
from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.file_datanode import FileDataNode
from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.mindsdb_datanode import MindsDBDataNode
from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.integration_datanode import IntegrationDataNode


class InformationSchema(DataNode):
    type = 'INFORMATION_SCHEMA'

    information_schema = {
        'SCHEMATA': ['CATALOG_NAME', 'SCHEMA_NAME', 'DEFAULT_CHARACTER_SET_NAME', 'DEFAULT_COLLATION_NAME', 'SQL_PATH'],
        'TABLES': ['TABLE_NAME', 'TABLE_SCHEMA', 'TABLE_TYPE', 'TABLE_ROWS', 'TABLE_COLLATION'],
        'COLUMNS': ['TABLE_CATALOG', 'TABLE_SCHEMA', 'TABLE_NAME', 'COLUMN_NAME', 'ORDINAL_POSITION', 'COLUMN_DEFAULT', 'IS_NULLABLE', 'DATA_TYPE', 'CHARACTER_MAXIMUM_LENGTH', 'CHARACTER_OCTET_LENGTH', 'NUMERIC_PRECISION', 'NUMERIC_SCALE', 'DATETIME_PRECISION', 'CHARACTER_SET_NAME', 'COLLATION_NAME', 'COLUMN_TYPE', 'COLUMN_KEY', 'EXTRA', 'PRIVILEGES', 'COLUMN_COMMENT', 'GENERATION_EXPRESSION'],
        'EVENTS': ['EVENT_CATALOG', 'EVENT_SCHEMA', 'EVENT_NAME', 'DEFINER', 'TIME_ZONE', 'EVENT_BODY', 'EVENT_DEFINITION', 'EVENT_TYPE', 'EXECUTE_AT', 'INTERVAL_VALUE', 'INTERVAL_FIELD', 'SQL_MODE', 'STARTS', 'ENDS', 'STATUS', 'ON_COMPLETION', 'CREATED', 'LAST_ALTERED', 'LAST_EXECUTED', 'EVENT_COMMENT', 'ORIGINATOR', 'CHARACTER_SET_CLIENT', 'COLLATION_CONNECTION', 'DATABASE_COLLATION'],
        'ROUTINES': ['SPECIFIC_NAME', 'ROUTINE_CATALOG', 'ROUTINE_SCHEMA', 'ROUTINE_NAME', 'ROUTINE_TYPE', 'DATA_TYPE', 'CHARACTER_MAXIMUM_LENGTH', 'CHARACTER_OCTET_LENGTH', 'NUMERIC_PRECISION', 'NUMERIC_SCALE', 'DATETIME_PRECISION', 'CHARACTER_SET_NAME', 'COLLATION_NAME', 'DTD_IDENTIFIER', 'ROUTINE_BODY', 'ROUTINE_DEFINITION', 'EXTERNAL_NAME', 'EXTERNAL_LANGUAGE', 'PARAMETER_STYLE', 'IS_DETERMINISTIC', 'SQL_DATA_ACCESS', 'SQL_PATH', 'SECURITY_TYPE', 'CREATED', 'LAST_ALTERED', 'SQL_MODE', 'ROUTINE_COMMENT', 'DEFINER', 'CHARACTER_SET_CLIENT', 'COLLATION_CONNECTION', 'DATABASE_COLLATION'],
        'TRIGGERS': ['TRIGGER_CATALOG', 'TRIGGER_SCHEMA', 'TRIGGER_NAME', 'EVENT_MANIPULATION', 'EVENT_OBJECT_CATALOG', 'EVENT_OBJECT_SCHEMA', 'EVENT_OBJECT_TABLE', 'ACTION_ORDER', 'ACTION_CONDITION', 'ACTION_STATEMENT', 'ACTION_ORIENTATION', 'ACTION_TIMING', 'ACTION_REFERENCE_OLD_TABLE', 'ACTION_REFERENCE_NEW_TABLE', 'ACTION_REFERENCE_OLD_ROW', 'ACTION_REFERENCE_NEW_ROW', 'CREATED', 'SQL_MODE', 'DEFINER', 'CHARACTER_SET_CLIENT', 'COLLATION_CONNECTION', 'DATABASE_COLLATION'],
        'PLUGINS': ['PLUGIN_NAME', 'PLUGIN_VERSION', 'PLUGIN_STATUS', 'PLUGIN_TYPE', 'PLUGIN_TYPE_VERSION', 'PLUGIN_LIBRARY', 'PLUGIN_LIBRARY_VERSION', 'PLUGIN_AUTHOR', 'PLUGIN_DESCRIPTION', 'PLUGIN_LICENSE', 'LOAD_OPTION', 'PLUGIN_MATURITY', 'PLUGIN_AUTH_VERSION'],
        'ENGINES': ['ENGINE', 'SUPPORT', 'COMMENT', 'TRANSACTIONS', 'XA', 'SAVEPOINTS']
    }

    def __init__(self, model_interface, ai_table, data_store, datasource_interface):
        self.datasource_interface = datasource_interface
        self.data_store = data_store
        self.persis_datanodes = {
            'mindsdb': MindsDBDataNode(model_interface, ai_table, data_store, datasource_interface),
            'files': FileDataNode(data_store)
        }

        self.get_dataframe_funcs = {
            'TABLES': self._get_tables,
            'COLUMNS': self._get_columns,
            'SCHEMATA': self._get_schemata,
            'EVENTS': partial(self._get_empty_table, 'EVENTS'),
            'ROUTINES': partial(self._get_empty_table, 'ROUTINES'),
            'TRIGGERS': partial(self._get_empty_table, 'TRIGGERS'),
            'PLUGINS': partial(self._get_empty_table, 'PLUGINS'),
            'ENGINES': self._get_engines
        }

    def __getitem__(self, key):
        return self.get(key)

    def get(self, name):
        name_lower = name.lower()

        if name.lower() == 'information_schema':
            return self

        if name_lower in self.persis_datanodes:
            return self.persis_datanodes[name_lower]

        datasource_names = self.datasource_interface.get_db_integrations().keys()
        for datasource_name in datasource_names:
            if datasource_name.lower() == name_lower:
                return IntegrationDataNode(datasource_name, self.data_store)

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
        raise Exception(f'Table information_schema.{tableName} does not exists')

    def get_datasources_names(self):
        datasource_names = self.datasource_interface.get_db_integrations().keys()
        return [
            x.lower() for x in datasource_names
        ]

    def _get_tables(self):
        columns = self.information_schema['TABLES']

        data = [
            [name, 'information_schema', 'SYSTEM VIEW', [], 'utf8mb4_0900_ai_ci']
            for name in self.information_schema.keys()
        ]

        for ds_name, ds in self.persis_datanodes.items():
            ds_tables = ds.get_tables()
            data += [[x, ds_name, 'BASE TABLE', [], 'utf8mb4_0900_ai_ci'] for x in ds_tables]

        for ds_name in self.get_datasources_names():
            ds = self.get(ds_name)
            ds_tables = ds.get_tables()
            data += [[x, ds_name, 'BASE TABLE', [], 'utf8mb4_0900_ai_ci'] for x in ds_tables]

        df = pd.DataFrame(data, columns=columns)
        return df

    def _get_columns(self):
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

        df = pd.DataFrame(result, columns=columns)
        return df

    def _get_schemata(self):
        columns = self.information_schema['SCHEMATA']
        data = [
            ['def', 'information_schema', 'utf8', 'utf8_general_ci', None]
        ]

        for database_name in self.persis_datanodes:
            data.append(['def', database_name, 'utf8mb4', 'utf8mb4_0900_ai_ci', None])

        datasource_names = self.datasource_interface.get_db_integrations().keys()
        for database_name in datasource_names:
            data.append(['def', database_name, 'utf8mb4', 'utf8mb4_0900_ai_ci', None])

        df = pd.DataFrame(data, columns=columns)
        return df

    def _get_engines(self):
        columns = self.information_schema['ENGINES']
        data = [['InnoDB', 'DEFAULT', 'Supports transactions, row-level locking, and foreign keys', 'YES', 'YES', 'YES']]

        df = pd.DataFrame(data, columns=columns)
        return df

    def _get_empty_table(self, table_name):
        columns = self.information_schema[table_name]
        data = []

        df = pd.DataFrame(data, columns=columns)
        return df

    def select(self, query):
        query_tables = get_all_tables(query)

        if len(query_tables) != 1:
            raise Exception(f'Only one table can be used in query to information_schema: {query}')

        table_name = query_tables[0].upper()

        if table_name not in self.get_dataframe_funcs:
            raise Exception('Information schema: Not implemented.')

        dataframe = self.get_dataframe_funcs[table_name]()

        try:
            data = query_df(dataframe, query)
        except Exception as e:
            print(f'Exception! {e}')
            return [], []

        return data.to_dict(orient='records'), data.columns.to_list()
