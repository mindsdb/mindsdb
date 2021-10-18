import dfsql
import pandas as pd

from mindsdb.api.mysql.mysql_proxy.classes.sql_query import get_all_tables
from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.datanode import DataNode


def get_table_alias(table_obj):
    if table_obj.alias is not None:
        return table_obj.alias
    return '.'.join(table_obj.parts)


class InformationSchema(DataNode):
    type = 'INFORMATION_SCHEMA'

    information_schema = {
        'SCHEMATA': ['CATALOG_NAME', 'SCHEMA_NAME', 'DEFAULT_CHARACTER_SET_NAME', 'DEFAULT_COLLATION_NAME', 'SQL_PATH'],
        'TABLES': ['TABLE_NAME', 'TABLE_SCHEMA', 'TABLE_TYPE', 'TABLE_ROWS', 'TABLE_COLLATION'],
        'COLUMNS': ['TABLE_CATALOG', 'TABLE_SCHEMA', 'TABLE_NAME', 'COLUMN_NAME', 'ORDINAL_POSITION', 'COLUMN_DEFAULT', 'IS_NULLABLE', 'DATA_TYPE', 'CHARACTER_MAXIMUM_LENGTH', 'CHARACTER_OCTET_LENGTH', 'NUMERIC_PRECISION', 'NUMERIC_SCALE', 'DATETIME_PRECISION', 'CHARACTER_SET_NAME', 'COLLATION_NAME', 'COLUMN_TYPE', 'COLUMN_KEY', 'EXTRA', 'PRIVILEGES', 'COLUMN_COMMENT', 'GENERATION_EXPRESSION'],
        'EVENTS': ['EVENT_CATALOG', 'EVENT_SCHEMA', 'EVENT_NAME', 'DEFINER', 'TIME_ZONE', 'EVENT_BODY', 'EVENT_DEFINITION', 'EVENT_TYPE', 'EXECUTE_AT', 'INTERVAL_VALUE', 'INTERVAL_FIELD', 'SQL_MODE', 'STARTS', 'ENDS', 'STATUS', 'ON_COMPLETION', 'CREATED', 'LAST_ALTERED', 'LAST_EXECUTED', 'EVENT_COMMENT', 'ORIGINATOR', 'CHARACTER_SET_CLIENT', 'COLLATION_CONNECTION', 'DATABASE_COLLATION'],
        'ROUTINES': ['SPECIFIC_NAME', 'ROUTINE_CATALOG', 'ROUTINE_SCHEMA', 'ROUTINE_NAME', 'ROUTINE_TYPE', 'DATA_TYPE', 'CHARACTER_MAXIMUM_LENGTH', 'CHARACTER_OCTET_LENGTH', 'NUMERIC_PRECISION', 'NUMERIC_SCALE', 'DATETIME_PRECISION', 'CHARACTER_SET_NAME', 'COLLATION_NAME', 'DTD_IDENTIFIER', 'ROUTINE_BODY', 'ROUTINE_DEFINITION', 'EXTERNAL_NAME', 'EXTERNAL_LANGUAGE', 'PARAMETER_STYLE', 'IS_DETERMINISTIC', 'SQL_DATA_ACCESS', 'SQL_PATH', 'SECURITY_TYPE', 'CREATED', 'LAST_ALTERED', 'SQL_MODE', 'ROUTINE_COMMENT', 'DEFINER', 'CHARACTER_SET_CLIENT', 'COLLATION_CONNECTION', 'DATABASE_COLLATION'],
        'TRIGGERS': ['TRIGGER_CATALOG', 'TRIGGER_SCHEMA', 'TRIGGER_NAME', 'EVENT_MANIPULATION', 'EVENT_OBJECT_CATALOG', 'EVENT_OBJECT_SCHEMA', 'EVENT_OBJECT_TABLE', 'ACTION_ORDER', 'ACTION_CONDITION', 'ACTION_STATEMENT', 'ACTION_ORIENTATION', 'ACTION_TIMING', 'ACTION_REFERENCE_OLD_TABLE', 'ACTION_REFERENCE_NEW_TABLE', 'ACTION_REFERENCE_OLD_ROW', 'ACTION_REFERENCE_NEW_ROW', 'CREATED', 'SQL_MODE','DEFINER', 'CHARACTER_SET_CLIENT', 'COLLATION_CONNECTION', 'DATABASE_COLLATION']
    }

    def __init__(self, dsObject=None):
        self.index = {}
        if isinstance(dsObject, dict):
            self.add(dsObject)

    def __getitem__(self, key):
        return self.get(key)

    def add(self, dsObject):
        for key, val in dsObject.items():
            self.index[key.upper()] = val

    def get(self, name):
        if name.upper() == 'INFORMATION_SCHEMA':
            return self
        ds = self.index.get(name.upper())
        return ds

    def hasTable(self, tableName):
        tn = tableName.upper()
        if tn in self.information_schema or tn in self.index:
            return True
        return False

    def getTableColumns(self, tableName):
        tn = tableName.upper()
        if tn in self.information_schema:
            return self.information_schema[tn]
        raise Exception()

    def get_integrations_names(self):
        return [
            x.lower() for x in self.index if x.lower() not in ['mindsdb', 'datasource']
        ]

    def _get_tables(self):
        columns = self.information_schema['TABLES']
        data = [
            ['SCHEMATA', 'information_schema', 'SYSTEM VIEW', [], 'utf8mb4_0900_ai_ci'],
            ['TABLES', 'information_schema', 'SYSTEM VIEW', [], 'utf8mb4_0900_ai_ci'],
            ['COLUMNS', 'information_schema', 'SYSTEM VIEW', [], 'utf8mb4_0900_ai_ci'],
            ['EVENTS', 'information_schema', 'SYSTEM VIEW', [], 'utf8mb4_0900_ai_ci'],
            ['ROUTINES', 'information_schema', 'SYSTEM VIEW', [], 'utf8mb4_0900_ai_ci'],
            ['TRIGGERS', 'information_schema', 'SYSTEM VIEW', [], 'utf8mb4_0900_ai_ci']
        ]

        for dsName, ds in self.index.items():
            ds_tables = ds.getTables()
            data += [[x, dsName, 'BASE TABLE', [], 'utf8mb4_0900_ai_ci'] for x in ds_tables]

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

        mindsb_dn = self.index['MINDSDB']
        for table_name in mindsb_dn.getTables():
            table_columns = mindsb_dn.getTableColumns(table_name)
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

        for database_name in self.index:
            data.append(['def', database_name, 'utf8mb4', 'utf8mb4_0900_ai_ci', None])

        df = pd.DataFrame(data, columns=columns)
        return df

    def _get_events(self):
        columns = self.information_schema['EVENTS']
        data = []

        df = pd.DataFrame(data, columns=columns)
        return df

    def _get_routines(self):
        columns = self.information_schema['ROUTINES']
        data = []

        df = pd.DataFrame(data, columns=columns)
        return df

    def _get_triggers(self):
        columns = self.information_schema['TRIGGERS']
        data = []

        df = pd.DataFrame(data, columns=columns)
        return df

    def select(self, query):
        query_tables = get_all_tables(query)

        if len(query_tables) != 1:
            raise Exception(f'Only one table can be used in query to information_schema: {query}')

        table = query_tables[0].upper()
        if table == 'TABLES':
            dataframe = self._get_tables()
        elif table == 'COLUMNS':
            dataframe = self._get_columns()
        elif table == 'SCHEMATA':
            dataframe = self._get_schemata()
        elif table == 'EVENTS':
            dataframe = self._get_events()
        elif table == 'ROUTINES':
            dataframe = self._get_routines()
        elif table == 'TRIGGERS':
            dataframe = self._get_triggers()
        else:
            raise Exception('Information schema: Not implemented.')

        table_name = query.from_table.parts[-1]
        # region FIXME https://github.com/mindsdb/dfsql/issues/37 https://github.com/mindsdb/mindsdb_sql/issues/53
        if ' 1 = 0' in str(query):
            q = str(query)
            q = q[:q.lower().find('where')] + ' limit 0'
            data = dfsql.sql_query(
                q,
                ds_kwargs={'case_sensitive': False},
                reduce_output=False,
                **{table_name: dataframe}
            )
        # endregion
        else:
            # ---
            try:
                if table == 'TABLES':
                    query = 'select * from TABLES'
                    table_name = 'TABLES'
                data = dfsql.sql_query(
                    str(query),
                    ds_kwargs={'case_sensitive': False},
                    reduce_output=False,
                    **{table_name: dataframe}
                )
            except Exception as e:
                print(f'Exception! {e}')
                return [], []

        return data.to_dict(orient='records'), data.columns.to_list()
