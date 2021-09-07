import dfsql
import pandas as pd

from mindsdb.api.mysql.mysql_proxy.classes.sql_query_new import get_all_tables
from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.datanode import DataNode


def get_table_alias(table_obj):
    if table_obj.alias is not None:
        return table_obj.alias
    return '.'.join(table_obj.parts)


class InformationSchema(DataNode):
    type = 'INFORMATION_SCHEMA'

    information_schema = {
        'SCHEMATA': ['schema_name', 'default_character_set_name', 'default_collation_name'],
        'TABLES': ['table_schema', 'table_name', 'table_type', 'table_rows'],
        'COLUMNS': ['table_schema', 'table_name', 'ordinal_position', 'column_name', 'data_type'],
        'EVENTS': ['event_schema', 'event_name'],
        'ROUTINES': ['routine_schema', 'specific_name', 'routine_type'],
        'TRIGGERS': ['trigger_schema', 'trigger_name']
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
        # INFORMATION_SCHEMA.SCHEMATA
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
        columns = ['TABLE_NAME', 'TABLE_SCHEMA', 'TABLE_TYPE', 'TABLE_ROWS', 'TABLE_COLLATION']
        data = [
            ['SCHEMATA', 'information_schema', 'SYSTEM VIEW', [], 'utf8mb4_0900_ai_ci'],
            ['TABLES', 'information_schema', 'SYSTEM VIEW', [], 'utf8mb4_0900_ai_ci'],
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
        # TODO
        pass

    def _get_schemata(self):
        columns = ['CATALOG_NAME', 'SCHEMA_NAME', 'DEFAULT_CHARACTER_SET_NAME', 'DEFAULT_COLLATION_NAME', 'SQL_PATH']
        data = [
            ['def', 'information_schema', 'utf8', 'utf8_general_ci', None]
        ]

        for database_name in self.index:
            data.append(['def', database_name, 'utf8mb4', 'utf8mb4_0900_ai_ci', None])

        df = pd.DataFrame(data, columns=columns)
        return df

    def _get_events(self):
        columns = ['EVENT_CATALOG', 'EVENT_SCHEMA', 'EVENT_NAME', 'DEFINER', 'TIME_ZONE', 'EVENT_BODY', 'EVENT_DEFINITION', 'EVENT_TYPE', 'EXECUTE_AT', 'INTERVAL_VALUE', 'INTERVAL_FIELD', 'SQL_MODE', 'STARTS', 'ENDS', 'STATUS', 'ON_COMPLETION', 'CREATED', 'LAST_ALTERED', 'LAST_EXECUTED', 'EVENT_COMMENT', 'ORIGINATOR', 'CHARACTER_SET_CLIENT', 'COLLATION_CONNECTION', 'DATABASE_COLLATION']
        data = []

        df = pd.DataFrame(data, columns=columns)
        return df

    def _get_routines(self):
        columns = ['SPECIFIC_NAME', 'ROUTINE_CATALOG', 'ROUTINE_SCHEMA', 'ROUTINE_NAME', 'ROUTINE_TYPE', 'DATA_TYPE', 'CHARACTER_MAXIMUM_LENGTH', 'CHARACTER_OCTET_LENGTH', 'NUMERIC_PRECISION', 'NUMERIC_SCALE', 'DATETIME_PRECISION', 'CHARACTER_SET_NAME', 'COLLATION_NAME', 'DTD_IDENTIFIER', 'ROUTINE_BODY', 'ROUTINE_DEFINITION', 'EXTERNAL_NAME', 'EXTERNAL_LANGUAGE', 'PARAMETER_STYLE', 'IS_DETERMINISTIC', 'SQL_DATA_ACCESS', 'SQL_PATH', 'SECURITY_TYPE', 'CREATED', 'LAST_ALTERED', 'SQL_MODE', 'ROUTINE_COMMENT', 'DEFINER', 'CHARACTER_SET_CLIENT', 'COLLATION_CONNECTION', 'DATABASE_COLLATION']
        data = []

        df = pd.DataFrame(data, columns=columns)
        return df

    def _get_triggers(self):
        columns = ['TRIGGER_CATALOG', 'TRIGGER_SCHEMA', 'TRIGGER_NAME', 'EVENT_MANIPULATION', 'EVENT_OBJECT_CATALOG', 'EVENT_OBJECT_SCHEMA', 'EVENT_OBJECT_TABLE', 'ACTION_ORDER', 'ACTION_CONDITION', 'ACTION_STATEMENT', 'ACTION_ORIENTATION', 'ACTION_TIMING', 'ACTION_REFERENCE_OLD_TABLE', 'ACTION_REFERENCE_NEW_TABLE', 'ACTION_REFERENCE_OLD_ROW', 'ACTION_REFERENCE_NEW_ROW', 'CREATED', 'SQL_MODE','DEFINER', 'CHARACTER_SET_CLIENT', 'COLLATION_CONNECTION', 'DATABASE_COLLATION']
        data = []

        df = pd.DataFrame(data, columns=columns)
        return df

    def select_query(self, query):
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
        data = dfsql.sql_query(
            str(query),
            ds_kwargs={'case_sensitive': False},
            reduce_output=True,
            **{table_name: dataframe}
        )

        if isinstance(data, pd.core.series.Series):
            data = data.to_frame()
        elif isinstance(data, pd.DataFrame) is False:
            data = pd.DataFrame([{query.targets[0].parts[0]: data}])

        return data.to_dict(orient='records')

    def select(self, columns=None, table=None, where=None, order_by=None, group_by=None, came_from=None):
        tn = table.upper()
        if tn == 'SCHEMATA':
            # there is two query we can process, both hardcoded:
            # SELECT schema_name as name FROM INFORMATION_SCHEMA.SCHEMATA;
            # SELECT default_character_set_name as CharacterSetName, default_collation_name as CollationName FROM INFORMATION_SCHEMA.SCHEMATA WHERE schema_name = 'information_schema';
            if len(columns) == 1 and columns[0] == 'schema_name':
                data = [{'schema_name': 'INFORMATION_SCHEMA'}]
                for key in self.index:
                    data.append({
                        'schema_name': key.upper()
                    })
                return data
            elif len(columns) == 3 and where is not None and 'schema_name' in where:
                return [{
                    'schema_name': where['schema_name']['$eq'],
                    'default_character_set_name': 'utf8',
                    'default_collation_name': 'utf8_general_ci'
                }]
        if tn == 'TABLES':
            # query examples:
            # SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = 'information_schema' AND table_type in ('BASE TABLE', 'SYSTEM VIEW');
            # SELECT table_name as name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = 'information_schema' AND table_type = 'VIEW';
            # TODO add real table rows
            tables = [
                # at least this tables should be returned for GUI clients
                {'table_name': 'SCHEMATA', 'table_schema': 'information_schema', 'table_type': 'SYSTEM VIEW', 'table_rows': []},
                {'table_name': 'TABLES', 'table_schema': 'information_schema', 'table_type': 'SYSTEM VIEW', 'table_rows': []},
                {'table_name': 'EVENTS', 'table_schema': 'information_schema', 'table_type': 'SYSTEM VIEW', 'table_rows': []},
                {'table_name': 'ROUTINES', 'table_schema': 'information_schema', 'table_type': 'SYSTEM VIEW', 'table_rows': []},
                {'table_name': 'TRIGGERS', 'table_schema': 'information_schema', 'table_type': 'SYSTEM VIEW', 'table_rows': []},
            ]
            for dsName, ds in self.index.items():
                t = ds.getTables()
                tables += [{'table_name': x, 'table_schema': dsName, 'table_type': 'BASE TABLE', 'table_rows': []} for x in t]

            filtered_tables = tables
            if isinstance(where, dict) and 'table_schema' in where:
                schema = where['table_schema']['$eq']
                filtered_tables = [x for x in filtered_tables if x['table_schema'].upper() == schema.upper()]

            if isinstance(where, dict) and 'table_type' in where:
                types = []
                if '$eq' in where['table_type']:
                    types = [where['table_type']['$eq'].upper()]
                if '$in' in where['table_type']:
                    types += [x.upper() for x in where['table_type']['$in']]
                filtered_tables = [x for x in filtered_tables if x['table_type'] in types]
            return filtered_tables
        if tn == 'COLUMNS':
            # SELECT * FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='CSV_DS' AND TABLE_NAME='part' ORDER BY ORDINAL_POSITION
            return []
        if tn == 'EVENTS':
            # SELECT event_name as name FROM INFORMATION_SCHEMA.EVENTS WHERE event_schema = 'information_schema';
            return []
        if tn == 'ROUTINES':
            # SELECT specific_name as name FROM INFORMATION_SCHEMA.ROUTINES WHERE routine_schema = 'information_schema' AND routine_type = 'FUNCTION';
            return []
        if tn == 'TRIGGERS':
            # SELECT trigger_name as name FROM INFORMATION_SCHEMA.TRIGGERS WHERE trigger_schema = 'information_schema';
            return []

        return []
