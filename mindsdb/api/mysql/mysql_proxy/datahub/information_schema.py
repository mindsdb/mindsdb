from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.datanode import DataNode

class InformationSchema(DataNode):
    type = 'INFORMATION_SCHEMA'

    index = {}

    information_schema = {
        'SCHEMATA': ['schema_name', 'default_character_set_name', 'default_collation_name'],
        'TABLES': ['table_schema', 'table_name', 'table_type'],
        'COLUMNS': ['table_schema', 'table_name', 'ordinal_position'],
        'EVENTS': ['event_schema', 'event_name'],
        'ROUTINES': ['routine_schema', 'specific_name', 'routine_type'],
        'TRIGGERS': ['trigger_schema', 'trigger_name']
    }

    def __init__(self, dsObject=None):
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

    def select(self, columns=None, table=None, where=None, order_by=None, group_by=None, came_from=None):
        tn = table.upper()
        if tn == 'SCHEMATA':
            # there is two query we can process, both hardcoded:
            # SELECT schema_name as name FROM INFORMATION_SCHEMA.SCHEMATA;
            # SELECT default_character_set_name as CharacterSetName, default_collation_name as CollationName FROM INFORMATION_SCHEMA.SCHEMATA WHERE schema_name = 'information_schema';
            if len(columns) == 1 and columns[0] == 'schema_name':
                data = [{'schema_name': 'information_schema'}]
                for key in self.index:
                    data.append({
                        'schema_name': key
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
            tables = [
                # at least this tables should be returned for GUI clients
                {'table_name': 'SCHEMATA', 'table_schema': 'information_schema', 'table_type': 'SYSTEM VIEW'},
                {'table_name': 'TABLES', 'table_schema': 'information_schema', 'table_type': 'SYSTEM VIEW'},
                {'table_name': 'EVENTS', 'table_schema': 'information_schema', 'table_type': 'SYSTEM VIEW'},
                {'table_name': 'ROUTINES', 'table_schema': 'information_schema', 'table_type': 'SYSTEM VIEW'},
                {'table_name': 'TRIGGERS', 'table_schema': 'information_schema', 'table_type': 'SYSTEM VIEW'},
            ]
            for dsName, ds in self.index.items():
                t = ds.getTables()
                tables += [{'table_name': x, 'table_schema': dsName, 'table_type': 'BASE TABLE'} for x in t]

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
