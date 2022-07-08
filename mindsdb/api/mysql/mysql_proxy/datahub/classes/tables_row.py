class TABLES_ROW_TYPE:
    __slots__ = ()
    BASE_TABLE = 'BASE TABLE'
    VIEW = 'VIEW'
    SYSTEM_VIEW = 'SYSTEM VIEW'


TABLES_ROW_TYPE = TABLES_ROW_TYPE()


class TablesRow:
    columns = [
        'TABLE_CATALOG', 'TABLE_SCHEMA', 'TABLE_NAME', 'TABLE_TYPE',
        'ENGINE', 'VERSION', 'ROW_FORMAT', 'TABLE_ROWS', 'AVG_ROW_LENGTH',
        'DATA_LENGTH', 'MAX_DATA_LENGTH', 'INDEX_LENGTH', 'DATA_FREE',
        'AUTO_INCREMENT', 'CREATE_TIME', 'UPDATE_TIME', 'CHECK_TIME',
        'TABLE_COLLATION', 'CHECKSUM', 'CREATE_OPTIONS', 'TABLE_COMMENT'
    ]

    def __init__(self, TABLE_CATALOG='def', TABLE_SCHEMA='information_schema',
                 TABLE_NAME=None, TABLE_TYPE=TABLES_ROW_TYPE.BASE_TABLE, ENGINE=None,
                 VERSION=None, ROW_FORMAT=None, TABLE_ROWS=0, AVG_ROW_LENGTH=0,
                 DATA_LENGTH=0, MAX_DATA_LENGTH=0, INDEX_LENGTH=0, DATA_FREE=0,
                 AUTO_INCREMENT=None, CREATE_TIME='2022-01-01 00:00:00.0',
                 UPDATE_TIME=None, CHECK_TIME=None, TABLE_COLLATION=None,
                 CHECKSUM=None, CREATE_OPTIONS=None, TABLE_COMMENT=None):
        self.TABLE_CATALOG = TABLE_CATALOG
        self.TABLE_SCHEMA = TABLE_SCHEMA
        self.TABLE_NAME = TABLE_NAME
        self.TABLE_TYPE = TABLE_TYPE
        self.ENGINE = ENGINE
        self.VERSION = VERSION
        self.ROW_FORMAT = ROW_FORMAT
        self.TABLE_ROWS = TABLE_ROWS
        self.AVG_ROW_LENGTH = AVG_ROW_LENGTH
        self.DATA_LENGTH = DATA_LENGTH
        self.MAX_DATA_LENGTH = MAX_DATA_LENGTH
        self.INDEX_LENGTH = INDEX_LENGTH
        self.DATA_FREE = DATA_FREE
        self.AUTO_INCREMENT = AUTO_INCREMENT
        self.CREATE_TIME = CREATE_TIME
        self.UPDATE_TIME = UPDATE_TIME
        self.CHECK_TIME = CHECK_TIME
        self.TABLE_COLLATION = TABLE_COLLATION
        self.CHECKSUM = CHECKSUM
        self.CREATE_OPTIONS = CREATE_OPTIONS
        self.TABLE_COMMENT = TABLE_COMMENT

    def to_list(self) -> list:
        return [self.TABLE_CATALOG, self.TABLE_SCHEMA, self.TABLE_NAME,
                self.TABLE_TYPE, self.ENGINE, self.VERSION, self.ROW_FORMAT,
                self.TABLE_ROWS, self.AVG_ROW_LENGTH, self.DATA_LENGTH,
                self.MAX_DATA_LENGTH, self.INDEX_LENGTH, self.DATA_FREE,
                self.AUTO_INCREMENT, self.CREATE_TIME, self.UPDATE_TIME,
                self.CHECK_TIME, self.TABLE_COLLATION, self.CHECKSUM,
                self.CREATE_OPTIONS, self.TABLE_COMMENT]

    @staticmethod
    def from_dict(data: dict):

        del_keys = []
        data = {k.upper(): v for k, v in data.items()}
        for key in data:
            if key not in TablesRow.columns:
                del_keys.append(key)

        for key in del_keys:
            del data[key]

        return TablesRow(**data)
