from dataclasses import dataclass
from datetime import datetime


class TABLES_ROW_TYPE:
    __slots__ = ()
    BASE_TABLE = 'BASE TABLE'
    VIEW = 'VIEW'
    SYSTEM_VIEW = 'SYSTEM VIEW'


TABLES_ROW_TYPE = TABLES_ROW_TYPE()


@dataclass
class TablesRow:
    TABLE_CATALOG: str = 'def'
    TABLE_SCHEMA: str = 'information_schema'
    TABLE_NAME: str = None
    TABLE_TYPE: str = TABLES_ROW_TYPE.BASE_TABLE
    ENGINE: str = None
    VERSION: int = None
    ROW_FORMAT: str = None
    TABLE_ROWS: int = 0
    AVG_ROW_LENGTH: int = 0
    DATA_LENGTH: int = 0
    MAX_DATA_LENGTH: int = 0
    INDEX_LENGTH: int = 0
    DATA_FREE: int = 0
    AUTO_INCREMENT: int = None
    CREATE_TIME: datetime = datetime(2024, 1, 1)
    UPDATE_TIME: datetime = datetime(2024, 1, 1)
    CHECK_TIME: datetime = datetime(2024, 1, 1)
    TABLE_COLLATION: str = None
    CHECKSUM: int = None
    CREATE_OPTIONS: str = None
    TABLE_COMMENT: str = ''

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
            if key not in TablesRow.__dataclass_fields__:
                del_keys.append(key)

        for key in del_keys:
            del data[key]

        return TablesRow(**data)
