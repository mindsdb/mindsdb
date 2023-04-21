from enum import Enum


class PostgresField:
    def __init__(self, name: str, object_id: int, dt_size: int, type_modifier: int, format_code: int, table_id: int = 0,
                 column_id: int = 0):
        self.name = name
        self.object_id = object_id
        self.dt_size = dt_size
        self.type_modifier = type_modifier
        self.format_code = format_code
        self.table_id = table_id
        self.column_id = column_id


class GenericField(PostgresField):
    def __init__(self, name: str, object_id: int, table_id: int = 0, column_id: int = 0):
        super().__init__(name=name, object_id=object_id, dt_size=-1, type_modifier=-1, format_code=0, table_id=table_id,
                         column_id=column_id)


class IntField(PostgresField):
    def __init__(self, name: str, table_id: int = 0, column_id: int = 0):
        super().__init__(name=name, object_id=23, dt_size=4, type_modifier=-1, format_code=0, table_id=table_id,
                         column_id=column_id)


class POSTGRES_TYPES(Enum):
    VARCHAR = 0
    INT = 1
    LONG = 2
    DOUBLE = 3
    DATETIME = 4
    DATE = 5
