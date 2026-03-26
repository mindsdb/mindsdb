from dataclasses import dataclass, field, MISSING

from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import MYSQL_DATA_TYPE


@dataclass(kw_only=True, slots=True)
class Column:
    name: str = field(default=MISSING)
    alias: str | None = None
    table_name: str | None = None
    table_alias: str | None = None
    type: MYSQL_DATA_TYPE | None = None
    database: str | None = None
    flags: dict = None
    charset: str | None = None
    original_type: str | None = None
    dtype: str | None = None

    def __post_init__(self):
        if self.alias is None:
            self.alias = self.name
        if self.table_alias is None:
            self.table_alias = self.table_name

    def get_hash_name(self, prefix):
        table_name = self.table_name if self.table_alias is None else self.table_alias
        name = self.name if self.alias is None else self.alias

        name = f"{prefix}_{table_name}_{name}"
        return name
