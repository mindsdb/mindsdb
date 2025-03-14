from datetime import datetime
from typing import Optional, List
from dataclasses import dataclass, field
from csv import excel as excel_csv_dialect

import pandas as pd


class _DEFAULT_SCHEMA_TYPE:
    pass


DEFAULT_SCHEMA = _DEFAULT_SCHEMA_TYPE()


@dataclass
class SchemaTableName:
    schema_name: Optional[str]
    table_name: str

    def weak_equal_to(self, value: 'SchemaTableName'):
        if self.schema_name is None:
            return self.table_name.lower() == value.table_name.lower()

        return self.schema_name.lower() == value.schema_name.lower() and self.table_name.lower() == value.table_name.lower()

    def __eq__(self, value: 'SchemaTableName'):
        return self.schema_name == value.schema_name and self.table_name == value.table_name


@dataclass
class HandlerInformationSchema:
    """Representation of information schema for a handler

    Args:
        tables (pd.DataFrame): DataFrame containing information from information_schema.tables
        columns (pd.DataFrame): DataFrame containing information from information_schema.columns
        _fetched_at (datetime): datetime of when the information schema was fetched
        _version (int): version of the information schema structure
    """
    tables: pd.DataFrame
    columns: pd.DataFrame
    _fetched_at: datetime = field(default=None)
    _version: int = field(default=1)

    @classmethod
    def from_dict(cls, data: dict) -> 'HandlerInformationSchema':
        return cls(
            tables=pd.DataFrame(data['tables']['data'], columns=data['tables']['columns']),
            columns=pd.DataFrame(data['columns']['data'], columns=data['columns']['columns']),
            _fetched_at=datetime.fromisoformat(data['_fetched_at']),
            _version=data.get('_version', 1)
        )

    def __post_init__(self):
        tables_columns_filter = ['TABLE_SCHEMA', 'TABLE_NAME', 'TABLE_TYPE']
        columns_columns_filter = ['TABLE_SCHEMA', 'TABLE_NAME', 'COLUMN_NAME', 'DATA_TYPE']
        columns_names_map = {
            'FIELD': 'COLUMN_NAME',
            'TYPE': 'DATA_TYPE'
        }

        # region prepare and validate infomration_schema.tables columns
        self.tables.columns = [x.upper() for x in self.tables.columns]
        if 'TABLE_NAME' not in self.tables:
            if len(self.tables) > 0:
                raise ValueError("'HandlerInformationSchema' initiated with tables list without TABLE_NAME")
            self.tables['TABLE_NAME'] = None
        if 'TABLE_SCHEMA' not in self.tables:
            self.tables['TABLE_SCHEMA'] = DEFAULT_SCHEMA
        if 'TABLE_TYPE' not in self.tables:
            self.tables['TABLE_TYPE'] = 'BASE TABLE'
        self.tables = self.tables[tables_columns_filter]
        # endregion

        # region prepare and validate infomration_schema.columns columns
        columns = [x.upper() for x in self.columns.columns]
        columns = [columns_names_map.get(x, x) for x in columns]
        self.columns.columns = columns

        for column_name in ['TABLE_NAME', 'COLUMN_NAME']:
            if column_name not in self.columns:
                if len(self.columns) > 0:
                    raise ValueError(f"'HandlerInformationSchema' initiated with columns list without {column_name}")
                self.columns[column_name] = None
        if 'TABLE_SCHEMA' not in self.columns:
            self.columns['TABLE_SCHEMA'] = DEFAULT_SCHEMA
        if 'DATA_TYPE' not in self.columns:
            self.columns['DATA_TYPE'] = None

        if 'TABLE_NAME' not in self.columns:
            if len(self.columns) > 0:
                raise ValueError("'HandlerInformationSchema' initiated with columns list without TABLE_NAME")
            self.columns['TABLE_NAME'] = None

        self.columns = self.columns[columns_columns_filter]
        # endregion

        if self._fetched_at is None:
            self._fetched_at = datetime.now()

    def filter_tables(self, tables_names: Optional[List[SchemaTableName]]) -> None:
        if tables_names is None:
            return

        def _filter_tables(value: pd.Series) -> bool:
            schema_table_name = SchemaTableName(
                schema_name=value['TABLE_SCHEMA'],
                table_name=value['TABLE_NAME']
            )
            return any(
                schema_table_name.weak_equal_to(table_name) for table_name in tables_names
            )

        self.tables = self.tables[
            self.tables.apply(_filter_tables, axis=1)
        ]

        def _filter_columns(value: pd.Series) -> bool:
            schema_name = value['TABLE_SCHEMA']
            table_name = value['TABLE_NAME']
            return (
                (self.tables['TABLE_SCHEMA'] == schema_name)
                & (self.tables['TABLE_NAME'] == table_name)
            ).any()

        self.columns = self.columns[
            self.columns.apply(_filter_columns, axis=1)
        ]

    def to_dict(self) -> dict:
        return {
            'tables': self.tables.to_dict(orient='split', index=False),
            'columns': self.columns.to_dict(orient='split', index=False),
            '_fetched_at': self._fetched_at.isoformat(),
            '_version': self._version
        }

    def get_table_as_excel_csv(self, table_name: str) -> str:
        return getattr(self, table_name).to_csv(
            index=False,
            sep=excel_csv_dialect.delimiter,
            quotechar=excel_csv_dialect.quotechar,
            lineterminator=excel_csv_dialect.lineterminator,
            quoting=excel_csv_dialect.quoting
        )

    def get_tables_as_str_list(self, database_name: str) -> List[str]:
        return self.tables.apply(
            lambda row: (
                f"`{database_name}`.`{row['TABLE_NAME']}`"
                if row['TABLE_SCHEMA'] is None else
                f"`{database_name}`.`{row['TABLE_SCHEMA']}`.`{row['TABLE_NAME']}`"
            ),
            axis=1
        ).tolist()
