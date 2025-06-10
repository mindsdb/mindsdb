import copy
from array import array
from typing import Any
from dataclasses import dataclass, field, MISSING

import numpy as np
import pandas as pd
from pandas.api import types as pd_types
import sqlalchemy.types as sqlalchemy_types

from mindsdb_sql_parser.ast import TableColumn

from mindsdb.utilities import log
from mindsdb.api.executor.exceptions import WrongArgumentError
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import MYSQL_DATA_TYPE


logger = log.getLogger(__name__)


def get_mysql_data_type_from_series(series: pd.Series, do_infer: bool = False) -> MYSQL_DATA_TYPE:
    """Maps pandas Series data type to corresponding MySQL data type.

    This function examines the dtype of a pandas Series and returns the appropriate
    MySQL data type enum value. For object dtypes, it can optionally attempt to infer
    a more specific type.

    Args:
        series (pd.Series): The pandas Series to determine the MySQL type for
        do_infer (bool): If True and series has object dtype, attempt to infer a more specific type

    Returns:
        MYSQL_DATA_TYPE: The corresponding MySQL data type enum value
    """
    dtype = series.dtype
    if pd_types.is_object_dtype(dtype) and do_infer is True:
        dtype = series.infer_objects().dtype

    if pd_types.is_object_dtype(dtype):
        return MYSQL_DATA_TYPE.TEXT
    if pd_types.is_datetime64_dtype(dtype):
        return MYSQL_DATA_TYPE.DATETIME
    if pd_types.is_string_dtype(dtype):
        return MYSQL_DATA_TYPE.TEXT
    if pd_types.is_bool_dtype(dtype):
        return MYSQL_DATA_TYPE.BOOL
    if pd_types.is_integer_dtype(dtype):
        return MYSQL_DATA_TYPE.INT
    if pd_types.is_numeric_dtype(dtype):
        return MYSQL_DATA_TYPE.FLOAT
    return MYSQL_DATA_TYPE.TEXT


def _dump_vector(value: Any) -> Any:
    if isinstance(value, array):
        return value.tolist()
    return value


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


def rename_df_columns(df: pd.DataFrame, names: list | None = None) -> None:
    """Inplace rename of dataframe columns

    Args:
        df (pd.DataFrame): dataframe
        names (Optional[List]): columns names to set
    """
    if names is not None:
        df.columns = names
    else:
        df.columns = list(range(len(df.columns)))


class ResultSet:
    def __init__(
        self,
        columns: list[Column] | None = None,
        values: list[list] | None = None,
        df: pd.DataFrame | None = None,
        affected_rows: int | None = None,
        is_prediction: bool = False,
        mysql_types: list[MYSQL_DATA_TYPE] | None = None,
    ):
        """
        Args:
            columns: list of Columns
            values (List[List]): data of resultSet, have to be list of lists with length equal to column
            df (pd.DataFrame): injected dataframe, have to have enumerated columns and length equal to columns
            affected_rows (int): number of affected rows
        """
        if columns is None:
            columns = []
        self._columns = columns

        if df is None:
            if values is None:
                df = None
            else:
                df = pd.DataFrame(values)
        self._df = df

        self.affected_rows = affected_rows

        self.is_prediction = is_prediction

        self.mysql_types = mysql_types

    def __repr__(self):
        col_names = ", ".join([col.name for col in self._columns])

        return f"{self.__class__.__name__}({self.length()} rows, cols: {col_names})"

    def __len__(self) -> int:
        if self._df is None:
            return 0
        return len(self._df)

    def __getitem__(self, slice_val):
        # return resultSet with sliced dataframe
        df = self._df[slice_val]
        return ResultSet(columns=self.columns, df=df)

    # --- converters ---

    @classmethod
    def from_df(
        cls,
        df: pd.DataFrame,
        database=None,
        table_name=None,
        table_alias=None,
        is_prediction: bool = False,
        mysql_types: list[MYSQL_DATA_TYPE] | None = None,
    ):
        match mysql_types:
            case None:
                mysql_types = [None] * len(df.columns)
            case list() if len(mysql_types) != len(df.columns):
                raise WrongArgumentError(f"Mysql types length mismatch: {len(mysql_types)} != {len(df.columns)}")

        columns = [
            Column(name=column_name, table_name=table_name, table_alias=table_alias, database=database, type=mysql_type)
            for column_name, mysql_type in zip(df.columns, mysql_types)
        ]

        rename_df_columns(df)
        return cls(df=df, columns=columns, is_prediction=is_prediction, mysql_types=mysql_types)

    @classmethod
    def from_df_cols(cls, df: pd.DataFrame, columns_dict: dict[str, Column], strict: bool = True) -> "ResultSet":
        """Create ResultSet from dataframe and dictionary of columns

        Args:
            df (pd.DataFrame): dataframe
            columns_dict (dict[str, Column]): dictionary of columns
            strict (bool): if True, raise an error if a column is not found in columns_dict

        Returns:
            ResultSet: result set

        Raises:
            ValueError: if a column is not found in columns_dict and strict is True
        """
        alias_idx = {column.alias: column for column in columns_dict.values() if column.alias is not None}

        columns = []
        for column_name in df.columns:
            if strict and column_name not in columns_dict:
                raise ValueError(f"Column {column_name} not found in columns_dict")
            column = columns_dict.get(column_name) or alias_idx.get(column_name) or Column(name=column_name)
            columns.append(column)

        rename_df_columns(df)

        return cls(columns=columns, df=df)

    def to_df(self):
        columns_names = self.get_column_names()
        df = self.get_raw_df()
        rename_df_columns(df, columns_names)
        return df

    def to_df_cols(self, prefix: str = "") -> tuple[pd.DataFrame, dict[str, Column]]:
        # returns dataframe and dict of columns
        #   can be restored to ResultSet by from_df_cols method

        columns = []
        col_names = {}
        for col in self._columns:
            name = col.get_hash_name(prefix)
            columns.append(name)
            col_names[name] = col

        df = self.get_raw_df()
        rename_df_columns(df, columns)
        return df, col_names

    # --- tables ---

    def get_tables(self):
        tables_idx = []
        tables = []
        cols = ["database", "table_name", "table_alias"]
        for col in self._columns:
            table = (col.database, col.table_name, col.table_alias)
            if table not in tables_idx:
                tables_idx.append(table)
                tables.append(dict(zip(cols, table)))
        return tables

    # --- columns ---

    def get_col_index(self, col):
        """
        Get column index
        :param col: column object
        :return: index of column
        """

        col_idx = None
        for i, col0 in enumerate(self._columns):
            if col0 is col:
                col_idx = i
                break
        if col_idx is None:
            raise WrongArgumentError(f"Column is not found: {col}")
        return col_idx

    def add_column(self, col, values=None):
        self._columns.append(col)

        col_idx = len(self._columns) - 1
        if self._df is not None:
            self._df[col_idx] = values
        return col_idx

    def del_column(self, col):
        idx = self.get_col_index(col)
        self._columns.pop(idx)

        self._df.drop(idx, axis=1, inplace=True)
        rename_df_columns(self._df)

    @property
    def columns(self):
        return self._columns

    def get_column_names(self):
        columns = [col.name if col.alias is None else col.alias for col in self._columns]
        return columns

    def find_columns(self, alias=None, table_alias=None):
        col_list = []
        for col in self.columns:
            if alias is not None and col.alias.lower() != alias.lower():
                continue
            if table_alias is not None and col.table_alias.lower() != table_alias.lower():
                continue
            col_list.append(col)

        return col_list

    def copy_column_to(self, col, result_set2):
        # copy with values
        idx = self.get_col_index(col)

        values = [row[idx] for row in self._records]

        col2 = copy.deepcopy(col)

        result_set2.add_column(col2, values)
        return col2

    def set_col_type(self, col_idx, type_name):
        self.columns[col_idx].type = type_name
        if self._df is not None:
            self._df[col_idx] = self._df[col_idx].astype(type_name)

    # --- records ---

    def get_raw_df(self):
        if self._df is None:
            names = range(len(self._columns))
            return pd.DataFrame([], columns=names)
        return self._df

    def add_raw_df(self, df):
        if len(df.columns) != len(self._columns):
            raise WrongArgumentError(f"Record length mismatch columns length: {len(df.columns)} != {len(self.columns)}")

        rename_df_columns(df)

        if self._df is None:
            self._df = df
        else:
            self._df = pd.concat([self._df, df], ignore_index=True)

    def add_raw_values(self, values):
        # If some values are None, the DataFrame could have incorrect integer types, since 'NaN' is technically a float, so it will convert ints to floats automatically.
        df = pd.DataFrame(values).convert_dtypes(
            convert_integer=True,
            convert_floating=True,
            infer_objects=False,
            convert_string=False,
            convert_boolean=False,
        )
        self.add_raw_df(df)

    def get_ast_columns(self) -> list[TableColumn]:
        """Converts ResultSet columns to a list of TableColumn objects with SQLAlchemy types.

        This method processes each column in the ResultSet, determines its MySQL data type
        (inferring it if necessary), and maps it to the appropriate SQLAlchemy type.
        The resulting TableColumn objects most likely will be used in CREATE TABLE statement.

        Returns:
            list[TableColumn]: A list of TableColumn objects with properly mapped SQLAlchemy types
        """
        columns: list[TableColumn] = []

        type_mapping = {
            MYSQL_DATA_TYPE.TINYINT: sqlalchemy_types.INTEGER,
            MYSQL_DATA_TYPE.SMALLINT: sqlalchemy_types.INTEGER,
            MYSQL_DATA_TYPE.MEDIUMINT: sqlalchemy_types.INTEGER,
            MYSQL_DATA_TYPE.INT: sqlalchemy_types.INTEGER,
            MYSQL_DATA_TYPE.BIGINT: sqlalchemy_types.INTEGER,
            MYSQL_DATA_TYPE.YEAR: sqlalchemy_types.INTEGER,
            MYSQL_DATA_TYPE.BOOL: sqlalchemy_types.BOOLEAN,
            MYSQL_DATA_TYPE.BOOLEAN: sqlalchemy_types.BOOLEAN,
            MYSQL_DATA_TYPE.FLOAT: sqlalchemy_types.FLOAT,
            MYSQL_DATA_TYPE.DOUBLE: sqlalchemy_types.FLOAT,
            MYSQL_DATA_TYPE.TIME: sqlalchemy_types.Time,
            MYSQL_DATA_TYPE.DATE: sqlalchemy_types.Date,
            MYSQL_DATA_TYPE.DATETIME: sqlalchemy_types.DateTime,
            MYSQL_DATA_TYPE.TIMESTAMP: sqlalchemy_types.TIMESTAMP,
        }

        for i, column in enumerate(self._columns):
            column_type: MYSQL_DATA_TYPE | None = column.type

            # infer MYSQL_DATA_TYPE if not set
            if isinstance(column_type, MYSQL_DATA_TYPE) is False:
                if column_type is not None:
                    logger.warning(f"Unexpected column type: {column_type}")
                if self._df is None:
                    column_type = MYSQL_DATA_TYPE.TEXT
                else:
                    column_type = get_mysql_data_type_from_series(self._df.iloc[:, i])

            sqlalchemy_type = type_mapping.get(column_type, sqlalchemy_types.TEXT)

            columns.append(TableColumn(name=column.alias, type=sqlalchemy_type))
        return columns

    def to_lists(self, json_types=False):
        """
        :param type_cast: cast numpy types
            array->list, datetime64->str
        :return: list of lists
        """

        if len(self.get_raw_df()) == 0:
            return []
        # output for APIs. simplify types
        if json_types:
            df = self.get_raw_df().copy()
            for name, dtype in df.dtypes.to_dict().items():
                if pd.api.types.is_datetime64_any_dtype(dtype):
                    df[name] = df[name].dt.strftime("%Y-%m-%d %H:%M:%S.%f")
            for i, column in enumerate(self.columns):
                if column.type == MYSQL_DATA_TYPE.VECTOR:
                    df[i] = df[i].apply(_dump_vector)
            df.replace({np.nan: None}, inplace=True)
            return df.to_records(index=False).tolist()

        # slower but keep timestamp type
        df = self._df.replace({np.nan: None})  # TODO rework
        return df.to_dict("split")["data"]

    def get_column_values(self, col_idx):
        # get by column index
        df = self.get_raw_df()
        return list(df[df.columns[col_idx]])

    def set_column_values(self, col_name, values):
        # values is one value or list of values
        cols = self.find_columns(col_name)
        if len(cols) == 0:
            col_idx = self.add_column(Column(name=col_name))
        else:
            col_idx = self.get_col_index(cols[0])

        if self._df is not None:
            self._df[col_idx] = values

    def add_from_result_set(self, rs):
        source_names = rs.get_column_names()

        col_sequence = []
        for name in self.get_column_names():
            col_sequence.append(source_names.index(name))

        raw_df = rs.get_raw_df()[col_sequence]

        self.add_raw_df(raw_df)

    @property
    def records(self):
        return list(self.get_records())

    def get_records(self):
        # get records as dicts.
        # !!! Attention: !!!
        # if resultSet contents duplicate column name: only one of them will be in output
        names = self.get_column_names()
        for row in self.to_lists():
            yield dict(zip(names, row))

    def length(self):
        return len(self)
