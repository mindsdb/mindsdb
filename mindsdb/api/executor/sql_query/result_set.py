import copy
import datetime
from dataclasses import dataclass, field, MISSING
from typing import List, Optional, Any

import numpy as np
import pandas as pd
from numpy import dtype as np_dtype
from pandas.api import types as pd_types
import sqlalchemy.types as sqlalchemy_types

from mindsdb_sql_parser.ast import TableColumn

from mindsdb.utilities import log
from mindsdb.api.executor.exceptions import WrongArgumentError
from mindsdb.api.mysql.mysql_proxy.utilities.lightwood_dtype import dtype as lightwood_dtype
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import MYSQL_DATA_TYPE
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import DATA_C_TYPE_MAP, CTypeProperties

logger = log.getLogger(__name__)


@dataclass(kw_only=True, slots=True)
class Column:
    name: str = field(default=MISSING)
    alias: str | None = None
    table_name: str | None = None
    table_alias: str | None = None
    type: MYSQL_DATA_TYPE | None = None   # replaced from dtypes
    database: str | None = None
    flags: dict = None  # TODO dump it to packet
    charset: str | None = None

    def __post_init__(self):
        if self.alias is None:
            self.alias = self.name
        if self.table_alias is None:
            self.table_alias = self.table_name

    def get_hash_name(self, prefix):
        table_name = self.table_name if self.table_alias is None else self.table_alias
        name = self.name if self.alias is None else self.alias

        name = f'{prefix}_{table_name}_{name}'
        return name

    def to_mysql_column_dict(self, database_name: str | None = None) -> dict[str, str | int]:
        # region infer type. Should not happen, but is it dtype of lightwood type?
        if isinstance(self.type, str):
            try:
                self.type = MYSQL_DATA_TYPE(self.type)
            except ValueError:
                if self.type == lightwood_dtype.date:
                    self.type = MYSQL_DATA_TYPE.DATE
                elif self.type == lightwood_dtype.datetime:
                    self.type = MYSQL_DATA_TYPE.DATETIME
                elif self.type == lightwood_dtype.float:
                    self.type = MYSQL_DATA_TYPE.FLOAT
                elif self.type == lightwood_dtype.integer:
                    self.type = MYSQL_DATA_TYPE.INT
                else:
                    self.type = MYSQL_DATA_TYPE.TEXT
        elif isinstance(self.type, np_dtype):
            if pd_types.is_integer_dtype(self.type):
                self.type = MYSQL_DATA_TYPE.INT
            elif pd_types.is_numeric_dtype(self.type):
                self.type = MYSQL_DATA_TYPE.FLOAT
            elif pd_types.is_datetime64_any_dtype(self.type):
                self.type = MYSQL_DATA_TYPE.DATETIME
            else:
                self.type = MYSQL_DATA_TYPE.TEXT
        # endregion

        if isinstance(self.type, MYSQL_DATA_TYPE) is False:
            logger.warning(f'Unexpected column type: {self.type}. Use TEXT as fallback.')
            self.type = MYSQL_DATA_TYPE.TEXT

        type_properties: CTypeProperties = DATA_C_TYPE_MAP[self.type]

        result = {
            "database": self.database or database_name,
            #  TODO add 'original_table'
            "table_name": self.table_name,
            "name": self.name,
            "alias": self.alias or self.name,
            "size": type_properties.size,
            "flags": type_properties.flags,
            # NOTE all work with text-type, but if/when wanted change types to real,
            # it will need to check all types casts in BinaryResultsetRowPacket
            "type": type_properties.code,
        }
        return result


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


def rename_df_columns(df: pd.DataFrame, names: Optional[List] = None) -> None:
    """Inplace rename of dataframe columns

    Args:
        df (pd.DataFrame): dataframe
        names (Optional[List]): columns names to set
    """
    if names is not None:
        df.columns = names
    else:
        df.columns = list(range(len(df.columns)))


def _dump_bool(var: Any) -> int | None:
    """Dumps a boolean value to an integer, as in MySQL boolean type is tinyint with values 0 and 1.

    Args:
        var (Any): The boolean value to dump

    Returns:
        int | None: 1 or 0 or None
    """
    if pd.isna(var):
        return None
    return '1' if var else '0'


def _dump_str(var: Any) -> str | None:
    """Dumps a value to a string.

    Args:
        var (Any): The value to dump

    Returns:
        str | None: The string representation of the value or None if the value is None
    """
    if pd.isna(var):
        return None
    return str(var)


def _dump_date(var: datetime.date | str | None) -> str | None:
    """Dumps a date value to a string.

    Args:
        var (datetime.date | str | None): The date value to dump

    Returns:
        str | None: The string representation of the date value or None if the value is None
    """
    if isinstance(var, (datetime.date, pd.Timestamp)):  # it is also True for datetime.datetime
        return var.strftime("%Y-%m-%d")
    elif isinstance(var, str):
        return var
    elif pd.isna(var):
        return None
    logger.warning(f'Unexpected value type for DATE: {type(var)}, {var}')
    return _dump_str(var)


def _dump_datetime(var: datetime.datetime | str | None) -> str | None:
    """Dumps a datetime value to a string.
    # NOTE mysql may display only %Y-%m-%d %H:%M:%S format for datetime column

    Args:
        var (datetime.datetime | str | None): The datetime value to dump

    Returns:
        str | None: The string representation of the datetime value or None if the value is None
    """
    if isinstance(var, datetime.date):  # it is also datetime.datetime
        if hasattr(var, 'tzinfo') and var.tzinfo is not None:
            return var.astimezone(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        return var.strftime("%Y-%m-%d %H:%M:%S")
    elif isinstance(var, pd.Timestamp):
        if var.tzinfo is not None:
            return var.tz_convert('UTC').strftime("%Y-%m-%d %H:%M:%S")
        return var.strftime("%Y-%m-%d %H:%M:%S")
    elif isinstance(var, str):
        return var
    elif pd.isna(var):
        return None
    logger.warning(f'Unexpected value type for DATETIME: {type(var)}, {var}')
    return _dump_str(var)


def _dump_time(var: datetime.time | str | None) -> str | None:
    """Dumps a time value to a string.

    Args:
        var (datetime.time | str | None): The time value to dump

    Returns:
        str | None: The string representation of the time value or None if the value is None
    """
    if isinstance(var, datetime.time):
        if var.tzinfo is not None:
            # NOTE strftime does not support timezone, so we need to convert to UTC
            offset_seconds = var.tzinfo.utcoffset(None).total_seconds()
            time_seconds = var.hour * 3600 + var.minute * 60 + var.second
            utc_seconds = (time_seconds - offset_seconds) % (24 * 3600)
            hours = int(utc_seconds // 3600)
            minutes = int((utc_seconds % 3600) // 60)
            seconds = int(utc_seconds % 60)
            var = datetime.time(hours, minutes, seconds, var.microsecond)
        return var.strftime("%H:%M:%S")
    elif isinstance(var, datetime.datetime):
        if var.tzinfo is not None:
            return var.astimezone(datetime.timezone.utc).strftime("%H:%M:%S")
        return var.strftime("%H:%M:%S")
    elif isinstance(var, pd.Timestamp):
        if var.tzinfo is not None:
            return var.tz_convert('UTC').strftime("%H:%M:%S")
        return var.strftime("%H:%M:%S")
    elif isinstance(var, str):
        return var
    elif pd.isna(var):
        return None
    logger.warning(f'Unexpected value type for TIME: {type(var)}, {var}')
    return _dump_str(var)


def _handle_series_as_date(series: pd.Series) -> pd.Series:
    """Convert values in a series to a string representation of a date.
    NOTE: MySQL require exactly %Y-%m-%d for DATE type.

    Args:
        series (pd.Series): The series to handle

    Returns:
        pd.Series: The series with the date values as strings
    """
    if pd_types.is_datetime64_any_dtype(series.dtype):
        return series.dt.strftime('%Y-%m-%d')
    elif pd_types.is_object_dtype(series.dtype):
        return series.apply(_dump_date)
    logger.info(f'Unexpected dtype: {series.dtype} for column with type DATE')
    return series.apply(_dump_str)


def _handle_series_as_datetime(series: pd.Series) -> pd.Series:
    """Convert values in a series to a string representation of a datetime.
    NOTE: MySQL's DATETIME type require exactly %Y-%m-%d %H:%M:%S format.

    Args:
        series (pd.Series): The series to handle

    Returns:
        pd.Series: The series with the datetime values as strings
    """
    if pd_types.is_datetime64_any_dtype(series.dtype):
        return series.dt.strftime('%Y-%m-%d %H:%M:%S')
    elif pd_types.is_object_dtype(series.dtype):
        return series.apply(_dump_datetime)
    logger.info(f'Unexpected dtype: {series.dtype} for column with type DATETIME')
    return series.apply(_dump_str)


def _handle_series_as_time(series: pd.Series) -> pd.Series:
    """Convert values in a series to a string representation of a time.
    NOTE: MySQL's TIME type require exactly %H:%M:%S format.

    Args:
        series (pd.Series): The series to handle

    Returns:
        pd.Series: The series with the time values as strings
    """
    if pd_types.is_timedelta64_ns_dtype(series.dtype):
        base_time = pd.Timestamp('2000-01-01')
        series = ((base_time + series).dt.strftime('%H:%M:%S'))
    elif pd_types.is_datetime64_dtype(series.dtype):
        series = series.dt.strftime('%H:%M:%S')
    elif pd_types.is_object_dtype(series.dtype):
        series = series.apply(_dump_time)
    else:
        logger.info(f'Unexpected dtype: {series.dtype} for column with type TIME')
        series = series.apply(_dump_str)
    return series


class ResultSet:
    def __init__(
        self,
        columns: list[Column] | None = None,
        values: list[list] | None = None,
        df: pd.DataFrame | None = None,
        affected_rows: int | None = None,
        is_prediction: bool = False,
        mysql_types: list[MYSQL_DATA_TYPE] | None = None
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
        col_names = ', '.join([col.name for col in self._columns])

        return f'{self.__class__.__name__}({self.length()} rows, cols: {col_names})'

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
        cls, df: pd.DataFrame, database=None, table_name=None, table_alias=None,
        is_prediction: bool = False, mysql_types: list[MYSQL_DATA_TYPE] | None = None
    ):
        match mysql_types:
            case None:
                mysql_types = [None] * len(df.columns)
            case list() if len(mysql_types) != len(df.columns):
                raise WrongArgumentError(
                    f'Mysql types length mismatch: {len(mysql_types)} != {len(df.columns)}'
                )

        columns = [
            Column(
                name=column_name,
                table_name=table_name,
                table_alias=table_alias,
                database=database,
                type=mysql_type
            ) for column_name, mysql_type
            in zip(df.columns, mysql_types)
        ]

        rename_df_columns(df)
        return cls(
            df=df,
            columns=columns,
            is_prediction=is_prediction,
            mysql_types=mysql_types
        )

    @classmethod
    def from_df_cols(cls, df: pd.DataFrame, columns_dict: dict[str, Column], strict: bool = True) -> 'ResultSet':
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
        alias_idx = {
            column.alias: column
            for column in columns_dict.values()
            if column.alias is not None
        }

        columns = []
        for column_name in df.columns:
            if strict and column_name not in columns_dict:
                raise ValueError(f'Column {column_name} not found in columns_dict')
            column = (
                columns_dict.get(column_name)
                or alias_idx.get(column_name)
                or Column(name=column_name)
            )
            columns.append(column)

        rename_df_columns(df)

        return cls(
            columns=columns,
            df=df
        )

    def to_df(self):
        columns_names = self.get_column_names()
        df = self.get_raw_df()
        rename_df_columns(df, columns_names)
        return df

    def to_df_cols(self, prefix: str = '') -> tuple[pd.DataFrame, dict[str, Column]]:
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
        cols = ['database', 'table_name', 'table_alias']
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
            raise WrongArgumentError(f'Column is not found: {col}')
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
        columns = [
            col.name if col.alias is None else col.alias
            for col in self._columns
        ]
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
            raise WrongArgumentError(f'Record length mismatch columns length: {len(df.columns)} != {len(self.columns)}')

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
            convert_boolean=False
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
            MYSQL_DATA_TYPE.TIME: sqlalchemy_types.TIME,
            MYSQL_DATA_TYPE.DATE: sqlalchemy_types.DATE,
            MYSQL_DATA_TYPE.DATETIME: sqlalchemy_types.DATETIME,
            MYSQL_DATA_TYPE.TIMESTAMP: sqlalchemy_types.TIMESTAMP,
        }

        for i, column in enumerate(self._columns):
            column_type: MYSQL_DATA_TYPE | None = column.type

            # infer MYSQL_DATA_TYPE if not set
            if isinstance(column_type, MYSQL_DATA_TYPE) is False:
                if column_type is not None:
                    logger.warning(f'Unexpected column type: {column_type}')
                if self._df is None:
                    column_type = MYSQL_DATA_TYPE.TEXT
                else:
                    column_type = get_mysql_data_type_from_series(self._df.iloc[:, i])

            sqlalchemy_type = type_mapping.get(column_type, sqlalchemy_types.TEXT)

            columns.append(
                TableColumn(
                    name=column.alias,
                    type=sqlalchemy_type
                )
            )
        return columns

    def dump_to_mysql(self, infer_column_size: bool = False) -> tuple[pd.DataFrame, list[dict[str, str | int]]]:
        """
        Dumps the ResultSet to a format that can be used to send as MySQL response packet.
        NOTE: This method modifies the original DataFrame and columns.

        Args:
            infer_column_size (bool): If True, infer the 'size' attribute of the column from the data.
                                      Exact size is not necessary, approximate is enough.

        Returns:
            tuple[pd.DataFrame, list[dict[str, str | int]]]: A tuple containing the modified DataFrame and a list
                                                             of MySQL column dictionaries. The dataframe values are
                                                             str or None, dtype=object
        """
        df = self._df

        if df is None:
            raise ValueError('ResultSet is empty')

        for i, column in enumerate(self.columns):
            series = df[i]
            if isinstance(column.type, MYSQL_DATA_TYPE) is False:
                column.type = get_mysql_data_type_from_series(series)

            column_type: MYSQL_DATA_TYPE = column.type

            match column_type:
                case MYSQL_DATA_TYPE.BOOL | MYSQL_DATA_TYPE.BOOLEAN:
                    series = series.apply(_dump_bool)
                case MYSQL_DATA_TYPE.DATE:
                    series = _handle_series_as_date(series)
                case MYSQL_DATA_TYPE.DATETIME:
                    series = _handle_series_as_datetime(series)
                case MYSQL_DATA_TYPE.TIME:
                    series = _handle_series_as_time(series)
                case _:
                    series = series.apply(_dump_str)

            # inplace modification of dt types raise SettingWithCopyWarning, so do regular replace
            # we may split this operation for dt and other types for optimisation
            df[i] = series.replace([np.NaN, pd.NA, pd.NaT], None)

        columns_dicts = [
            column.to_mysql_column_dict()
            for column in self.columns
        ]

        if infer_column_size and any(column_info.size is None for column_info in columns_dicts):
            if len(df) == 0:
                for column_info in columns_dicts:
                    if column_info['size'] is None:
                        column_info['size'] = 1
            else:
                sample = df.head(100)
                for i, column_info in enumerate(columns_dicts):
                    try:
                        column_info['size'] = sample[sample.columns[i]].astype(str).str.len().max()
                    except Exception:
                        column_info['size'] = 1

        return df, columns_dicts

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
            df = df.replace({np.nan: None})
            return df.to_records(index=False).tolist()

        # slower but keep timestamp type
        df = self._df.replace({np.nan: None})  # TODO rework
        return df.to_dict('split')['data']

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
            col_sequence.append(
                source_names.index(name)
            )

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
