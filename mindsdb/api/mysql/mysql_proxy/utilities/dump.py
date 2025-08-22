import struct
import datetime
from typing import Any
from array import array

import numpy as np
from numpy import dtype as np_dtype
import pandas as pd
from pandas.api import types as pd_types

from mindsdb.api.executor.sql_query.result_set import ResultSet, get_mysql_data_type_from_series, Column
from mindsdb.api.mysql.mysql_proxy.utilities.lightwood_dtype import dtype as lightwood_dtype
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import (
    MYSQL_DATA_TYPE,
    DATA_C_TYPE_MAP,
    CTypeProperties,
    CHARSET_NUMBERS,
)
from mindsdb.utilities import log
from mindsdb.utilities.json_encoder import CustomJSONEncoder

logger = log.getLogger(__name__)

json_encoder = CustomJSONEncoder()


def column_to_mysql_column_dict(column: Column, database_name: str | None = None) -> dict[str, str | int]:
    """Convert Column object to dict with column properties.

    Args:
        column (Column): Column object to convert.
        database_name (str | None): Name of the database.

    Returns:
        dict[str, str | int]: Dictionary with mysql column properties.
    """
    # region infer type. Should not happen, but what if it is dtype of lightwood type?
    if isinstance(column.type, str):
        try:
            column.type = MYSQL_DATA_TYPE(column.type)
        except ValueError:
            if column.type == lightwood_dtype.date:
                column.type = MYSQL_DATA_TYPE.DATE
            elif column.type == lightwood_dtype.datetime:
                column.type = MYSQL_DATA_TYPE.DATETIME
            elif column.type == lightwood_dtype.float:
                column.type = MYSQL_DATA_TYPE.FLOAT
            elif column.type == lightwood_dtype.integer:
                column.type = MYSQL_DATA_TYPE.INT
            else:
                column.type = MYSQL_DATA_TYPE.TEXT
    elif isinstance(column.type, np_dtype):
        if pd_types.is_integer_dtype(column.type):
            column.type = MYSQL_DATA_TYPE.INT
        elif pd_types.is_numeric_dtype(column.type):
            column.type = MYSQL_DATA_TYPE.FLOAT
        elif pd_types.is_datetime64_any_dtype(column.type):
            column.type = MYSQL_DATA_TYPE.DATETIME
        else:
            column.type = MYSQL_DATA_TYPE.TEXT
    # endregion

    if isinstance(column.type, MYSQL_DATA_TYPE) is False:
        logger.warning(f"Unexpected column type: {column.type}. Use TEXT as fallback.")
        column.type = MYSQL_DATA_TYPE.TEXT

    charset = CHARSET_NUMBERS["utf8_unicode_ci"]
    if column.type in (MYSQL_DATA_TYPE.JSON, MYSQL_DATA_TYPE.VECTOR):
        charset = CHARSET_NUMBERS["binary"]

    type_properties: CTypeProperties = DATA_C_TYPE_MAP[column.type]

    result = {
        "database": column.database or database_name,
        #  TODO add 'original_table'
        "table_name": column.table_name,
        "name": column.name,
        "alias": column.alias or column.name,
        "size": type_properties.size,
        "flags": type_properties.flags,
        "type": type_properties.code,
        "charset": charset,
    }
    return result


def _dump_bool(var: Any) -> int | None:
    """Dumps a boolean value to an integer, as in MySQL boolean type is tinyint with values 0 and 1.
    NOTE: None consider as True in dataframe with dtype=bool, we can't change it

    Args:
        var (Any): The boolean value to dump

    Returns:
        int | None: 1 or 0 or None
    """
    if pd.isna(var):
        return None
    return "1" if var else "0"


def _dump_str(var: Any) -> str | None:
    """Dumps a value to a string.

    Args:
        var (Any): The value to dump

    Returns:
        str | None: The string representation of the value or None if the value is None
    """
    if isinstance(var, bytes):
        try:
            return var.decode("utf-8")
        except Exception:
            return str(var)[2:-1]
    if isinstance(var, (dict, list)):
        try:
            return json_encoder.encode(var)
        except Exception:
            return str(var)
    # pd.isna returns array of bools for list
    # and the truth value of a numpy array is ambiguous
    if isinstance(var, (list, np.ndarray)) is False and pd.isna(var):
        return None
    return str(var)


def _dump_int_or_str(var: Any) -> str | None:
    """Dumps a value to a string.
    If the value is numeric - then cast it to int to avoid float representation.

    Args:
        var (Any): The value to dump.

    Returns:
        str | None: The string representation of the value or None if the value is None
    """
    if pd.isna(var):
        return None
    try:
        return str(int(var))
    except ValueError:
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
    logger.warning(f"Unexpected value type for DATE: {type(var)}, {var}")
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
        if hasattr(var, "tzinfo") and var.tzinfo is not None:
            return var.astimezone(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        return var.strftime("%Y-%m-%d %H:%M:%S")
    elif isinstance(var, pd.Timestamp):
        if var.tzinfo is not None:
            return var.tz_convert("UTC").strftime("%Y-%m-%d %H:%M:%S")
        return var.strftime("%Y-%m-%d %H:%M:%S")
    elif isinstance(var, str):
        return var
    elif pd.isna(var):
        return None
    logger.warning(f"Unexpected value type for DATETIME: {type(var)}, {var}")
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
            return var.tz_convert("UTC").strftime("%H:%M:%S")
        return var.strftime("%H:%M:%S")
    elif isinstance(var, str):
        return var
    elif pd.isna(var):
        return None
    logger.warning(f"Unexpected value type for TIME: {type(var)}, {var}")
    return _dump_str(var)


def _dump_vector(value: Any) -> bytes | None:
    """Convert array or list of floats to a bytes.

    Args:
        value (Any): The value to dump

    Returns:
        bytes | None: The bytes representation of the vector value or None if the value is None
    """
    if isinstance(value, (array, list, np.ndarray)):
        return b"".join([struct.pack("<f", el) for el in value])
    elif pd.isna(value):
        return None
    err_msg = f"Unexpected value type for VECTOR: {type(value)}, {value}"
    logger.error(err_msg)
    raise ValueError(err_msg)


def _handle_series_as_date(series: pd.Series) -> pd.Series:
    """Convert values in a series to a string representation of a date.
    NOTE: MySQL require exactly %Y-%m-%d for DATE type.

    Args:
        series (pd.Series): The series to handle

    Returns:
        pd.Series: The series with the date values as strings
    """
    if pd_types.is_datetime64_any_dtype(series.dtype):
        return series.dt.strftime("%Y-%m-%d")
    elif pd_types.is_object_dtype(series.dtype):
        return series.apply(_dump_date)
    logger.info(f"Unexpected dtype: {series.dtype} for column with type DATE")
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
        return series.dt.strftime("%Y-%m-%d %H:%M:%S")
    elif pd_types.is_object_dtype(series.dtype):
        return series.apply(_dump_datetime)
    logger.info(f"Unexpected dtype: {series.dtype} for column with type DATETIME")
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
        base_time = pd.Timestamp("2000-01-01")
        series = (base_time + series).dt.strftime("%H:%M:%S")
    elif pd_types.is_datetime64_dtype(series.dtype):
        series = series.dt.strftime("%H:%M:%S")
    elif pd_types.is_object_dtype(series.dtype):
        series = series.apply(_dump_time)
    else:
        logger.info(f"Unexpected dtype: {series.dtype} for column with type TIME")
        series = series.apply(_dump_str)
    return series


def _handle_series_as_int(series: pd.Series) -> pd.Series:
    """Dump series to str(int) (or just str, of can't case to int). This need because of DataFrame store imput int as
    float if dtype is object: pd.DataFrame([None, 1], dtype='object') -> [NaN, 1.0]

    Args:
        series (pd.Series): The series to handle

    Returns:
        pd.Series: The series with the int values as strings
    """
    if pd_types.is_integer_dtype(series.dtype):
        if series.dtype == "Int64":
            # NOTE: 'apply' converts values to python floats
            return series.astype(object).apply(_dump_str)
        return series.apply(_dump_str)
    return series.apply(_dump_int_or_str)


def _handle_series_as_vector(series: pd.Series) -> pd.Series:
    """Convert values in a series to a bytes representation of a vector.
    NOTE: MySQL's VECTOR type require exactly 4 bytes per float.

    Args:
        series (pd.Series): The series to handle

    Returns:
        pd.Series: The series with the vector values as bytes
    """
    return series.apply(_dump_vector)


def dump_result_set_to_mysql(
    result_set: ResultSet, infer_column_size: bool = False
) -> tuple[pd.DataFrame, list[dict[str, str | int]]]:
    """
    Dumps the ResultSet to a format that can be used to send as MySQL response packet.
    NOTE: This method modifies the original DataFrame and columns.

    Args:
        result_set (ResultSet): result set to dump
        infer_column_size (bool): If True, infer the 'size' attribute of the column from the data.
                                  Exact size is not necessary, approximate is enough.

    Returns:
        tuple[pd.DataFrame, list[dict[str, str | int]]]: A tuple containing the modified DataFrame and a list
                                                            of MySQL column dictionaries. The dataframe values are
                                                            str or None, dtype=object
    """
    df = result_set.get_raw_df()

    for i, column in enumerate(result_set.columns):
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
            case (
                MYSQL_DATA_TYPE.INT
                | MYSQL_DATA_TYPE.TINYINT
                | MYSQL_DATA_TYPE.SMALLINT
                | MYSQL_DATA_TYPE.MEDIUMINT
                | MYSQL_DATA_TYPE.BIGINT
                | MYSQL_DATA_TYPE.YEAR
            ):
                series = _handle_series_as_int(series)
            case MYSQL_DATA_TYPE.VECTOR:
                series = _handle_series_as_vector(series)
            case _:
                series = series.apply(_dump_str)

        # inplace modification of dt types raise SettingWithCopyWarning, so do regular replace
        # we may split this operation for dt and other types for optimisation
        df[i] = series.replace([np.NaN, pd.NA, pd.NaT], None)

    columns_dicts = [column_to_mysql_column_dict(column) for column in result_set.columns]

    if infer_column_size and any(column_info.get("size") is None for column_info in columns_dicts):
        if len(df) == 0:
            for column_info in columns_dicts:
                if column_info["size"] is None:
                    column_info["size"] = 1
        else:
            sample = df.head(100)
            for i, column_info in enumerate(columns_dicts):
                try:
                    column_info["size"] = sample[sample.columns[i]].astype(str).str.len().max()
                except Exception:
                    column_info["size"] = 1

    return df, columns_dicts
