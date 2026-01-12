import sys
from abc import ABC
from typing import Callable, Generator, ClassVar
from dataclasses import dataclass, fields

import numpy
import pandas

from mindsdb.utilities import log
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import MYSQL_DATA_TYPE
from mindsdb_sql_parser.ast import ASTNode
from mindsdb.utilities.types.column import Column


logger = log.getLogger(__name__)


@dataclass(frozen=True)
class _INFORMATION_SCHEMA_COLUMNS_NAMES:
    """Set of DataFrame columns that must be returned when calling `handler.get_columns(...)`.
    These column names match the standard INFORMATION_SCHEMA.COLUMNS structure
    used in SQL databases to describe table metadata.
    """

    COLUMN_NAME: str = "COLUMN_NAME"
    DATA_TYPE: str = "DATA_TYPE"
    ORDINAL_POSITION: str = "ORDINAL_POSITION"
    COLUMN_DEFAULT: str = "COLUMN_DEFAULT"
    IS_NULLABLE: str = "IS_NULLABLE"
    CHARACTER_MAXIMUM_LENGTH: str = "CHARACTER_MAXIMUM_LENGTH"
    CHARACTER_OCTET_LENGTH: str = "CHARACTER_OCTET_LENGTH"
    NUMERIC_PRECISION: str = "NUMERIC_PRECISION"
    NUMERIC_SCALE: str = "NUMERIC_SCALE"
    DATETIME_PRECISION: str = "DATETIME_PRECISION"
    CHARACTER_SET_NAME: str = "CHARACTER_SET_NAME"
    COLLATION_NAME: str = "COLLATION_NAME"
    MYSQL_DATA_TYPE: str = "MYSQL_DATA_TYPE"


INF_SCHEMA_COLUMNS_NAMES = _INFORMATION_SCHEMA_COLUMNS_NAMES()
INF_SCHEMA_COLUMNS_NAMES_SET = set(f.name for f in fields(INF_SCHEMA_COLUMNS_NAMES))


class HandlerStatusResponse:
    def __init__(
        self,
        success: bool = True,
        error_message: str = None,
        redirect_url: str = None,
        copy_storage: str = None,
    ) -> None:
        self.success = success
        self.error_message = error_message
        self.redirect_url = redirect_url
        self.copy_storage = copy_storage

    def to_json(self):
        data = {"success": self.success, "error": self.error_message}
        if self.redirect_url is not None:
            data["redirect_url"] = self.redirect_url
        if self.copy_storage is not None:
            data["copy_storage"] = self.copy_storage
        return data

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"success={self.success}, "
            f"error={self.error_message}, "
            f"redirect_url={self.redirect_url}, "
            f"copy_storage={self.copy_storage})"
        )


class DataHandlerResponse(ABC):
    """Base class for all data handler responses."""

    type: ClassVar[str]

    @property
    def resp_type(self):
        # For back compatibility with old code, use the type attribute instead of resp_type
        return self.type


class ErrorResponse(DataHandlerResponse):
    """Response for error cases.

    Attributes:
        type: RESPONSE_TYPE.ERROR
        error_code: int
        error_message: str | None
        is_expected_error: bool
        exception: Exception | None
    """

    type: ClassVar[str] = RESPONSE_TYPE.ERROR
    error_code: int
    error_message: str | None
    is_expected_error: bool
    exception: Exception | None

    def __init__(self, error_code: int = 0, error_message: str | None = None, is_expected_error: bool = False):
        self.error_code = error_code
        self.error_message = error_message
        self.is_expected_error = is_expected_error
        self.exception = None
        current_exception = sys.exc_info()
        if current_exception[0] is not None:
            self.exception = current_exception[1]

    def to_columns_table_response(self, map_type_fn: Callable) -> None:
        raise ValueError(
            f"Cannot convert {self.type} to {RESPONSE_TYPE.COLUMNS_TABLE}, the error is: {self.error_message}"
        )


class OkResponse(DataHandlerResponse):
    """Response for successful cases without data (e.g. CREATE TABLE, DROP TABLE, etc.).

    Attributes:
        type: RESPONSE_TYPE.OK
        affected_rows: int - how many rows were affected by the query
    """

    type: ClassVar[str] = RESPONSE_TYPE.OK
    affected_rows: int

    def __init__(self, affected_rows: int = None):
        self.affected_rows = affected_rows


class TableResponse(DataHandlerResponse):
    """Response for successful cases with data (e.g. SELECT, SHOW, etc.).

    Attributes:
        type: RESPONSE_TYPE.TABLE
        affected_rows: int | None - how many rows were affected by the query
        data_generator: Generator[pandas.DataFrame, None, None] | None - generator of data for lazy loading
        _columns: list[Column] | None - list of columns
        _data: pandas.DataFrame | None - loaded data
        _fetched: bool - if data was already fetched (data_generator is consumed)
        _invalid: bool - if data has already been fetched and cannot be iterated over
    """

    type: ClassVar[str] = RESPONSE_TYPE.TABLE
    affected_rows: int | None
    _data_generator: Generator[pandas.DataFrame, None, None] | None
    _columns: list[Column] | None
    _data: pandas.DataFrame | None
    _fetched: bool
    _invalid: bool

    def __init__(
        self,
        data: pandas.DataFrame | None = None,
        data_generator: Generator[pandas.DataFrame, None, None] | None = None,
        affected_rows: int | None = None,
        columns: list[Column] = None,
    ):
        """
        Either data and/or data_generator must be provided.
        Args:
            data (pandas.DataFrame): initial data
            data_generator (Generator[pandas.DataFrame, None, None]): generator of data
            affected_rows (int): total data rowcount - can be None depending on the handler
                                 NOTE: name affected_rows for compatibility with OKResponse
            columns (list[Column]): list of columns
        """
        self._data_generator = data_generator
        self._columns = columns
        self.affected_rows = affected_rows
        self._data = data
        self._fetched = False if data_generator else True
        self._invalid = False

    @property
    def data_generator(self) -> Generator[pandas.DataFrame, None, None]:
        return self._data_generator

    @data_generator.setter
    def data_generator(self, value):
        self._fetched = False if value else True
        self._data_generator = value

    def fetchall(self) -> pandas.DataFrame:
        """Fetch all data and store it in the _data attribute.

        Returns:
            pandas.DataFrame: Data frame.
        """
        self._raise_if_invalid()
        if self._data_generator is None or self._fetched:
            return self._data

        for el in self._data_generator:
            if self._data is None:
                self._data = el
            else:
                self._data = pandas.concat([self._data, el])

        self._fetched = True
        self._data_generator = None

        return self._data

    def fetchmany(self) -> pandas.DataFrame | None:
        """Fetch one piece of data and store it in the _data attribute.

        Returns:
            pandas.DataFrame: Data frame, piece of data.
        """
        self._raise_if_invalid()
        try:
            piece = next(self._data_generator)
            self._data = pandas.concat([self._data, piece])
        except StopIteration:
            self._fetched = True
            self._data_generator = None
            return None
        return piece

    def iterate_no_save(self) -> Generator[pandas.DataFrame, None, None]:
        """Iterate over the data and yield each piece of data. Do not save the data to the _data attribute.
        NOTE: do it only once, before return result to the user

        Returns:
            Generator[pandas.DataFrame, None, None]: Generator of data frames.
        """
        self._raise_if_invalid()
        if self._data is not None:
            yield self._data
        if self._data_generator:
            self._invalid = True
            for el in self._data_generator:
                yield el

    def _raise_if_invalid(self):
        if self._invalid:
            raise ValueError("Data has already been fetched and cannot be iterated over.")

    @property
    def data_frame(self) -> pandas.DataFrame:
        """Get the data frame. Represents the entire dataset.

        Returns:
            pandas.DataFrame: Data frame.
        """
        self.fetchall()
        return self._data

    @data_frame.setter
    def data_frame(self, value):
        """for back compatibility
        """
        self._data = value

    @property
    def columns(self) -> list[Column]:
        """Get the columns.

        Returns:
            list[Column]: List of columns.
        """
        self._resolve_columns()
        return self._columns

    def _resolve_columns(self):
        if self._columns is not None:
            return
        self.fetchall()
        self._columns = [Column(name=c) for c in self._data.columns]

    def set_columns_attrs(self, table_name: str | None, table_alias: str | None, database: str | None):
        """Set the attributes of the columns.

        Args:
            table_name (str | None): Table name.
            table_alias (str | None): Table alias.
            database (str | None): Database name.
        """
        self._resolve_columns()
        for column in self._columns:
            if table_name:
                column.table_name = table_name
            if table_alias:
                column.table_alias = table_alias
            if database:
                column.database = database

    def to_columns_table_response(self, map_type_fn: Callable) -> None:
        """Transform the response to a `columns table` response.
        NOTE: original dataframe will be mutated

        Args:
            map_type_fn (Callable): Function to map the data type to the MySQL data type.
        """
        if self.type == RESPONSE_TYPE.COLUMNS_TABLE:
            return
        if self.type != RESPONSE_TYPE.TABLE:
            raise ValueError(f"Cannot convert {self.resp} to {RESPONSE_TYPE.COLUMNS_TABLE}")

        self.fetchall()
        self._resolve_columns()
        self.type = RESPONSE_TYPE.COLUMNS_TABLE

        if self._data is None:
            return
        self._data.columns = [name.upper() for name in self._data.columns]
        self._data[INF_SCHEMA_COLUMNS_NAMES.MYSQL_DATA_TYPE] = self._data[INF_SCHEMA_COLUMNS_NAMES.DATA_TYPE].apply(
            map_type_fn
        )

        # region validate df
        current_columns_set = set(self._data.columns)
        if INF_SCHEMA_COLUMNS_NAMES_SET != current_columns_set:
            raise ValueError(f"Columns set for INFORMATION_SCHEMA.COLUMNS is wrong: {list(current_columns_set)}")
        # endregion

        self._data = self._data.astype(
            {
                INF_SCHEMA_COLUMNS_NAMES.COLUMN_NAME: "string",
                INF_SCHEMA_COLUMNS_NAMES.DATA_TYPE: "string",
                INF_SCHEMA_COLUMNS_NAMES.ORDINAL_POSITION: "Int32",
                INF_SCHEMA_COLUMNS_NAMES.COLUMN_DEFAULT: "string",
                INF_SCHEMA_COLUMNS_NAMES.IS_NULLABLE: "string",
                INF_SCHEMA_COLUMNS_NAMES.CHARACTER_MAXIMUM_LENGTH: "Int32",
                INF_SCHEMA_COLUMNS_NAMES.CHARACTER_OCTET_LENGTH: "Int32",
                INF_SCHEMA_COLUMNS_NAMES.NUMERIC_PRECISION: "Int32",
                INF_SCHEMA_COLUMNS_NAMES.NUMERIC_SCALE: "Int32",
                INF_SCHEMA_COLUMNS_NAMES.DATETIME_PRECISION: "Int32",
                INF_SCHEMA_COLUMNS_NAMES.CHARACTER_SET_NAME: "string",
                INF_SCHEMA_COLUMNS_NAMES.COLLATION_NAME: "string",
            }
        )
        self._data.replace([numpy.NaN, pandas.NA], None, inplace=True)


def normalize_response(response) -> TableResponse | OkResponse | ErrorResponse:
    """Convert legacy HandlerResponse to new response types.

    If response is already a new type (TableResponse, OkResponse, ErrorResponse),
    return it as-is. If response is a legacy HandlerResponse, convert it based
    on its resp_type.

    Args:
        response: Either a new response type or legacy HandlerResponse

    Returns:
        TableResponse | OkResponse | ErrorResponse: Normalized response
    """
    # Already new format - return as-is
    if isinstance(response, (TableResponse, OkResponse, ErrorResponse)):
        return response

    # Legacy HandlerResponse - convert based on type
    if isinstance(response, HandlerResponse):
        if response.resp_type == RESPONSE_TYPE.ERROR:
            err = ErrorResponse(
                error_code=response.error_code,
                error_message=response.error_message,
                is_expected_error=response.is_expected_error,
            )
            err.exception = response.exception
            return err

        if response.resp_type == RESPONSE_TYPE.OK:
            return OkResponse(affected_rows=response.affected_rows)

        # TABLE or COLUMNS_TABLE
        if response.data_frame is not None:
            columns = list(response.data_frame.columns)
        else:
            columns = []

        mysql_types = response.mysql_types
        if mysql_types is None:
            mysql_types = [None] * len(columns)

        return TableResponse(
            data=response.data_frame,
            columns=[
                Column(name=column_name, type=mysql_type) for column_name, mysql_type in zip(columns, mysql_types)
            ],
            data_generator=iter([]),  # empty generator for legacy responses
        )

    # Unknown type - return as-is (shouldn't happen normally)
    return response


# ! deprecated
class HandlerResponse:
    """Legacy response class for compatibility with old code.
    NOTE: do not use this class directly, use DataHandlerResponse instead
    """

    def __init__(
        self,
        resp_type: RESPONSE_TYPE,
        data_frame: pandas.DataFrame = None,
        query: ASTNode = 0,
        error_code: int = 0,
        error_message: str | None = None,
        affected_rows: int | None = None,
        mysql_types: list[MYSQL_DATA_TYPE] | None = None,
        is_expected_error: bool = False,
    ) -> None:
        self.resp_type = resp_type
        self.query = query
        self.data_frame = data_frame
        self.error_code = error_code
        self.error_message = error_message
        self.affected_rows = affected_rows
        if isinstance(self.affected_rows, int) is False or self.affected_rows < 0:
            self.affected_rows = 0
        self.mysql_types = mysql_types
        self.is_expected_error = is_expected_error
        self.exception = None
        current_exception = sys.exc_info()
        if current_exception[0] is not None:
            self.exception = current_exception[1]

    @property
    def type(self):
        return self.resp_type

    def to_columns_table_response(self, map_type_fn: Callable) -> None:
        """Transform the response to a `columns table` response.
        NOTE: original dataframe will be mutated
        """
        if self.resp_type == RESPONSE_TYPE.COLUMNS_TABLE:
            return
        if self.resp_type != RESPONSE_TYPE.TABLE:
            if self.resp_type == RESPONSE_TYPE.ERROR:
                raise ValueError(
                    f"Cannot convert {self.resp_type} to {RESPONSE_TYPE.COLUMNS_TABLE}, "
                    f"the error is: {self.error_message}"
                )
            raise ValueError(f"Cannot convert {self.resp_type} to {RESPONSE_TYPE.COLUMNS_TABLE}")

        self.data_frame.columns = [name.upper() for name in self.data_frame.columns]
        self.data_frame[INF_SCHEMA_COLUMNS_NAMES.MYSQL_DATA_TYPE] = self.data_frame[
            INF_SCHEMA_COLUMNS_NAMES.DATA_TYPE
        ].apply(map_type_fn)

        # region validate df
        current_columns_set = set(self.data_frame.columns)
        if INF_SCHEMA_COLUMNS_NAMES_SET != current_columns_set:
            raise ValueError(f"Columns set for INFORMATION_SCHEMA.COLUMNS is wrong: {list(current_columns_set)}")
        # endregion

        self.data_frame = self.data_frame.astype(
            {
                INF_SCHEMA_COLUMNS_NAMES.COLUMN_NAME: "string",
                INF_SCHEMA_COLUMNS_NAMES.DATA_TYPE: "string",
                INF_SCHEMA_COLUMNS_NAMES.ORDINAL_POSITION: "Int32",
                INF_SCHEMA_COLUMNS_NAMES.COLUMN_DEFAULT: "string",
                INF_SCHEMA_COLUMNS_NAMES.IS_NULLABLE: "string",
                INF_SCHEMA_COLUMNS_NAMES.CHARACTER_MAXIMUM_LENGTH: "Int32",
                INF_SCHEMA_COLUMNS_NAMES.CHARACTER_OCTET_LENGTH: "Int32",
                INF_SCHEMA_COLUMNS_NAMES.NUMERIC_PRECISION: "Int32",
                INF_SCHEMA_COLUMNS_NAMES.NUMERIC_SCALE: "Int32",
                INF_SCHEMA_COLUMNS_NAMES.DATETIME_PRECISION: "Int32",
                INF_SCHEMA_COLUMNS_NAMES.CHARACTER_SET_NAME: "string",
                INF_SCHEMA_COLUMNS_NAMES.COLLATION_NAME: "string",
            }
        )
        self.data_frame.replace([numpy.nan, pandas.NA], None, inplace=True)

        self.resp_type = RESPONSE_TYPE.COLUMNS_TABLE

    def to_json(self):
        try:
            data = None
            if self.data_frame is not None:
                data = self.data_frame.to_json(orient="split", index=False, date_format="iso")
        except Exception as e:
            logger.error("%s.to_json: error - %s", self.__class__.__name__, e)
            data = None
        return {
            "type": self.resp_type,
            "query": self.query,
            "data_frame": data,
            "error_code": self.error_code,
            "error": self.error_message,
        }

    def __repr__(self):
        return "%s: resp_type=%s, query=%s, data_frame=\n%s\nerr_code=%s, error=%s, affected_rows=%s" % (
            self.__class__.__name__,
            self.resp_type,
            self.query,
            self.data_frame,
            self.error_code,
            self.error_message,
            self.affected_rows,
        )
