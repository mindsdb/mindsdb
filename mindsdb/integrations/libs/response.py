from typing import Callable
from dataclasses import dataclass, fields

import numpy
import pandas

from mindsdb.utilities import log
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import MYSQL_DATA_TYPE
from mindsdb_sql_parser.ast import ASTNode


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


class HandlerResponse:
    def __init__(
            self, resp_type: RESPONSE_TYPE, data_frame: pandas.DataFrame = None, query: ASTNode = 0,
            error_code: int = 0, error_message: str | None = None, affected_rows: int | None = None,
            mysql_types: list[MYSQL_DATA_TYPE] | None = None
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
            raise ValueError(
                f"Cannot convert {self.resp_type} to {RESPONSE_TYPE.COLUMNS_TABLE}"
            )

        self.data_frame.columns = [name.upper() for name in self.data_frame.columns]
        self.data_frame[INF_SCHEMA_COLUMNS_NAMES.MYSQL_DATA_TYPE] = self.data_frame[
            INF_SCHEMA_COLUMNS_NAMES.DATA_TYPE
        ].apply(map_type_fn)

        # region validate df
        current_columns_set = set(self.data_frame.columns)
        if INF_SCHEMA_COLUMNS_NAMES_SET != current_columns_set:
            raise ValueError(
                f"Columns set for INFORMATION_SCHEMA.COLUMNS is wrong: {list(current_columns_set)}"
            )
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
        self.data_frame.replace([numpy.NaN, pandas.NA], None, inplace=True)

        self.resp_type = RESPONSE_TYPE.COLUMNS_TABLE

    def to_json(self):
        try:
            data = None
            if self.data_frame is not None:
                data = self.data_frame.to_json(
                    orient="split", index=False, date_format="iso"
                )
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
        return data

    def __repr__(self):
        return f"{self.__class__.__name__}: success={self.success},\
              error={self.error_message},\
              redirect_url={self.redirect_url}"
