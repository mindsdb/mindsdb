from typing import Generator
from dataclasses import dataclass

import orjson
import numpy as np
import pandas as pd

from mindsdb.utilities.json_encoder import CustomJSONEncoder
from mindsdb.api.executor.sql_query.result_set import ResultSet
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import MYSQL_DATA_TYPE


@dataclass
class SQLAnswer:
    """Container for SQL query execution results and metadata.

    Attributes:
        resp_type: Type of response (OK, ERROR, TABLE, COLUMNS_TABLE).
        result_set: Query result data as a ResultSet object.
        status: Status code for the response.
        state_track: List of state tracking information.
        error_code: Error code if query execution failed.
        error_message: Human-readable error message if query failed.
        affected_rows: Number of rows affected by the query (for DML operations).
        mysql_types: List of MySQL data types for result columns.
    """
    resp_type: RESPONSE_TYPE = RESPONSE_TYPE.OK
    result_set: ResultSet | None = None
    status: int | None = None
    state_track: list[list] | None = None
    error_code: int | None = None
    error_message: str | None = None
    affected_rows: int | None = None
    mysql_types: list[MYSQL_DATA_TYPE] | None = None

    @property
    def type(self) -> RESPONSE_TYPE:
        """Get the response type.

        Returns:
            RESPONSE_TYPE: The type of this SQL response.
        """
        return self.resp_type

    def stream_http_response_sse(self, context: dict | None) -> Generator[str, None, None]:
        """Stream response in Server-Sent Events (SSE) format.

        Args:
            context: Optional context information.

        Yields:
            str: SSE-formatted data lines (prefixed with "data: ").
        """
        for piece in self.stream_http_response_jsonlines(context=context):
            yield f"data: {piece}\n"

    def stream_http_response_jsonlines(self, context: dict | None) -> Generator[str, None, None]:
        """Stream response as newline-delimited JSON (JSONL).

        Args:
            context: Optional context information.

        Yields:
            str: JSON-encoded lines terminated with newline characters.
        """
        _default_json = CustomJSONEncoder().default

        if self.resp_type in (RESPONSE_TYPE.OK, RESPONSE_TYPE.ERROR):
            response = self.dump_http_response(context=context)
            yield orjson.dumps(response) + "\n"
            return

        yield (
            orjson.dumps(
                {
                    "type": RESPONSE_TYPE.TABLE,
                    "column_names": [column.alias or column.name for column in self.result_set.columns],
                }
            ).decode()
            + "\n"
        )

        for el in self.result_set.stream_data():
            el.replace([np.NaN, pd.NA, pd.NaT], None, inplace=True)
            yield (
                orjson.dumps(
                    el.to_dict("split")["data"],
                    default=_default_json,
                    option=orjson.OPT_SERIALIZE_NUMPY | orjson.OPT_PASSTHROUGH_DATETIME,
                ).decode()
                + "\n"
            )

    def dump_http_response(self, context: dict | None = None) -> dict:
        """Serialize the complete response as a single dictionary.

        Args:
            context: Optional context information.

        Returns:
            dict: Serialized response.
        """
        if context is None:
            context = {}
        if self.resp_type == RESPONSE_TYPE.OK:
            return {
                "type": self.resp_type,
                "affected_rows": self.affected_rows,
                "context": context,
            }
        elif self.resp_type in (RESPONSE_TYPE.TABLE, RESPONSE_TYPE.COLUMNS_TABLE):
            data = self.result_set.to_lists(json_types=True)
            return {
                "type": RESPONSE_TYPE.TABLE,
                "data": data,
                "column_names": [column.alias or column.name for column in self.result_set.columns],
                "context": context,
            }
        elif self.resp_type == RESPONSE_TYPE.ERROR:
            return {
                "type": RESPONSE_TYPE.ERROR,
                "error_code": self.error_code or 0,
                "error_message": self.error_message,
                "context": context,
            }
        else:
            raise ValueError(f"Unsupported response type for dump HTTP response: {self.resp_type}")
