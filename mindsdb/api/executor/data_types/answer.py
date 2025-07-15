from dataclasses import dataclass
from typing import List, Optional

from mindsdb.api.executor.sql_query.result_set import ResultSet


@dataclass(kw_only=True, slots=True)
class ExecuteAnswer:
    data: Optional[ResultSet] = None
    state_track: Optional[List[List]] = None
    error_code: Optional[int] = None
    error_message: Optional[str] = None
    affected_rows: Optional[int] = None
