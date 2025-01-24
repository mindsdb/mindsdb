from typing import List
from mindsdb.api.executor.sql_query.result_set import ResultSet


class ExecuteAnswer:
    def __init__(
        self,
        data: ResultSet = None,
        state_track: List[List] = None,
        error_code: int = None,
        error_message: str = None,
    ):
        self.data = data
        self.state_track = state_track
        self.error_code = error_code
        self.error_message = error_message
