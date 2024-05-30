from typing import List
from mindsdb.api.executor import ResultSet


class ANSWER_TYPE:
    __slots__ = ()
    TABLE = "table"
    OK = "ok"
    ERROR = "error"


ANSWER_TYPE = ANSWER_TYPE()


class ExecuteAnswer:
    def __init__(
        self,
        # TODO remove, is not used
        answer_type: ANSWER_TYPE,
        data: ResultSet = None,
        status: int = None,
        state_track: List[List] = None,
        error_code: int = None,
        error_message: str = None,
    ):
        self.data = data
        self.status = status
        self.state_track = state_track
        self.error_code = error_code
        self.error_message = error_message
