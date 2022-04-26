
from typing import List, Dict
class ANSWER_TYPE:
    __slots__ = ()
    TABLE = 'table'
    OK = 'ok'
    ERROR = 'error'


ANSWER_TYPE = ANSWER_TYPE()


class ExecuteAnswer:
    def __init__(self,
                 # TODO remove, is not used
                 answer_type: ANSWER_TYPE,
                 columns = None,
                 data: List[List] = None,
                 status: int = None,
                 state_track: List[List] = None,
                 error_code: int = None,
                 error_message: str = None,
                 # model_types = None
                 ):
        # self.answer_type = answer_type
        self.columns = columns
        self.data = data
        self.status = status
        self.state_track = state_track
        self.error_code = error_code
        self.error_message = error_message
        # self.model_types = model_types

