from typing import List, Dict, Union

from mindsdb.api.common.libs import RESPONSE_TYPE


class SQLAnswer:
    def __init__(self, resp_type: RESPONSE_TYPE, columns: List[Dict] = None, data: List[Dict] = None,
                 status: int = None, state_track: List[List] = None, error_code: Union[int, bytes] = None, error_message: Union[str, bytes] = None):
        self.resp_type = resp_type
        self.columns = columns
        self.data = data
        self.status = status
        self.state_track = state_track
        self.error_code = error_code
        self.error_message = error_message

    @property
    def type(self):
        return self.resp_type
