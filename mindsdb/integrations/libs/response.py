from typing import List, Dict

from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE


class HandlerResponse:
    def __init__(self, resp_type: RESPONSE_TYPE, columns: List[Dict] = None, data: List[Dict] = None,
                 status: int = None, state_track: List[List] = None, error_code: int = None, error_message: str = None):
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
