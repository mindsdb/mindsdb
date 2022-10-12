from pandas import DataFrame

from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
from mindsdb_sql.parser.ast import ASTNode


class HandlerResponse:
    def __init__(self, resp_type: RESPONSE_TYPE, data_frame: DataFrame = None,
                 query: ASTNode = 0, error_code: int = 0, error_message: str = None) -> None:
        self.resp_type = resp_type
        self.query = query

        self.data_frame = data_frame
        self.error_code = error_code
        self.error_message = error_message

    @property
    def type(self):
        return self.resp_type

    def to_json(self):
        try:
            data = self.data_frame.to_json(orient="split", index=False)
        except Exception:
            data = None
        return  {"type": self.resp_type,
                 "query": self.query,
                 "data_frame": data,
                 "error_code": self.error_code,
                 "error": self.error_message}

class HandlerStatusResponse:
    def __init__(self, success: bool = True, error_message: str = None) -> None:
        self.success = success
        self.error_message = error_message

    def to_json(self):
        return {"success": self.success,
                "error": self.error_message}
