from pandas import DataFrame

from mindsdb.utilities import log
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from mindsdb_sql_parser.ast import ASTNode


logger = log.getLogger(__name__)

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
            data = None
            if self.data_frame is not None:
                data = self.data_frame.to_json(orient="split", index=False, date_format="iso")
        except Exception as e:
            logger.error("%s.to_json: error - %s", self.__class__.__name__, e)
            data = None
        return  {"type": self.resp_type,
                 "query": self.query,
                 "data_frame": data,
                 "error_code": self.error_code,
                 "error": self.error_message}

    def __repr__(self):
        return "%s: resp_type=%s, query=%s, data_frame=%s, err_code=%s, error=%s" % (
                self.__class__.__name__,
                self.resp_type,
                self.query,
                self.data_frame,
                self.error_code,
                self.error_message
            )

class HandlerStatusResponse:
    def __init__(self, success: bool = True,
                 error_message: str = None,
                 redirect_url: str = None,
                 copy_storage: str = None
    ) -> None:
        self.success = success
        self.error_message = error_message
        self.redirect_url = redirect_url
        self.copy_storage = copy_storage

    def to_json(self):
        data = {"success": self.success, "error": self.error_message}
        if self.redirect_url is not None:
            data['redirect_url'] = self.redirect_url
        return data

    def __repr__(self):
        return f"{self.__class__.__name__}: success={self.success},\
              error={self.error_message},\
              redirect_url={self.redirect_url}"


class ExecutorResponse:
    def __init__(self, resp_type: RESPONSE_TYPE, query: object, error_code: int = 0, error_message: str = None):
        self.resp_type = resp_type
        self.query = query

        self.error_code = error_code
        self.error_message = error_message

    @property
    def type(self):
        return self.resp_type

    def to_json(self):
        return  {"type": self.resp_type,
                 "query": self.query,
                 "error_code": self.error_code,
                 "error": self.error_message}

    def __repr__(self):
        return "%s: resp_type=%s, query=%s, err_code=%s, error=%s" % (
                self.__class__.__name__,
                self.resp_type,
                self.query,
                self.error_code,
                self.error_message,
            )
