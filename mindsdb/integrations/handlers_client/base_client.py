"""Parent class for all clients - DB and ML."""
import requests
from pandas import read_json
from mindsdb.integrations.libs.net_helpers import sending_attempts
from mindsdb.utilities import log


class BaseClient:
    """A base parent class for DB and ML client implementations.

    Attributes:
        headers: dict of default headers
        as_service: if false - delegates all calls to a handler instance
    """
    def __init__(self, as_service=True):
        self.headers = {"Content-Type": "application/json"}
        self.as_service = as_service

    # Make the wrapper a very thin layout between user and LightwoodHandler
    # in case of local lightwood installation
    def __getattr__(self, attr):
        """Delegates all calls to a handler instance if self.as_service == False."""
        log.logger.info("calling '%s' as: ", attr)
        if self.__dict__["as_service"]:
            log.logger.info("service")
            return getattr(self, attr)
        log.logger.info("handler")
        handler = self.__dict__["handler"]
        return getattr(handler, attr)

    def _convert_response(self, resp):
        """Converts data_frame from json to pandas.DataFrame object.

        Does nothing if data_frame key is not present
        """
        if isinstance(resp, dict) and "data_frame" in resp and resp["data_frame"] is not None:
            df = read_json(resp["data_frame"], orient="split")
            resp["data_frame"] = df
        return resp

    @sending_attempts()
    def _do(self, endpoint, _type="get", **params) -> requests.Response:
        """Performs several attempts to send a request to the service.

        Args:
            endpoint: a service endpoint name
            _type: request type (get, put, post)
            params: dict of request params

        Raises:
            Exception if all attempts were failed.
        """
        call = None
        _type = _type.lower()
        if _type == "get":
            call = requests.get
        elif _type == "post":
            call = requests.post
        elif _type == "put":
            call = requests.put

        url = f"{self.base_url}/{endpoint}"

        headers = params.get("headers", None)
        if headers is None:
            headers = self.headers
        else:
            headers.update(self.headers)
        params["headers"] = headers

        r = call(url, **params)
        return r
