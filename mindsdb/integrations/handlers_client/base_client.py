"""Parent class for all clients - DB and ML."""
import logging
import requests
from pandas import read_json
from mindsdb.integrations.libs.net_helpers import sending_attempts
# from mindsdb.utilities import log
logger = logging.getLogger("mindsdb.main")


class BaseClient:
    """A base parent class for DB and ML client implementations.

    Attributes:
        headers: dict of default headers
        as_service: if false - delegates all calls to a handler instance
    """
    def __init__(self, as_service=True):
        self.headers = {"Content-Type": "application/json"}
        self.as_service = as_service

    # Make the wrapper a very thin layout between user and Handler
    # in case of monolithic usage
    def __getattribute__(self, attr):
        """Delegates all calls to a handler instance if self.as_service == False."""
        name =  object.__getattribute__(self, "__class__").__name__
        logger.info("%s: calling '%s' attribute", name, attr)
        # attrs_dict = object.__getattribute__(self, "__dict__")
        # logger.info("%s: __dict__ - %s", name, attrs_dict)
        try:
            handler = object.__getattribute__(self, "handler")
            as_service = object.__getattribute__(self, "as_service")
            if as_service:
                logger.info("%s works in a service mode. calling %s attribute", name, attr)
                try:
                    return object.__getattribute__(self, attr)
                except AttributeError:
                    logger.info("%s: '%s' attribute not found, get it from local handler instance", name, attr)
                    # handler = attrs_dict["handler"]
                    return getattr(handler, attr)
            logger.info("%s works in a local mode. calling %s attribute", name, attr)
            # handler = self.__dict__["handler"]
            return getattr(handler, attr)
        except AttributeError:
            return object.__getattribute__(self, attr)

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

        logger.info("%s: calling url - %s, params - %s", self.__class__.__name__, url, params)
        r = call(url, **params)
        return r
