"""Parent class for all clients - DB and ML."""
import requests
from pandas import read_json
from mindsdb.integrations.libs.net_helpers import sending_attempts
from mindsdb.utilities.log import get_log

logger = get_log("main")


class Switcher:
    """This class use as a decorator only
    To receive DB or ML handler in case of monolithic mode
    and ML or DB clients in case of modular one.
    Thus we support back compatibility!."""

    def __init__(self, client_class):
        self.client_class = client_class

    def __call__(self, *args, **kwargs):
        client = self.client_class(*args, **kwargs)
        if client.as_service:
            return client
        return client.handler


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
    def __getattr__(self, attr):
        """If by some reasons of our wonderful code organization
        and strong code coupling, there is no requested attribute in Client (ML or DB),
        then the Client delegates this request to a Handler (ML or DB) instantiated and stored
        in self.handler attribute."""
        logger.info(
            "%s: getting '%s' attribute from handler", self.__class__.__name__, attr
        )
        return getattr(self.handler, attr)

    def _convert_response(self, resp):
        """Converts data_frame from json to pandas.DataFrame object.
        Does nothing if data_frame key is not present
        """
        if (
            isinstance(resp, dict)
            and "data_frame" in resp
            and resp["data_frame"] is not None
        ):
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

        logger.info(
            "%s: calling url - %s, params - %s", self.__class__.__name__, url, params
        )
        r = call(url, **params)
        return r
