import requests
from mindsdb.integrations.libs.net_helpers import sending_attempts


class BaseClient:
    def __init__(self, as_service=True):
        self.headers = {"Content-Type": "application/json"}
        self.as_service = as_service

    # Make the wrapper a very thin layout between user and LightwoodHandler
    # in case of local lightwood installation
    def __getattr__(self, attr):
        if self.__dict__["as_service"]:
            return getattr(self, attr)
        handler = self.__dict__["handler"]
        return getattr(handler, attr)

    @sending_attempts()
    def _do(self, endpoint, _type="get", **params):
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
