import requests
from urllib.parse import urljoin

from mindsdb.utilities import log

logger = log.getLogger(__name__)


def move_under(d, key_contents_to_move, key_to_move_under=None):
    """
    Moves the keys in nested dict with key key_contents_to_move under the dict defined by key_to_move_under.
    If no key_to_move_under is provided, the keys are moved under d.
    eg.
    Calling this on the following dict (d) with key_contents_to_move = "a":
    {
        "a": {
            "b": 1,
            "c": 2
        }
    }
    results in:
    {
        "b": 1,
        "c": 2
    }
    """
    if key_contents_to_move not in d:
        return
    for k, v in d[key_contents_to_move].items():
        if key_to_move_under:
            d[key_to_move_under][k] = v
        else:
            d[k] = v
    del d[key_contents_to_move]


class SAPERP:

    def __init__(self, url: str, api_key: str) -> None:
        self.base_url = url
        self.api_key = api_key

    def _request(self, method: str, relative_endpoint: str, data=None):
        kwargs = {
            "method": method,
            "url": urljoin(self.base_url, relative_endpoint),
            "headers": {
                "APIKey": self.api_key,
                "Accept": "application/json",
                "DataServiceVersion": "2.0"
            }
        }
        if data is not None:
            kwargs["data"] = data
        return requests.request(**kwargs)

    def is_connected(self) -> bool:
        if self._request("get", "").ok:
            return True
        return False

    def get(self, endpoint):
        """ Common method for all get endpoints """
        try:
            resp = self._request("get", endpoint)
            if resp.ok:
                resp = resp.json()["d"]
                if "results" in resp:
                    resp = resp["results"]
                else:
                    resp = [resp]
            else:
                resp = []
            for r in resp:
                move_under(r, "__metadata")
            return resp
        except Exception as e:
            logger.error(f"Error requesting endpoint {endpoint}: {e}")
            return {}
