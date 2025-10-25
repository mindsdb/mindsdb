from http import HTTPStatus
from urllib.parse import urljoin

import requests


class BigCommerceAPIClient:
    def __init__(self, url: str, access_token: str):
        # we have to use both endpoints: v2/ and v3/, so delete it from base url
        self.base_url = url.rstrip("/")
        self.base_url = self.base_url.rstrip("v2")
        self.base_url = self.base_url.rstrip("v3")
        if not self.base_url.endswith("/"):
            self.base_url += "/"

        self.access_token = access_token
        self.session = requests.Session()
        self.session.headers.update(
            {
                "X-Auth-Token": self.access_token,
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

    def get_products(
        self,
        filter: dict = None,
        sort_condition: dict = None,
        limit: int = 999999999,
    ):
        # doc: https://developer.bigcommerce.com/docs/rest-catalog/products#get-all-products
        params = {
            "limit": limit,
        }
        if filter is not None:
            params.update(filter)
        if sort_condition is not None:
            params.update(sort_condition)
        return self._make_request_v3("GET", "catalog/products", params=params)

    def get_customers(
        self,
        filter: dict = None,
        sort_condition: dict = None,
        limit: int = 999999999,
    ):
        # doc: https://developer.bigcommerce.com/docs/rest-management/customers#get-all-customers
        params = {
            "limit": 999999999,
        }
        if filter:
            params.update(filter)
        if sort_condition:
            params["sort"] = sort_condition
        return self._make_request_v3("GET", "customers", params=params)

    def get_orders(
        self,
        filter: dict = None,
        sort_condition: str = None,
        limit: int = 999999999,
    ):
        # doc: https://developer.bigcommerce.com/docs/rest-management/orders#get-all-orders
        params = {"limit": limit}
        if filter:
            params.update(filter)
        if sort_condition:
            params["sort"] = sort_condition

        return self._make_request_v2("GET", "orders", params=filter)

    def get_orders_count(self) -> int:
        response = self._make_request_v2("GET", "orders/count")
        return response["count"]

    def get_customers_count(self) -> int:
        response = self._make_request("GET", urljoin(self.base_url, "v3/customers"), params={"limit": 1})
        return response["meta"]["pagination"]["total"]

    def get_products_count(self) -> int:
        response = self._make_request("GET", urljoin(self.base_url, "v3/products"), params={"limit": 1})
        return response["meta"]["pagination"]["total"]

    def _make_request_v2(self, method: str, url: str, *args, **kwargs) -> list[dict]:
        url = urljoin(urljoin(self.base_url, "v2/"), url)
        response = self._make_request(method, url, *args, **kwargs)
        return response

    def _make_request_v3(self, method: str, url: str, *args, **kwargs) -> list[dict]:
        url = urljoin(urljoin(self.base_url, "v3/"), url)
        response = self._make_request(method, url, *args, **kwargs)
        return response["data"]

    def _make_request(self, method: str, url: str, params: dict = None, data: dict = None) -> dict:
        response = self.session.request(method, url, params=params, json=data)

        if response.status_code == HTTPStatus.NO_CONTENT:
            return []
        if response.status_code != HTTPStatus.OK:
            raise Exception(f"Request failed with status code {response.status_code}: {response.text}")

        return response.json()
