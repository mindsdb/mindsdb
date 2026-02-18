from http import HTTPStatus
from urllib.parse import urljoin

import requests


DEFAULT_LIMIT = 999999999


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
        limit: int = None,
    ):
        # doc: https://developer.bigcommerce.com/docs/rest-catalog/products#get-all-products
        params = {
            "limit": limit or DEFAULT_LIMIT,
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
        limit: int = None,
    ):
        # doc: https://developer.bigcommerce.com/docs/rest-management/customers#get-all-customers
        params = {
            "limit": limit or DEFAULT_LIMIT,
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
        limit: int = None,
    ):
        # doc: https://developer.bigcommerce.com/docs/rest-management/orders#get-all-orders
        params = {"limit": limit or DEFAULT_LIMIT}
        if filter:
            params.update(filter)
        if sort_condition:
            params["sort"] = sort_condition

        return self._make_request_v2("GET", "orders", params=params)

    def get_orders_count(self) -> int:
        response = self._make_request_v2("GET", "orders/count")
        return response["count"]

    def get_customers_count(self) -> int:
        response = self._make_request("GET", urljoin(self.base_url, "v3/customers"), params={"limit": 1})
        return response["meta"]["pagination"]["total"]

    def get_products_count(self) -> int:
        response = self._make_request("GET", urljoin(self.base_url, "v3/products"), params={"limit": 1})
        return response["meta"]["pagination"]["total"]

    def get_categories(
        self,
        filter: dict = None,
        limit: int = None,
    ):
        # doc: https://developer.bigcommerce.com/docs/rest-catalog/category-trees/categories#get-all-categories
        params = {
            "limit": limit or DEFAULT_LIMIT,
        }
        if filter is not None:
            params.update(filter)
        return self._make_request_v3("GET", "catalog/trees/categories", params=params)

    def get_categories_count(self) -> int:
        response = self._make_request("GET", urljoin(self.base_url, "v3/catalog/trees/categories"), params={"limit": 1})
        return response.get("meta", {}).get("pagination", {}).get("total", 0)

    def get_pickups(
        self,
        filter: dict = None,
        limit: int = None,
    ):
        # doc: https://developer.bigcommerce.com/docs/rest-management/pickup#get-pickups
        params = {
            "limit": limit or DEFAULT_LIMIT,
        }
        if filter is not None:
            params.update(filter)
        return self._make_request_v3("GET", "orders/pickups", params=params)

    def get_pickups_count(self) -> int:
        response = self._make_request("GET", urljoin(self.base_url, "v3/pickups"), params={"limit": 1})
        return response.get("meta", {}).get("pagination", {}).get("total", 0)

    def get_promotions(
        self,
        filter: dict = None,
        sort_condition: dict = None,
        limit: int = None,
    ):
        # doc: https://developer.bigcommerce.com/docs/rest-management/promotions/promotions-bulk#get-all-promotions
        params = {
            "limit": limit or DEFAULT_LIMIT,
        }
        if filter is not None:
            params.update(filter)
        if sort_condition is not None:
            params.update(sort_condition)
        return self._make_request_v3("GET", "promotions", params=params)

    def get_promotions_count(self) -> int:
        response = self._make_request("GET", urljoin(self.base_url, "v3/promotions"), params={"limit": 1})
        return response.get("meta", {}).get("pagination", {}).get("total", 0)

    def get_wishlists(
        self,
        filter: dict = None,
        limit: int = None,
    ):
        # doc: https://developer.bigcommerce.com/docs/rest-management/wishlists#get-all-wishlists
        params = {
            "limit": limit or DEFAULT_LIMIT,
        }
        if filter is not None:
            params.update(filter)
        return self._make_request_v3("GET", "wishlists", params=params)

    def get_wishlists_count(self) -> int:
        response = self._make_request("GET", urljoin(self.base_url, "v3/wishlists"), params={"limit": 1})
        return response.get("meta", {}).get("pagination", {}).get("total", 0)

    def get_segments(
        self,
        filter: dict = None,
        limit: int = None,
    ):
        # doc: https://developer.bigcommerce.com/docs/rest-management/customer-segmentation/segments#get-all-segments
        params = {
            "limit": limit or DEFAULT_LIMIT,
        }
        if filter is not None:
            params.update(filter)
        return self._make_request_v3("GET", "segments", params=params)

    def get_segments_count(self) -> int:
        response = self._make_request("GET", urljoin(self.base_url, "v3/segments"), params={"limit": 1})
        return response.get("meta", {}).get("pagination", {}).get("total", 0)

    def get_brands(
        self,
        filter: dict = None,
        sort_condition: dict = None,
        limit: int = None,
    ):
        # doc: https://developer.bigcommerce.com/docs/rest-catalog/brands#get-all-brands
        params = {
            "limit": limit or DEFAULT_LIMIT,
        }
        if filter is not None:
            params.update(filter)
        if sort_condition is not None:
            params.update(sort_condition)
        return self._make_request_v3("GET", "catalog/brands", params=params)

    def get_brands_count(self) -> int:
        response = self._make_request("GET", urljoin(self.base_url, "v3/catalog/brands"), params={"limit": 1})
        return response.get("meta", {}).get("pagination", {}).get("total", 0)

    def _make_request_v2(self, method: str, url: str, *args, **kwargs) -> list[dict]:
        # NOTE: v2 limit max is 250
        url = urljoin(urljoin(self.base_url, "v2/"), url)
        api_limit = 250
        params = kwargs.pop("params", {})
        request_limit = params.get("limit", DEFAULT_LIMIT)
        params["limit"] = min(api_limit, request_limit)
        current_page = 1
        response_len = 1
        data = []
        while response_len > 0 and len(data) < request_limit:
            params["page"] = current_page
            response = self._make_request(method, url, params=params, *args, **kwargs)
            if isinstance(response, dict):
                # for "get_count" requests
                return response
            current_page += 1
            response_len = len(response)
            data += response
        return data[:request_limit]

    def _make_request_v3(self, method: str, url: str, *args, **kwargs) -> list[dict]:
        url = urljoin(urljoin(self.base_url, "v3/"), url)
        data = []
        params = kwargs.pop("params", {})
        current_page = 1
        total_pages = 1
        while current_page <= total_pages:
            params["page"] = current_page
            response = self._make_request(method, url, params=params, *args, **kwargs)
            current_page = response["meta"]["pagination"]["current_page"] + 1
            total_pages = response["meta"]["pagination"]["total_pages"]
            data += response["data"]
        return data

    def _make_request(self, method: str, url: str, params: dict = None, data: dict = None) -> dict:
        response = self.session.request(method, url, params=params, json=data)

        if response.status_code == HTTPStatus.NO_CONTENT:
            return []
        if response.status_code != HTTPStatus.OK:
            raise Exception(f"Request failed with status code {response.status_code}: {response.text}")

        return response.json()
