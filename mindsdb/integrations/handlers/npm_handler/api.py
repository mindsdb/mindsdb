import requests


class NPM:

    def __init__(self, package_name: str):
        resp = requests.get("https://api.npms.io/v2/package/" + package_name)
        if not resp or resp.status_code != 200:
            raise Exception(f"Unable to get package datails: '{package_name}'")
        self.data = resp.json()

    def get_data(self):
        return self.data

    @staticmethod
    def is_connected():
        return True if requests.get("https://api.npms.io/v2/search?q=a&size=1").status_code == 200 else False

    def get_cols_in(self, path, cols):
        curr_root = self.data
        for p in path:
            curr_root = curr_root[p]
        req_cols = {}
        for col in cols:
            req_cols[col] = curr_root[col] if col in curr_root else {}
        return req_cols
