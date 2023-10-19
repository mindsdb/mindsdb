import requests


class OilPriceAPIClient:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_endpoint = "https://api.oilpriceapi.com/v1/prices"
        self.valid_values_by_type = ["spot_price", "daily_average_price"]
        self.valid_values_by_code = ["BRENT_CRUDE_USD", "WTI_USD"]

    def make_request(self, url, params={}):
        headers = {'Content-type': 'application/json'}
        if self.api_key:
            headers['Authorization'] = 'Token ' + self.api_key
        resp = requests.get(url, headers=headers, params=params)
        content = {}
        if resp.status_code == 200:
            content = {'content': resp.json(), 'code': 200}
        else:
            content = {'content': {}, 'code': resp.status_code, 'error': resp.text}
        return content

    def _is_valid_by_type(self, val):
        return val in self.valid_values_by_type

    def _is_valid_by_code(self, val):
        return val in self.valid_values_by_code

    def create_params_dict(self, by_type, by_code):
        params = {}
        if by_type is not None:
            params["by_type"] = by_type
        if by_code is not None:
            params["by_code"] = by_code
        return params

    def get_latest_price(self, by_type=None, by_code=None):
        url = f'{self.base_endpoint}/latest/'
        params = self.create_params_dict(by_type=by_type, by_code=by_code)
        return self.make_request(url, params=params)

    def get_price_past_day(self, by_type=None, by_code=None):
        url = f'{self.base_endpoint}/past_day/'
        params = self.create_params_dict(by_type=by_type, by_code=by_code)
        return self.make_request(url, params=params)
