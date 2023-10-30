import requests


class ZipCodeBaseClient:

    def __init__(self, api_key):
        self.api_key = api_key
        self.base_endpoint = "https://app.zipcodebase.com/api/v1"

    def make_request(self, url, params=None):
        headers = {'Content-type': 'application/json'}
        if self.api_key:
            headers['apikey'] = self.api_key
        resp = requests.get(url, headers=headers, params=params)
        content = {}
        if resp.status_code == 200:
            content = {'content': resp.json(), 'code': 200}
        else:
            content = {'content': {}, 'code': resp.status_code, 'error': resp.text}
        return content

    def code_to_location(self, codes):
        url = f'{self.base_endpoint}/search'
        params = (
            ("codes", codes),
        )
        return self.make_request(url, params)

    def codes_within_radius(self, code, radius, country, unit):
        url = f'{self.base_endpoint}/radius'
        params = (
            ("code", code),
            ("radius", radius),
            ("country", country),
            ("unit", unit),
        )
        return self.make_request(url, params)

    def codes_by_city(self, city, country, limit):
        url = f'{self.base_endpoint}/code/city'
        params = (
            ("city", city),
            ("country", country),
            ("limit", limit),
        )
        return self.make_request(url, params)

    def codes_by_state(self, state, country, limit):
        url = f'{self.base_endpoint}/code/state'
        params = (
            ("state_name", state),
            ("country", country),
            ("limit", limit),
        )
        return self.make_request(url, params)

    def states_by_country(self, country):
        url = f'{self.base_endpoint}/country/province'
        params = (
            ("country", country),
        )
        return self.make_request(url, params)

    def remaining_requests(self):
        url = f'{self.base_endpoint}/status'
        params = ()
        return self.make_request(url, params)
