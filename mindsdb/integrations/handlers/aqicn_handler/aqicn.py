import requests


class AQIClient:
    def __init__(self, api_key):
        self.api_key = api_key
        self.params = {"token": api_key}
        self.base_endpoint = "https://api.waqi.info/feed"

    def make_request(self, url, additionalParams={}):
        newParams = {**self.params, **additionalParams}
        resp = requests.get(url, params=newParams)
        res = resp.json()
        content = {}
        if res["status"] == "ok":
            content = {'content': resp.json(), 'code': 200}
        else:
            content = {'content': resp.json(), 'code': 404}
        return content

    def air_quality_city(self, city):
        url = f'{self.base_endpoint}/{city}/'
        return self.make_request(url)

    def air_quality_lat_lng(self, lat, lng):
        url = f'{self.base_endpoint}/geo:{lat};{lng}/'
        return self.make_request(url)

    def air_quality_user_location(self):
        url = f'{self.base_endpoint}/here/'
        return self.make_request(url)

    def air_quality_station_by_name(self, name):
        url = 'https://api.waqi.info/search/'
        return self.make_request(url, {"keyword": name})
