import json
import requests


class LumaClient:
    def __init__(self, api_key):
        self.auth_token = api_key
        self.luma_base_endpoint = "https://api.lu.ma/"
        self.validate_api_key()

    def make_request(self, url, method='GET', payload=None):
        if method not in ['GET', 'POST']:
            raise ValueError('Invalid HTTP request method')
        headers = {"accept": "application/json", "content-type": "application/json"}
        if self.auth_token:
            headers['x-luma-api-key'] = self.auth_token
        request_method = getattr(requests, method.lower())
        resp = request_method(url, json=payload, headers=headers)
        return {"content": resp.json(), "code": resp.status_code}

    def validate_api_key(self):
        url = f'{self.luma_base_endpoint}public/v1/user/get-self'
        content = self.make_request(url)
        if content['code'] != 200:
            raise Exception("User Authentication failed - " + json.dumps(content["content"]))
        return content

    def create_event(self, data):
        url = f'{self.luma_base_endpoint}public/v1/event/create'
        content = self.make_request(url, method="POST", payload=data)
        if content['code'] != 200:
            raise Exception("Create failed - " + json.dumps(content["content"]))
        return content

    def get_event(self, event_api_id):
        url = f'{self.luma_base_endpoint}public/v1/event/get?api_id={event_api_id}'
        content = self.make_request(url)
        if content['code'] != 200:
            raise Exception("Get event failed - " + json.dumps(content["content"]))
        return content

    def list_events(self):
        url = f'{self.luma_base_endpoint}public/v1/calendar/list-events?series_mode=sessions'
        content = self.make_request(url)
        if content['code'] != 200:
            raise Exception("Get event failed - " + json.dumps(content["content"]))
        return content
