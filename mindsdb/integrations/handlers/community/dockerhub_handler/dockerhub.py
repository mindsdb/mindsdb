import json
import requests


class DockerHubClient:
    def __init__(self):
        self.auth_token = None
        self.docker_hub_base_endpoint = "https://hub.docker.com/v2/"

    def make_request(self, url, method='GET', data=None):
        if method not in ['GET', 'POST']:
            raise ValueError('Invalid HTTP request method')
        headers = {'Content-type': 'application/json'}
        if self.auth_token:
            headers['Authorization'] = 'JWT ' + self.auth_token
        request_method = getattr(requests, method.lower())
        if data and len(data) > 0:
            data = json.dumps(data)
            resp = request_method(url, data, headers=headers)
        else:
            resp = request_method(url, headers=headers)
        content = {}
        if resp.status_code == 200:
            content = {'content': json.loads(resp.content.decode()), 'code': 200}
        else:
            content = {'content': {}, 'code': resp.status_code, 'error': resp.text}
        return content

    def login(self, username=None, password=None):
        data = {'username': username, 'password': password}
        self.auth_token = None
        resp = self.make_request(self.docker_hub_base_endpoint + 'users/login/', 'POST', data)
        if resp['code'] == 200:
            self.auth_token = resp['content']['token']
        return resp

    def get_images_summary(self, namespace, repo):
        url = f'{self.docker_hub_base_endpoint}namespaces/{namespace}/repositories/{repo}/images-summary'
        return self.make_request(url)

    def get_repo_images(self, namespace, repo):
        url = f'{self.docker_hub_base_endpoint}namespaces/{namespace}/repositories/{repo}/images'
        return self.make_request(url)

    def get_repo_tag(self, namespace, repo, tag):
        url = f'{self.docker_hub_base_endpoint}namespaces/{namespace}/repositories/{repo}/tags/{tag}'
        return self.make_request(url)

    def get_repo_tags(self, namespace, repo):
        url = f'{self.docker_hub_base_endpoint}namespaces/{namespace}/repositories/{repo}/tags'
        return self.make_request(url)

    def get_org_settings(self, namespace):
        url = f'{self.docker_hub_base_endpoint}orgs/{namespace}/settings'
        return self.make_request(url)
