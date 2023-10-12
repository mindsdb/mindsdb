import requests

class SharepointAPI:
    def __init__(self, client_id: str, client_secret: str, tenant_id: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.bearer_token = None
        self.connected = False

    def get_bearer_token(self):
        try:
            self.bearer_token = requests.post(url=f'https://login.microsoftonline.com/{self.tenant_id}/oauth2/token',
                                              )