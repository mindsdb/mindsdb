import requests


class ClickhouseConnectionChecker:
    def __init__(self, **kwargs):
        self.host = kwargs.get("host")
        self.port = kwargs.get("port")
        self.user = kwargs.get("user")
        self.password = kwargs.get("password")

    def check_connection(self):
        try:
            res = requests.post(f"http://{self.host}:{self.port}",
                                data="select 1;",
                                params={'user': self.user, 'password': self.password})
            connected = res.status_code == 200
        except Exception:
            connected = False
        return connected
