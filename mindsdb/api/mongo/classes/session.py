import base64

from mindsdb.api.mongo.classes.scram import Scram
from mindsdb.utilities.config import Config


class Session():
    def __init__(self, config):
        self.config = config

    def init_scram(self, method):
        self.scram = Scram(method=method, get_salted_password=self.get_salted_password)

    def get_salted_password(self, username, method=None):
        real_user = self.config['api']['mongodb'].get('user', '')
        password = self.config['api']['mongodb'].get('password', '')
        if username != real_user:
            raise Exception(f'Wrong username {username}')

        salted_password = self.scram.salt_password(real_user, password)

        return base64.b64decode(self.scram.salt), salted_password
