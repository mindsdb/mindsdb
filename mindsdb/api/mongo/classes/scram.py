# https://stackoverflow.com/questions/29298346/xmpp-sasl-scram-sha1-authentication
# https://tools.ietf.org/html/rfc5802

import base64
import hashlib
import hmac
import os

from pymongo.auth import _password_digest


class Scram():
    ''' implementation of server-side SCRAM-SHA-1 auth of mongodb
        TODO add SCRAM-SHA-256 auth
    '''

    def __init__(self, user, password):
        self.user = user
        self.password = password
        self.snonce = base64.b64encode(os.urandom(24)).decode()
        self.salt = base64.b64encode(os.urandom(16))
        self.iterations = 4096
        self.messages = []

    def process_client_first_message(self, payload):
        payload = payload[3:]
        payload_parts = self._split_payload(payload)
        self.client_user = payload_parts['n']
        self.unonce = payload_parts['r']
        self.messages.append(payload)

        responce_msg = f"r={self.unonce}{self.snonce},s={self.salt.decode()},i={self.iterations}"
        self.messages.append(responce_msg)
        return responce_msg

    def process_client_second_message(self, payload):
        self.messages.append(payload[:payload.rfind(',p=')])    # without 'p' part

        # TODO add user password check here

        messages = ','.join(self.messages)
        salted_password = self._salt_password()
        server_key = self._sha1_hmac(salted_password, b'Server Key')
        server_signature = self._sha1_hmac(server_key, messages.encode('utf-8'))
        return f'v={base64.b64encode(server_signature).decode()}'

    def _sha1_hmac(self, key, msg):
        return hmac.new(key, msg, digestmod=hashlib.sha1).digest()

    def _salt_password(self):
        password = _password_digest(self.user, self.password).encode("utf-8")
        return hashlib.pbkdf2_hmac(
            'sha1', password, base64.b64decode(self.salt), self.iterations
        )

    def _split_payload(self, payload):
        parts = {}
        for part in [x for x in payload.split(',') if len(x) > 0]:
            parts[part[0]] = part[2:]
        return parts
