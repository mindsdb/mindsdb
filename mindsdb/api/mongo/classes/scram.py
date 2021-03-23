# https://stackoverflow.com/questions/29298346/xmpp-sasl-scram-sha1-authentication
# https://tools.ietf.org/html/rfc5802
# https://tools.ietf.org/html/rfc7677

import base64
import hashlib
import hmac
import os

from pymongo.auth import _password_digest, _xor, saslprep


class Scram():
    ''' implementation of server-side SCRAM-SHA-1 and SCRAM-SHA-256 auth for mongodb
    '''

    def __init__(self, method='sha1', get_salted_password=None):
        self.get_salted_password = get_salted_password
        self.snonce = base64.b64encode(os.urandom(24)).decode()
        self.iterations = 4096
        self.messages = []

        if method == 'sha1':
            self.method_str = 'sha1'
            self.method_func = hashlib.sha1
            self.salt = base64.b64encode(os.urandom(16))
        elif method == 'sha256':
            self.method_str = 'sha256'
            self.method_func = hashlib.sha256
            self.salt = base64.b64encode(os.urandom(28))

    def process_client_first_message(self, payload):
        payload = payload[3:]
        payload_parts = self._split_payload(payload)
        self.client_user = payload_parts['n']

        if self.get_salted_password is not None:
            salt_bytes, self.salted_password = self.get_salted_password(self.client_user, self.method_str)
            self.salt = base64.b64encode(salt_bytes)
        else:
            self.salted_password = self.salt_password()

        self.unonce = payload_parts['r']
        self.messages.append(payload)

        responce_msg = f"r={self.unonce}{self.snonce},s={self.salt.decode()},i={self.iterations}"
        self.messages.append(responce_msg)
        return responce_msg

    def process_client_second_message(self, payload):
        self.messages.append(payload[:payload.rfind(',p=')])    # without 'p' part

        messages = ','.join(self.messages)
        server_key = self._hmac(self.salted_password, b'Server Key')
        server_signature = self._hmac(server_key, messages.encode('utf-8'))

        client_key = self._hmac(self.salted_password, b'Client Key')
        stored_key = self.method_func(client_key).digest()
        client_signature = self._hmac(stored_key, messages.encode('utf-8'))
        expected_client_proof = base64.b64encode(_xor(client_key, client_signature)).decode()

        income_client_proof = payload[payload.rfind(',p=') + 3:]

        if expected_client_proof != income_client_proof:
            raise Exception('wrong password')

        return f'v={base64.b64encode(server_signature).decode()}'

    def _hmac(self, key, msg):
        return hmac.new(key, msg, digestmod=self.method_func).digest()

    def salt_password(self, user, password):
        if self.method_str == 'sha1':
            password = _password_digest(user, password).encode("utf-8")
        elif self.method_str == 'sha256':
            password = saslprep(password).encode("utf-8")

        return hashlib.pbkdf2_hmac(
            self.method_str, password, base64.b64decode(self.salt), self.iterations
        )

    def _split_payload(self, payload):
        parts = {}
        for part in [x for x in payload.split(',') if len(x) > 0]:
            parts[part[0]] = part[2:]
        return parts
