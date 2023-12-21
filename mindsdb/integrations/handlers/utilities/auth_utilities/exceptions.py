
class AuthException(Exception):
    def __init__(self, message, auth_url=None):
        super().__init__(message)

        self.auth_url = auth_url