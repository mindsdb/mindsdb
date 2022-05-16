import pytds


class MSSQLConnectionChecker:
    def __init__(self, **kwargs):
        self.host = kwargs.get('host')
        self.port = kwargs.get('port')
        self.user = kwargs.get('user')
        self.password = kwargs.get('password')

    def _get_connnection(self):
        return pytds.connect(
            user=self.user,
            password=self.password,
            dsn=self.host,
            port=self.port,
            as_dict=True,
            autocommit=True  # .commit() doesn't work
        )

    def check_connection(self):
        try:
            conn = self._get_connnection()
            conn.close()
            connected = True
        except Exception:
            connected = False

        return connected
