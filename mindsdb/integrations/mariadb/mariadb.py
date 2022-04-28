import mysql.connector


class MariadbConnectionChecker:
    def __init__(self, **kwargs):
        self.host = kwargs.get('host')
        self.port = kwargs.get('port')
        self.user = kwargs.get('user')
        self.password = kwargs.get('password')
        self.ssl = kwargs.get('ssl')
        self.ssl_ca = kwargs.get('ssl_ca')
        self.ssl_cert = kwargs.get('ssl_cert')
        self.ssl_key = kwargs.get('ssl_key')

    def check_connection(self):
        try:
            config = {
                "host": self.host,
                "port": self.port,
                "user": self.user,
                "password": self.password
            }
            if self.ssl is True:
                config['client_flags'] = [mysql.connector.constants.ClientFlag.SSL]
                if self.ssl_ca is not None:
                    config["ssl_ca"] = self.ssl_ca
                if self.ssl_cert is not None:
                    config["ssl_cert"] = self.ssl_cert
                if self.ssl_key is not None:
                    config["ssl_key"] = self.ssl_key
            con = mysql.connector.connect(**config)
            connected = con.is_connected()
            con.close()
        except Exception:
            connected = False
        return connected
