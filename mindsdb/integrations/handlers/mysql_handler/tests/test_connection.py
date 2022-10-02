from mindsdb.integrations.handlers.mysql_handler.mysql_handler import MySQLHandler
kwargs = {"connection_data": {
                    "host": "localhost",
                    "port": "3307",
                    "user": "ssl_user",
                    "password": "ssl",
                    "database": "test",
                    "ssl": True,
                    "ssl_ca": "./mysql/ca.pem",
                    "ssl_cert": "./mysql/client-cert.pem",
                    "ssl_key": "./mysql/client-key.pem",
        }
}
handler = MySQLHandler('test_mysql_handler', **kwargs)
handler.connect()

