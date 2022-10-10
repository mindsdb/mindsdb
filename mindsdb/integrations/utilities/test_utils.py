from mindsdb.integrations.handlers.postgres_handler import Handler as PGHandler


PG_HANDLER_NAME = 'test_handler'
PG_CONNECTION_DATA = {
    "user": "demo_user",
    "password": "demo_password",
    "host": "3.220.66.106",
    "port": "5432",
    "database": "demo"
}


class HandlerControllerMock:
    def __init__(self):
        self.handlers = {
            PG_HANDLER_NAME: PGHandler(
                PG_HANDLER_NAME,
                **{"connection_data": PG_CONNECTION_DATA}
            )
        }

    def get_handler(self, name):
        return self.handlers[name]

    def get(self, name):
        return {
            'id': 0,
            'name': PG_HANDLER_NAME
        }
