from pymongo import MongoClient

from mindsdb.integrations.handlers.mongodb_handler import Handler as MongoDBHandler


class DocumentDBHandler(MongoDBHandler):
    """
    This handler handles connection and execution of the DocumentDB statements.
    """

    name = 'documentdb'

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
        connection_data = kwargs.get('connection_data', {})
        self.host = connection_data.get("host")
        self.port = int(connection_data.get("port") or 27017)
        self.user = connection_data.get("username")
        self.password = connection_data.get("password")
        self.database = connection_data.get('database')
        self.flatten_level = connection_data.get('flatten_level', 0)
        self.mykwargs = connection_data.get('kwargs', {})

        self.connection = None
        self.is_connected = False

    def connect(self):
        kwargs = {}
        if isinstance(self.user, str) and len(self.user) > 0:
            kwargs['username'] = self.user

        if isinstance(self.password, str) and len(self.password) > 0:
            kwargs['password'] = self.password

        connection = MongoClient(
            host=self.host, port=self.port, **{**kwargs, **self.mykwargs}
        )

        self.is_connected = True
        self.connection = connection
        return self.connection
