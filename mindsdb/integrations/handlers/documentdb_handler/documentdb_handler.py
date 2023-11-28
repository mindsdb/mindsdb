from collections import OrderedDict

from pymongo import MongoClient

from mindsdb.integrations.handlers.mongodb_handler import Handler as MongoDBHandler
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


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


connection_args = OrderedDict(
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the DocumentDB server.',
        'required': True,
        'label': 'User',
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password to authenticate the user with the DocumentDB server.',
        'required': True,
        'label': 'Password',
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'The database name to use when connecting with the DocumentDB server.',
        'required': True,
        'label': 'Database',
    },
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the DocumentDB server. NOTE: use \'127.0.0.1\' instead of \'localhost\' to connect to local server.',
        'required': True,
        'label': 'Host',
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'The TCP/IP port of the DocumentDB server. Must be an integer.',
        'required': True,
        'label': 'Port',
    },
    kwargs={
        'type': dict,
        'description': 'Additional parameters of DocumentDB same as MongoDB.',
        'required': False,
        'label': 'Kwargs',
    },
)


connection_args_example = OrderedDict(
    host='127.0.0.1',
    port=27017,
    username='documentdb',
    password='password',
    database='database',
)
