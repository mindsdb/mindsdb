from collections import OrderedDict
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

from ..mysql_handler import Handler as MySQLHandler
from rockset import Client


class RocksetHandler(MySQLHandler):
    """
    This handler handles connection and execution of the Rockset integration
    """
    name = 'rockset'

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)


connection_args = OrderedDict([
    user = {
        'type': ARG_TYPE.STRING,
        'description': 'Rockset user name'
    },
    password = {
        'type': ARG_TYPE.STRING,
        'description': 'Rockset password'
    },
    api_key = {
        'type': ARG_TYPE.STRING,
        'description': 'Rockset API key'
    },
    api_server = {
        'type': ARG_TYPE.STRING,
        'description': 'Rockset API server'
    },
    host = {
        'type': ARG_TYPE.STRING,
        'description': 'Rockset host'
    },
    port = {
        'type': ARG_TYPE.STRING,
        'description': 'Rockset port'
    },
    database = {
        'type': ARG_TYPE.STRING,
        'description': 'Rockset database'
    }

connection_args_example = OrderedDict(
    user = 'rockset',
    password = 'rockset',
    api_key = "adkjf234rksjfa23waejf2",
    api_server = 'api-us-west-2.rockset.io',
    host = 'localhost',
    port = '3306',
    database = 'test'