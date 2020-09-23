from mindsdb.utilities.config import Config
from mindsdb.api.mongo.server import run_server


def start(config, verbose=False):
    config = Config(config)
    if verbose:
        config['log']['level']['console'] = 'INFO'

    run_server(config)
