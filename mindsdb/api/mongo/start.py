from mindsdb.utilities.config import Config
from mindsdb.api.mongo.server import run_server
from mindsdb.utilities.log import initialize_log


def start(config, verbose=False):
    config = Config(config)
    if verbose:
        config['log']['level']['console'] = 'DEBUG'

    initialize_log(config, 'mongodb', wrap_print=True)

    run_server(config)
