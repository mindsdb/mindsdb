from mindsdb.utilities.config import Config
from mindsdb.api.mongo.server import run_server
from mindsdb.utilities.log import initialize_log


def start(verbose=False):
    config = Config()
    if verbose:
        config.set(['log', 'level', 'console'], 'DEBUG')

    initialize_log(config, 'mongodb', wrap_print=True)

    run_server(config)
