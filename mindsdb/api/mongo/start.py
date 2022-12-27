from mindsdb.utilities.config import Config
from mindsdb.api.mongo.server import run_server
from mindsdb.utilities.log import initialize_log
from mindsdb.interfaces.storage import db


def start(verbose=False):
    config = Config()
    db.init()
    initialize_log(config, 'mongodb', wrap_print=True)

    run_server(config)
