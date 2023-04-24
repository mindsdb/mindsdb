from mindsdb.utilities.config import Config
from mindsdb.api.mongo.server import run_server
from mindsdb.interfaces.storage import db
from mindsdb.utilities.log import initialize_log
from mindsdb.utilities.functions import init_lexer_parsers


def start(verbose=False):
    config = Config()
    db.init()
    init_lexer_parsers()

    initialize_log(config, 'mongodb', wrap_print=True)

    run_server(config)
