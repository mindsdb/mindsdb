from mindsdb.api.mongo.server import run_server
from mindsdb.interfaces.storage import db
from mindsdb.utilities import log
from mindsdb.utilities.config import Config
from mindsdb.utilities.functions import init_lexer_parsers


def start(verbose=False):
    logger = log.getLogger(__name__)
    logger.info("Mongo API is starting..")
    config = Config()
    db.init()
    init_lexer_parsers()

    run_server(config)
