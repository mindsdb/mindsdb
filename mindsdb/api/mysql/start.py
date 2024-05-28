import mindsdb.interfaces.storage.db as db
from mindsdb.api.mysql.mysql_proxy.mysql_proxy import MysqlProxy
from mindsdb.utilities import log
from mindsdb.utilities.functions import init_lexer_parsers

logger = log.getLogger(__name__)

def initialize_services():
    """Initialize database and lexer parsers."""
    try:
        logger.info("Initializing database...")
        db.init()
        logger.info("Database initialized successfully.")
        
        logger.info("Initializing lexer parsers...")
        init_lexer_parsers()
        logger.info("Lexer parsers initialized successfully.")
    except Exception as e:
        logger.error(f"Initialization error: {e}")
        raise

def start_mysql_proxy():
    """Start the MySQL proxy."""
    try:
        logger.info("Starting MySQL Proxy...")
        MysqlProxy.startProxy()
        logger.info("MySQL Proxy started successfully.")
    except Exception as e:
        logger.error(f"Failed to start MySQL Proxy: {e}")
        raise

def start(verbose=False):
    """Start the MySQL API service."""
    log_level = 'DEBUG' if verbose else 'INFO'
    log.setLevel(log_level)
    logger.info("MySQL API is starting...")

    try:
        initialize_services()
        start_mysql_proxy()
    except Exception as e:
        logger.error(f"MySQL API failed to start: {e}")

if __name__ == "__main__":
    start(verbose=True)
