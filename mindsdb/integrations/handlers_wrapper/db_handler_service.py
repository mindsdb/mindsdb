import os
import json
from mindsdb.utilities import log
from mindsdb.integrations.handlers_wrapper.db_handler_wrapper import DBHandlerWrapper
from mindsdb.utilities.config import Config
import mindsdb.interfaces.storage.db as db
from mindsdb.utilities.log import (
    initialize_log,
    get_log
)


if __name__ == "__main__":
    Config()
    db.init()
    # initialize_log(logger_name="main")
    logger = get_log("main")
    app = DBHandlerWrapper()
    port = int(os.environ.get('PORT', 5001))
    host = os.environ.get('HOST', '0.0.0.0')
    log.logger.info("Running dbservice: host=%s, port=%s", host, port)
    app.run(debug=True, host=host, port=port)
