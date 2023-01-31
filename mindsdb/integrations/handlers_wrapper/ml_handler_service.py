import os
from mindsdb.utilities.config import Config
import mindsdb.interfaces.storage.db as db
from mindsdb.integrations.handlers_wrapper.ml_handler_wrapper import MLHandlerWrapper
from mindsdb.utilities.log import initialize_log, get_log


if __name__ == "__main__":
    config = Config()
    db.init()
    initialize_log(config=config)
    logger = get_log(logger_name="main")
    app = MLHandlerWrapper(__name__)
    port = int(os.environ.get("PORT", 5001))
    host = os.environ.get("HOST", "0.0.0.0")
    logger.info("Running ML service: host=%s, port=%s", host, port)
    app.run(debug=True, host=host, port=port)
