import os
import json
from mindsdb.utilities.log import get_log
from mindsdb.utilities.config import Config
import mindsdb.interfaces.storage.db as db
from mindsdb.integrations.handlers_wrapper.ml_handler_wrapper import MLHandlerWrapper
from mindsdb.utilities.log import (
    initialize_log,
    get_log
)

# log = get_log()


if __name__ == "__main__":
    Config()
    db.init()
    logger = get_log("main")
    params = os.environ.get('PARAMS', '{}')
    kwargs = json.loads(params)
    if "name" not in kwargs:
        kwargs["name"] = __name__
    app = MLHandlerWrapper(**kwargs)
    port = int(os.environ.get('PORT', 5001))
    host = os.environ.get('HOST', '0.0.0.0')
    logger.info("Running ML service: host=%s, port=%s", host, port)
    app.run(debug=True, host=host, port=port)
