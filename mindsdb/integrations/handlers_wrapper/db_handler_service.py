import os

import mindsdb.interfaces.storage.db as db
from mindsdb.integrations.handlers_wrapper.db_grpc_wrapper import DBServiceServicer
from mindsdb.utilities.config import Config
from mindsdb.utilities import log

if __name__ == "__main__":
    config = Config()
    db.init()
    logger = log.getLogger(__name__)
    app = DBServiceServicer()
    port = int(os.environ.get("PORT", 5001))
    host = os.environ.get("HOST", "0.0.0.0")
    logger.info("Running dbservice(%s): host=%s, port=%s", type(app), host, port)
    app.run(debug=True, host=host, port=port)
