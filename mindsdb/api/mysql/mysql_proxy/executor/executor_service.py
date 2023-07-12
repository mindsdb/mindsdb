import os

import mindsdb.interfaces.storage.db as db
from mindsdb.api.mysql.mysql_proxy.executor.executor_grpc_wrapper import (
    ExecutorServiceServicer,
)
from mindsdb.utilities.config import Config
from mindsdb.utilities.log import get_log

if __name__ == "__main__":
    config = Config()
    db.init()
    logger = get_log(logger_name="main")
    app = ExecutorServiceServicer()
    port = int(os.environ.get("PORT", 5500))
    host = os.environ.get("HOST", "0.0.0.0")
    logger.info("Running Executor service: host=%s, port=%s", host, port)
    app.run(debug=True, host=host, port=port)
