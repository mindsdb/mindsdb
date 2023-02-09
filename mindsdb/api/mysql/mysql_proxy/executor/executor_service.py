import os
from mindsdb.api.mysql.mysql_proxy.executor.executor_wrapper import ExecutorService
from mindsdb.api.mysql.mysql_proxy.executor.executor_grpc_wrapper import ExecutorServiceServicer
from mindsdb.utilities.config import Config
import mindsdb.interfaces.storage.db as db
from mindsdb.utilities.log import initialize_log, get_log


if __name__ == "__main__":
    config = Config()
    db.init()
    initialize_log(config=config)
    logger = get_log(logger_name="main")
    api_type = os.environ.get("MINDSDB_INTERCONNECTION_API", "").lower()
    if api_type == "grpc":
        app = ExecutorServiceServicer()
    else:
        app = ExecutorService()
    port = int(os.environ.get("PORT", 5500))
    host = os.environ.get("HOST", "0.0.0.0")
    logger.info("Running Executor service: host=%s, port=%s", host, port)
    app._run(debug=True, host=host, port=port)
