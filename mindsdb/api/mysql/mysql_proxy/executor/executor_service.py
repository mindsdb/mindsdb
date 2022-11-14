import os
from mindsdb.api.mysql.mysql_proxy.executor.executor_wrapper import ExecutorService
from mindsdb.api.mysql.mysql_proxy.utilities import (
    logger
)

if __name__ == "__main__":
    app = ExecutorService()
    port = int(os.environ.get('PORT', 5500))
    host = os.environ.get('HOST', '0.0.0.0')
    logger.info("Running ML service: host=%s, port=%s", host, port)
    app.run(debug=True, host=host, port=port)
