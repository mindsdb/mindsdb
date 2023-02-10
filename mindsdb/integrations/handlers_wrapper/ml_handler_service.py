import os
from mindsdb.utilities.config import Config
import mindsdb.interfaces.storage.db as db
from mindsdb.integrations.handlers_wrapper.ml_grpc_wrapper import MLServiceServicer
from mindsdb.integrations.libs.handler_helpers import registry
from mindsdb.utilities.log import initialize_log, get_log


if __name__ == "__main__":
    config = Config()
    db.init()
    initialize_log(config=config)
    logger = get_log(logger_name="main")

    app = MLServiceServicer()
    port = int(os.environ.get("PORT", 5001))
    host = os.environ.get("HOST")

    # If there is a handler discovery service
    # start a thread to send service info
    # to the discovery service
    registry_url = os.environ.get("REGISTRY_URL")
    if registry_url:
        registry_url = f"{registry_url}/register"
        interval = os.environ.get("REGISTRY_INTERVAL", 50)
        service_type = os.environ.get("MINDSDB_SERVICE_TYPE", "lightwood")
        data = {"host": host, "port": port, "type": service_type}
        registry(registry_url, data, interval)

    logger.info("Running ML service: host=%s, port=%s", host, port)
    app.run(debug=True, host="0.0.0.0", port=port)
